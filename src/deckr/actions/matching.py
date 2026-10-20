"""Requirement matching within an application's negotiated control scope.

These pure helpers neither choose a device nor grant authority. Callers supply
the current negotiated descriptor and the configured capability scope.
"""

from collections.abc import Collection, Sequence

from deckr.actions.bindings import (
    BindingMetadata,
    CapabilityRequirement,
    CapabilityRequirementSelector,
    MatchedCapability,
)
from deckr.actions.catalog import ActionDescriptor
from deckr.hardware.descriptors import (
    CapabilityDescriptor,
    CapabilityRef,
    DeviceDescriptor,
    DeviceRef,
)


def _selector_matches(
    selector: CapabilityRequirementSelector, capability: CapabilityDescriptor
) -> bool:
    return (
        all(
            expected is None or expected == actual
            for expected, actual in (
                (selector.capability_id, capability.capability_id),
                (selector.family, capability.family),
                (selector.capability_type, capability.capability_type),
                (selector.direction, capability.direction),
            )
        )
        and set(selector.event_types) <= set(capability.event_types)
        and set(selector.command_types) <= set(capability.command_types)
    )


def _views(capability: CapabilityDescriptor) -> set[str]:
    views = set()
    if not capability.family.startswith("dev.deckr."):
        views.add("extension")
    if capability.projection is None:
        views.add("raw")
        if capability.family.startswith("dev.deckr."):
            views.add("native")
    else:
        views.add(
            "derived"
            if capability.projection.projection_type == "derivation"
            else "projected"
        )
    return views


def _candidates(
    requirement: CapabilityRequirement, capabilities: Sequence[CapabilityDescriptor]
) -> tuple[CapabilityDescriptor, ...]:
    eligible = tuple(
        capability
        for capability in capabilities
        if set(requirement.event_types) <= set(capability.event_types)
        and set(requirement.command_types) <= set(capability.command_types)
        and (not requirement.views or set(requirement.views) & _views(capability))
    )
    for selector in requirement.preferences:
        matches = tuple(
            capability
            for capability in eligible
            if _selector_matches(selector, capability)
        )
        if matches:
            return matches
    return ()


def _scope(
    device: DeviceDescriptor,
    device_ref: DeviceRef,
    control_id: str,
    configured_capability_ids: Collection[str] | None,
) -> tuple[CapabilityDescriptor, ...]:
    if device_ref.device_id != device.device_id or device_ref.fingerprint not in (
        None,
        device.fingerprint,
    ):
        raise ValueError("device reference differs from the negotiated descriptor")
    control = next(
        (item for item in device.controls if item.control_id == control_id), None
    )
    if control is None:
        raise ValueError("control is outside the negotiated descriptor")
    if configured_capability_ids is None:
        return control.capabilities
    ids = set(configured_capability_ids)
    if not ids <= {cap.capability_id for cap in control.capabilities}:
        raise ValueError("configured capability is outside the bound control")
    return tuple(cap for cap in control.capabilities if cap.capability_id in ids)


def _match(
    requirement: CapabilityRequirement,
    capability: CapabilityDescriptor,
    device: DeviceDescriptor,
    device_ref: DeviceRef,
    control_id: str,
) -> MatchedCapability:
    source = None
    provenance = "native" if capability.family.startswith("dev.deckr.") else "extension"
    if capability.projection is not None:
        projection = capability.projection
        provenance = projection.projection_type
        source = CapabilityRef(
            deviceRef=device_ref,
            controlId=projection.source.control_id,
            capabilityId=projection.source.capability_id,
        )
        capabilities = (
            device.capabilities
            if source.control_id is None
            else next(
                (
                    control.capabilities
                    for control in device.controls
                    if control.control_id == source.control_id
                ),
                (),
            )
        )
        if not any(cap.capability_id == source.capability_id for cap in capabilities):
            raise ValueError("projection source is outside the negotiated descriptor")
    return MatchedCapability(
        requirementName=requirement.name,
        capability=CapabilityRef(
            deviceRef=device_ref,
            controlId=control_id,
            capabilityId=capability.capability_id,
        ),
        family=capability.family,
        type=capability.capability_type,
        direction=capability.direction,
        eventTypes=capability.event_types,
        commandTypes=capability.command_types,
        provenance=provenance,
        source=source,
    )


def match_capability_requirements(
    action: ActionDescriptor,
    device: DeviceDescriptor,
    device_ref: DeviceRef,
    control_id: str,
    *,
    configured_capability_ids: Collection[str] | None = None,
) -> tuple[MatchedCapability, ...]:
    """Select the first descriptor-order candidate of the first eligible preference.

    Equivalent-candidate selection is a convenience policy. Applications can
    choose another equivalent candidate and use ``validate_binding_resources``.
    A missing required match raises ValueError; optional misses are omitted.
    """
    capabilities = _scope(device, device_ref, control_id, configured_capability_ids)
    matches = []
    for requirement in action.requirements or ():
        candidates = _candidates(requirement, capabilities)
        if candidates:
            matches.append(
                _match(requirement, candidates[0], device, device_ref, control_id)
            )
        elif requirement.required:
            raise ValueError(f"required capability {requirement.name!r} is unavailable")
    return tuple(matches)


def validate_binding_resources(
    binding: BindingMetadata,
    action: ActionDescriptor,
    device: DeviceDescriptor,
    device_ref: DeviceRef,
    control_id: str,
    *,
    configured_capability_ids: Collection[str] | None = None,
) -> None:
    """Validate a complete ordered match result, allowing equivalent candidates."""
    capabilities = _scope(device, device_ref, control_id, configured_capability_ids)
    if (
        binding.action_id != action.action_id
        or binding.device_ref != device_ref
        or binding.control_ref.device_ref != device_ref
        or binding.control_ref.control_id != control_id
    ):
        raise ValueError(
            "binding differs from the selected action or device/control scope"
        )
    remaining = iter(binding.matched_capabilities)
    for requirement in action.requirements or ():
        candidates = _candidates(requirement, capabilities)
        if not candidates:
            if requirement.required:
                raise ValueError(
                    f"required capability {requirement.name!r} is unavailable"
                )
            continue
        supplied = next(remaining, None)
        if supplied is None or supplied.requirement_name != requirement.name:
            raise ValueError(
                "matches must occur once per satisfied requirement in declaration order"
            )
        capability = next(
            (
                cap
                for cap in candidates
                if cap.capability_id == supplied.capability.capability_id
            ),
            None,
        )
        if capability is None:
            raise ValueError("match is not eligible for the first available preference")
        expected = _match(requirement, capability, device, device_ref, control_id)
        if (
            supplied.model_copy(update={"event_types": (), "command_types": ()})
            != expected.model_copy(update={"event_types": (), "command_types": ()})
            or set(supplied.event_types) != set(expected.event_types)
            or set(supplied.command_types) != set(expected.command_types)
        ):
            raise ValueError(
                "match changes descriptor identity, supported names, provenance, or source"
            )
    if next(remaining, None) is not None:
        raise ValueError("match names an undeclared or unsatisfied requirement")
