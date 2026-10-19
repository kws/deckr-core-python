"""Hardware profile admission and descriptor stability checks."""

from collections.abc import Collection, Mapping

from deckr.contracts.messages import EndpointAddress
from deckr.hardware.descriptors import DeviceDescriptor, DeviceRef
from deckr.hardware.profiles import HardwareClaimTerms


def descriptor_requires_new_claim(
    previous: DeviceDescriptor, current: DeviceDescriptor
) -> bool:
    """Keep claims across display-label edits, but fence other descriptor changes."""

    presentation = {"display_name": True, "controls": {"__all__": {"label"}}}
    return previous.model_dump(exclude=presentation) != current.model_dump(
        exclude=presentation
    )


def device_reference_matches(
    ref: DeviceRef,
    manager: EndpointAddress,
    devices: Mapping[str, DeviceDescriptor],
) -> bool:
    descriptor = devices.get(ref.device_id)
    return (
        ref.manager_id == manager.endpoint_id
        and descriptor is not None
        and ref.fingerprint in {None, descriptor.fingerprint}
    )


def claim_matches_inventory(
    terms: HardwareClaimTerms,
    participants: Collection[EndpointAddress],
    manager: EndpointAddress,
    devices: Mapping[str, DeviceDescriptor],
) -> bool:
    """This manager exposes one exclusive instance of each device."""

    return (
        terms.manager_endpoint == manager
        and set(participants) == {manager, terms.controller_endpoint}
        and all(
            device.instance_count == 1
            and device_reference_matches(device.device_ref, manager, devices)
            for device in terms.devices
        )
    )
