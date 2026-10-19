"""Descriptor-scoped validation for managed hardware operations."""

from collections.abc import Mapping
from decimal import Decimal

from deckr.contracts.models import thaw_json
from deckr.core.json_schema import local_validator
from deckr.hardware.capabilities import (
    button_activation_input_value,
    button_momentary_input_value,
    device_power_command_params,
    encoder_relative_input_value,
    raster_bitmap_command_params,
    touch_gesture_input_value,
)
from deckr.hardware.descriptors import CapabilityRef, DeviceDescriptor
from deckr.hardware.raster_validation import raster_image_fields
from deckr.hardware.state import HardwareUnavailable


def validate_command(capability, command_type: str, params) -> None:
    if (
        capability.direction not in {"output", "command"}
        or not {"settable", "invokable"}.intersection(capability.access)
        or command_type not in capability.command_types
    ):
        raise HardwareUnavailable(
            "unsupported_command", "Descriptor does not support this command"
        )
    validate_payload(capability.command_schema, params)
    try:
        if (
            capability.family == "dev.deckr.output.raster"
            and capability.capability_type == "bitmap"
        ):
            parsed = raster_bitmap_command_params(command_type, params)
            if command_type == "set_frame":
                validate_constraints(capability, raster_image_fields(parsed))
        elif capability.family == "dev.deckr.device.power":
            device_power_command_params(params)
        validate_constraints(capability, params)
    except (TypeError, ValueError, ArithmeticError) as exc:
        raise HardwareUnavailable("invalid_capability_payload", str(exc)) from exc


def validate_input(capability, event_type: str, value) -> None:
    if (
        capability.direction != "input"
        or "emits" not in capability.access
        or event_type not in capability.event_types
    ):
        raise HardwareUnavailable("unsupported_input", "Input is not declared")
    validate_payload(capability.value_schema, value)
    parsers = {
        ("dev.deckr.input.button", "activation"): button_activation_input_value,
        ("dev.deckr.input.button", "momentary"): button_momentary_input_value,
        ("dev.deckr.input.encoder", "relative"): encoder_relative_input_value,
        ("dev.deckr.input.touch", "gesture"): touch_gesture_input_value,
    }
    try:
        parser = parsers.get((capability.family, capability.capability_type))
        if parser is not None:
            parser(value)
        if (
            isinstance(value, Mapping)
            and value.get("eventType", event_type) != event_type
        ):
            raise ValueError("Input value and event type disagree")
        validate_constraints(capability, value)
    except (TypeError, ValueError, ArithmeticError) as exc:
        raise HardwareUnavailable("invalid_capability_payload", str(exc)) from exc


def validate_constraints(capability, value) -> None:
    if not isinstance(value, Mapping):
        return
    for constraint in capability.constraints:
        if constraint.subject not in value:
            continue
        actual = value[constraint.subject]
        if constraint.value is not None and actual != constraint.value:
            raise ValueError(f"{constraint.subject} disagrees with its fixed value")
        if constraint.values and actual not in constraint.values:
            raise ValueError(f"{constraint.subject} is outside its allowed values")
        if constraint.minimum is not None and actual < constraint.minimum:
            raise ValueError(f"{constraint.subject} is below its minimum")
        if constraint.maximum is not None and actual > constraint.maximum:
            raise ValueError(f"{constraint.subject} is above its maximum")
        if constraint.step is not None:
            offset = Decimal(str(actual)) - Decimal(str(constraint.minimum or 0))
            if offset % Decimal(str(constraint.step)):
                raise ValueError(f"{constraint.subject} is outside its step increments")


def validate_value(capability, value) -> None:
    validate_payload(capability.value_schema, value)
    try:
        validate_constraints(capability, value)
    except (TypeError, ValueError, ArithmeticError) as exc:
        raise HardwareUnavailable("invalid_capability_payload", str(exc)) from exc


def validate_state_request(capability) -> None:
    if not {"readable", "requestable"}.intersection(capability.access):
        raise HardwareUnavailable(
            "unsupported_state_request", "Descriptor does not support state requests"
        )


def resolve_capability(devices: Mapping[str, DeviceDescriptor], ref: CapabilityRef):
    descriptor = devices.get(ref.device_ref.device_id)
    if descriptor is None or ref.device_ref.fingerprint not in {
        None,
        descriptor.fingerprint,
    }:
        raise HardwareUnavailable(
            "device_scope_mismatch", "Device is outside this claim"
        )
    capabilities = descriptor.capabilities
    if ref.control_id is not None:
        control = next(
            (c for c in descriptor.controls if c.control_id == ref.control_id), None
        )
        if control is None:
            raise HardwareUnavailable(
                "control_scope_mismatch",
                "Control is absent from the claimed descriptor",
            )
        capabilities = control.capabilities
    capability = next(
        (c for c in capabilities if c.capability_id == ref.capability_id), None
    )
    if capability is None:
        raise HardwareUnavailable(
            "capability_scope_mismatch",
            "Capability is absent from the claimed descriptor",
        )
    return capability


def validate_payload(schema, value) -> None:
    if schema is None:
        raise HardwareUnavailable(
            "capability_schema_missing",
            "Capability does not declare the required schema",
        )
    document = thaw_json(schema.json_schema)
    try:
        local_validator(document).validate(thaw_json(value))
    except Exception as exc:
        raise HardwareUnavailable("invalid_capability_payload", str(exc)) from exc
