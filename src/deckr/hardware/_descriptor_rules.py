"""Descriptor rules."""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping
from typing import Any, Literal

CapabilityAccess = Literal[
    "emits",
    "readable",
    "settable",
    "requestable",
    "invokable",
]


CapabilityDirection = Literal["input", "output", "state", "command"]


ConnectionStatus = Literal["connected", "available", "unavailable", "unknown"]


ControlGeometryUnit = Literal["grid", "pixel", "normalized", "millimeter"]


ProjectionOwner = Literal["hardware_manager", "adapter", "component"]


ProjectionType = Literal["projection", "derivation"]


DECKR_INPUT_BUTTON = "dev.deckr.input.button"


DECKR_INPUT_ENCODER = "dev.deckr.input.encoder"


DECKR_INPUT_TOUCH = "dev.deckr.input.touch"


DECKR_OUTPUT_RASTER = "dev.deckr.output.raster"


DECKR_DEVICE_POWER = "dev.deckr.device.power"


CORE_CAPABILITY_FAMILIES = frozenset(
    {
        DECKR_DEVICE_POWER,
        DECKR_INPUT_BUTTON,
        DECKR_INPUT_ENCODER,
        DECKR_INPUT_TOUCH,
        DECKR_OUTPUT_RASTER,
    }
)


CORE_CAPABILITY_TYPES_BY_FAMILY: Mapping[str, frozenset[str]] = {
    DECKR_DEVICE_POWER: frozenset({"screen"}),
    DECKR_INPUT_BUTTON: frozenset({"activation", "momentary"}),
    DECKR_INPUT_ENCODER: frozenset({"relative"}),
    DECKR_INPUT_TOUCH: frozenset({"gesture"}),
    DECKR_OUTPUT_RASTER: frozenset({"bitmap"}),
}


BUTTON_ACTIVATION_EVENTS = ("press",)


BUTTON_MOMENTARY_EVENTS = ("down", "up")


ENCODER_RELATIVE_EVENTS = ("rotate",)


TOUCH_GESTURE_EVENTS = ("tap", "swipe")


CONTROL_ACTIVATION_EVENTS = ("up", "press", "tap")


RASTER_COMMAND_TYPES = ("set_frame", "clear")


POWER_COMMAND_TYPES = ("sleep", "wake")


DESCRIPTOR_SCHEMA_VERSION = "1"


DEVICE_DESCRIPTOR_SCHEMA_ID = "dev.deckr.hardware.device_descriptor.v1"


CONTROL_DESCRIPTOR_SCHEMA_ID = "dev.deckr.hardware.control_descriptor.v1"


CAPABILITY_DESCRIPTOR_SCHEMA_ID = "dev.deckr.hardware.capability_descriptor.v1"


_CONTRACT_NAME_PATTERN = r"^[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)*$"


_GLOBALLY_QUALIFIED_NAME_PATTERN = r"^[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)+$"


_EXTENSION_CAPABILITY_FAMILY_PATTERN = (
    r"^(?!dev\.deckr\.)[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)+$"
)


_CONTRACT_NAME_RE = re.compile(_CONTRACT_NAME_PATTERN)


_GLOBALLY_QUALIFIED_NAME_RE = re.compile(_GLOBALLY_QUALIFIED_NAME_PATTERN)


_ENDPOINT_ADDRESS_RE = re.compile(r"^(action_provider|controller|hardware_manager):")


_JSON_SCHEMA_CONTRACT_KEYS = frozenset(
    {
        "$ref",
        "allOf",
        "anyOf",
        "const",
        "enum",
        "items",
        "oneOf",
        "properties",
        "type",
    }
)


_DIRECTION_ACCESS: Mapping[CapabilityDirection, frozenset[CapabilityAccess]] = {
    "input": frozenset({"emits"}),
    "output": frozenset({"settable", "invokable"}),
    "state": frozenset({"readable", "requestable", "settable", "emits"}),
    "command": frozenset({"invokable"}),
}


def _require_non_empty(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _require_finite(value: float | None, *, field_name: str) -> float | None:
    if value is not None and not math.isfinite(value):
        raise ValueError(f"{field_name} must be finite")
    return value


def _require_json_wire_safe(value: Any, *, field_name: str) -> None:
    if isinstance(value, Mapping):
        for item in value.values():
            _require_json_wire_safe(item, field_name=field_name)
        return
    if isinstance(value, list | tuple):
        for item in value:
            _require_json_wire_safe(item, field_name=field_name)
        return
    if value is None or isinstance(value, str | bool | int):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{field_name} must not contain NaN or Infinity")
        return
    raise ValueError(
        f"{field_name} contains unsupported JSON value type: {type(value).__name__}"
    )


def _require_not_endpoint_address(value: str, *, field_name: str) -> str:
    normalized = _require_non_empty(value, field_name=field_name)
    if _ENDPOINT_ADDRESS_RE.match(normalized):
        raise ValueError(f"{field_name} must not be a Deckr endpoint address")
    return normalized


def _require_contract_token(value: str, *, field_name: str) -> str:
    normalized = _require_non_empty(value, field_name=field_name)
    if normalized != value:
        raise ValueError(f"{field_name} must not contain surrounding whitespace")
    if not _CONTRACT_NAME_RE.fullmatch(normalized):
        raise ValueError(
            f"{field_name} must be a lowercase contract identifier, not a display label"
        )
    return normalized


def _require_globally_qualified_name(value: str, *, field_name: str) -> str:
    normalized = _require_contract_token(value, field_name=field_name)
    if not _GLOBALLY_QUALIFIED_NAME_RE.fullmatch(normalized):
        raise ValueError(f"{field_name} must be globally namespaced")
    return normalized


def _require_unique(values: Iterable[str], *, field_name: str) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    if duplicates:
        raise ValueError(
            f"{field_name} must be unique: {', '.join(sorted(duplicates))}"
        )


def _validate_unique_tuple(
    values: tuple[str, ...], *, field_name: str
) -> tuple[str, ...]:
    _require_unique(values, field_name=field_name)
    return values
