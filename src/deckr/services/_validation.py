"""Validation."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

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


def _require_text(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


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
