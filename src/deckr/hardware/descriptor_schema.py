"""Descriptor schema."""

from __future__ import annotations

from typing import Any

from deckr.hardware._descriptor_rules import (
    _CONTRACT_NAME_PATTERN,
    _EXTENSION_CAPABILITY_FAMILY_PATTERN,
    CAPABILITY_DESCRIPTOR_SCHEMA_ID,
    CONTROL_DESCRIPTOR_SCHEMA_ID,
    CORE_CAPABILITY_FAMILIES,
    DESCRIPTOR_SCHEMA_VERSION,
    DEVICE_DESCRIPTOR_SCHEMA_ID,
)
from deckr.hardware.device_descriptor import _SCHEMA_MODELS


def descriptor_schema_artifacts() -> dict[str, dict[str, Any]]:
    """Return JSON Schema artifacts for descriptor consumers."""

    artifacts: dict[str, dict[str, Any]] = {}
    for schema_id, (title, model) in _SCHEMA_MODELS.items():
        schema = model.model_json_schema(
            by_alias=True,
            ref_template="#/$defs/{model}",
        )
        schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
        schema["$id"] = schema_id
        schema["title"] = title
        schema["x-deckr-schema-version"] = DESCRIPTOR_SCHEMA_VERSION
        _apply_descriptor_schema_rules(schema)
        artifacts[schema_id] = schema
    return artifacts


def _apply_descriptor_schema_rules(schema: dict[str, Any]) -> None:
    for candidate in _schema_definitions(schema):
        properties = candidate.get("properties")
        if not isinstance(properties, dict):
            continue
        if {"capabilityId", "family", "type"}.issubset(properties):
            properties["family"] = {
                "anyOf": [
                    {"enum": sorted(CORE_CAPABILITY_FAMILIES)},
                    {
                        "pattern": _EXTENSION_CAPABILITY_FAMILY_PATTERN,
                        "type": "string",
                    },
                ],
                "description": (
                    "Deckr-owned families must be one of the v1 core families; "
                    "extension families must be globally namespaced and must not "
                    "use the dev.deckr. namespace."
                ),
                "title": "Family",
            }
            type_schema = properties["type"]
            if isinstance(type_schema, dict):
                type_schema["pattern"] = _CONTRACT_NAME_PATTERN


def _schema_definitions(schema: dict[str, Any]) -> tuple[dict[str, Any], ...]:
    definitions = [schema]
    defs = schema.get("$defs")
    if isinstance(defs, dict):
        definitions.extend(item for item in defs.values() if isinstance(item, dict))
    return tuple(definitions)


def device_descriptor_schema() -> dict[str, Any]:
    """Return the Python ``DeviceDescriptor`` schema for local introspection."""

    return descriptor_schema_artifacts()[DEVICE_DESCRIPTOR_SCHEMA_ID]


def control_descriptor_schema() -> dict[str, Any]:
    """Return the Python ``ControlDescriptor`` schema for local introspection."""

    return descriptor_schema_artifacts()[CONTROL_DESCRIPTOR_SCHEMA_ID]


def capability_descriptor_schema() -> dict[str, Any]:
    """Return the Python ``CapabilityDescriptor`` schema for local introspection."""

    return descriptor_schema_artifacts()[CAPABILITY_DESCRIPTOR_SCHEMA_ID]
