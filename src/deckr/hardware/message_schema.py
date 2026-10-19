"""Message schema."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from deckr.contracts.messages import (
    HARDWARE_MESSAGES_LANE,
    HARDWARE_MESSAGES_SCHEMA_ID,
    DeckrMessage,
)
from deckr.contracts.models import DeckrModel
from deckr.hardware.message_bodies import HARDWARE_BODY_BY_MESSAGE_TYPE


def hardware_message_schema() -> dict[str, Any]:
    definitions: dict[str, Any] = {}
    envelope_ref = _add_schema_model(definitions, DeckrMessage)
    variants: list[dict[str, Any]] = []
    for message_type, body_type in HARDWARE_BODY_BY_MESSAGE_TYPE.items():
        body_ref = _add_schema_model(definitions, body_type)
        variants.append(
            {
                "allOf": [
                    envelope_ref,
                    {
                        "type": "object",
                        "required": ["lane", "messageType", "contract", "body"],
                        "properties": {
                            "lane": {"const": HARDWARE_MESSAGES_LANE},
                            "messageType": {"const": message_type},
                            "contract": {"$ref": "#/$defs/ContractPointer"},
                            "body": body_ref,
                        },
                    },
                ]
            }
        )
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": HARDWARE_MESSAGES_SCHEMA_ID,
        "title": "Deckr hardware_messages Lane Message",
        "x-deckr-schema-version": "1",
        "oneOf": variants,
        "$defs": definitions,
    }


def _add_schema_model(
    definitions: dict[str, Any],
    model: type[DeckrModel],
) -> dict[str, str]:
    schema = model.model_json_schema(
        by_alias=True,
        ref_template="#/$defs/{model}",
    )
    for name, definition in schema.pop("$defs", {}).items():
        _add_schema_definition(definitions, name, definition)
    schema.pop("$schema", None)
    name = model.__name__
    _add_schema_definition(definitions, name, schema)
    return {"$ref": f"#/$defs/{name}"}


def _add_schema_definition(
    definitions: dict[str, Any],
    name: str,
    definition: Mapping[str, Any],
) -> None:
    schema = deepcopy(dict(definition))
    existing = definitions.get(name)
    if existing is not None:
        if existing != schema:
            raise RuntimeError(f"Conflicting schema definition {name!r}")
        return
    definitions[name] = schema
