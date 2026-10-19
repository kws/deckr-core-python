"""Schema."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from deckr.actions.message_types import ACTION_BODY_BY_MESSAGE_TYPE, ACTION_EXTENSION
from deckr.contracts.messages import (
    ACTION_MESSAGES_SCHEMA_ID,
    ACTIONS_LANE,
    DeckrMessage,
)
from deckr.contracts.models import DeckrModel

_ACTION_NO_CONTRACT_MESSAGE_TYPES = frozenset({})


_ACTION_OPTIONAL_CONTRACT_MESSAGE_TYPES = frozenset(
    {
        ACTION_EXTENSION,
    }
)


def action_message_schema() -> dict[str, Any]:
    """Return the Python ``actions`` model schema for local introspection."""

    definitions: dict[str, Any] = {}
    envelope_ref = _add_schema_model(definitions, DeckrMessage)
    variants: list[dict[str, Any]] = []
    for message_type, body_type in ACTION_BODY_BY_MESSAGE_TYPE.items():
        body_ref = _add_schema_model(definitions, body_type)
        required = ["lane", "messageType", "body"]
        properties: dict[str, Any] = {
            "lane": {"const": ACTIONS_LANE},
            "messageType": {"const": message_type},
            "body": body_ref,
        }
        if message_type in _ACTION_NO_CONTRACT_MESSAGE_TYPES:
            properties["contract"] = False
        elif message_type not in _ACTION_OPTIONAL_CONTRACT_MESSAGE_TYPES:
            required.append("contract")
            properties["contract"] = {"$ref": "#/$defs/ContractPointer"}
        variants.append(
            {
                "allOf": [
                    envelope_ref,
                    {
                        "type": "object",
                        "required": required,
                        "properties": properties,
                    },
                ]
            }
        )
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": ACTION_MESSAGES_SCHEMA_ID,
        "title": "Deckr actions Lane Message",
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
