"""Declared schema identities must survive discovery and validate locally."""

import pytest

from deckr.action_runtime import ACTION_RUNTIME_SERVICE_PROTOCOL as PROTOCOL
from deckr.services import (
    ServiceMessageBody,
    ServiceMessageDefinition,
    ServicePayloadSchema,
)
from deckr.services.payload_validation import (
    register_service_payload_validator,
    validate_service_payloads,
)


def test_action_reference_is_declared_and_resolves_after_wire_roundtrip():
    definition = ServiceMessageDefinition.from_wire(
        PROTOCOL.messages["close_page"].to_dict()
    )
    schema_id = "https://kws.github.io/deckr/contract/v1/schemas/deckr/action-runtime/close-page-body.v1.schema.json"
    assert definition.params_schema.to_dict() == {
        "schemaId": schema_id,
        "schema": {"$ref": schema_id},
    }
    body = ServiceMessageBody(
        serviceNamespace=PROTOCOL.namespace,
        name="close_page",
        intent="command",
        exchangePattern="one_way",
        params={
            "target": {"kind": "page_session", "pageSessionId": "page", "generation": 0}
        },
    )
    validate_service_payloads(definition, body)
    with pytest.raises(ValueError, match="Service payload does not match"):
        validate_service_payloads(definition, body.model_copy(update={"params": {}}))


def test_native_binding_does_not_override_different_schema_or_resolve_unknown_reference():
    definition = PROTOCOL.messages["close_page"]
    body = ServiceMessageBody(
        serviceNamespace=PROTOCOL.namespace,
        name="close_page",
        intent="command",
        exchangePattern="one_way",
        params={
            "target": {"kind": "page_session", "pageSessionId": "page", "generation": 0}
        },
    )
    for schema in (
        ServicePayloadSchema(
            schemaId=definition.params_schema.schema_id, schema={"type": "string"}
        ),
        ServicePayloadSchema(
            schemaId="https://example.org/unknown",
            schema={"$ref": "https://example.org/unknown"},
        ),
    ):
        with pytest.raises(ValueError, match="Service payload does not match"):
            validate_service_payloads(
                definition.model_copy(update={"params_schema": schema}), body
            )
    with pytest.raises(ValueError, match="already registered"):
        register_service_payload_validator(
            definition.params_schema.schema_id, lambda _: None
        )


def test_all_action_messages_have_the_correct_payload_position():
    for definition in PROTOCOL.messages.values():
        is_event = definition.intent.value == "event"
        assert (definition.event_schema is not None) == is_event
        assert (definition.params_schema is not None) != is_event
