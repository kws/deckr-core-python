"""Select service payload schemas from the declared message and reply status."""

from collections.abc import Callable
from typing import Any

from deckr.contracts.models import thaw_json
from deckr.core.json_schema import local_validator
from deckr.services.messages import ServiceMessageBody, ServiceMessageStatus

_native_validators: dict[str, Callable[[Any], Any]] = {}


def register_service_payload_validator(
    schema_id: str, validator: Callable[[Any], Any]
) -> None:
    """Bind a trusted local model to an exact schema reference, without fetching it.

    Domain declarations register their implementations at import time. Advertised
    schemas cannot register validators or replace an existing implementation.
    """
    existing = _native_validators.get(schema_id)
    if existing is not None and existing != validator:
        raise ValueError(f"Service payload validator already registered: {schema_id}")
    _native_validators[schema_id] = validator


def validate_service_payloads(definition, body: ServiceMessageBody) -> None:
    if body.status is None:
        fields = (
            (definition.params_schema, body.params),
            (definition.event_schema, body.event),
        )
    elif body.status == ServiceMessageStatus.OK:
        fields = ((definition.result_schema, body.result),)
    else:
        fields = (
            (
                definition.error_schema,
                body.error.model_dump(by_alias=True, exclude_none=True, mode="json")
                if body.error
                else None,
            ),
        )
    for schema, payload in fields:
        if schema is not None:
            try:
                document = thaw_json(schema.json_schema)
                native = _native_validators.get(schema.schema_id)
                if native is not None and document == {"$ref": schema.schema_id}:
                    native(thaw_json(payload))
                else:
                    local_validator(document).validate(thaw_json(payload))
            except Exception as exc:
                raise ValueError(
                    f"Service payload does not match {schema.schema_id}"
                ) from exc


def validated_service_body(definition, **fields):
    body = ServiceMessageBody(**fields)
    validate_service_payloads(definition, body)
    return body.to_dict()


def model_payload_schema(schema_id: str, model):
    """Declare an exact reference implemented by a local, conforming wire model."""
    from deckr.services.protocol import ServicePayloadSchema

    register_service_payload_validator(schema_id, model.model_validate)
    return ServicePayloadSchema(schemaId=schema_id, schema={"$ref": schema_id})
