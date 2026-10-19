"""Select service payload schemas from the declared message and reply status."""

from deckr.contracts.models import thaw_json
from deckr.core.json_schema import local_validator
from deckr.services.messages import ServiceMessageBody, ServiceMessageStatus


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
                local_validator(thaw_json(schema.json_schema)).validate(
                    thaw_json(payload)
                )
            except Exception as exc:
                raise ValueError(
                    f"Service payload does not match {schema.schema_id}"
                ) from exc


def validated_service_body(definition, **fields):
    body = ServiceMessageBody(**fields)
    validate_service_payloads(definition, body)
    return body.to_dict()
