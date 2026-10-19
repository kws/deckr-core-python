"""Message-declaration payload and envelope checks (SEM-SVC-005)."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from message_bus_mocks import mock_deckr
from test_service_provider import (
    CONSUMER,
    SERVICE,
    _protocol,
    _request,
    _wait_for_valid,
)
from test_services_client import _CLIENT_ADDRESS, _agreement, _descriptor, _services

from deckr.concord import ConcordAgreementSpec
from deckr.services import (
    ServiceMessageBody,
    ServiceMessageDefinition,
    ServicePayloadSchema,
    ServiceUnavailable,
    ServiceUseAuthorizationError,
)
from deckr.services.messages import service_response_message
from deckr.services.payload_validation import validate_service_payloads


def _schema(field, kind):
    return ServicePayloadSchema(
        schemaId=f"org.example.{field}.v1",
        schema={
            "type": "object",
            "properties": {field: {"type": kind}},
            "required": [field],
            "additionalProperties": False,
        },
    )


@pytest.mark.parametrize(
    "field,status",
    [("params", None), ("event", None), ("result", "ok"), ("error", "error")],
)
def test_payload_selection_uses_the_declared_field_and_reply_status(field, status):
    definition = ServiceMessageDefinition(
        intent="command",
        exchangePattern="request_reply",
        direction="bidirectional",
        **{
            f"{field}Schema": _schema("code", "string")
            if field == "error"
            else _schema("value", "integer")
        },
    )
    body = ServiceMessageBody(
        serviceNamespace="org.example",
        name="sample",
        intent="command",
        exchangePattern="request_reply",
        status=status,
        **{
            field: {"code": "failed", "message": "failure"}
            if field == "error"
            else {"value": 1}
        },
    )
    if field == "error":
        # Error schemas describe the complete structured error, not result or params.
        definition = definition.model_copy(
            update={
                "error_schema": ServicePayloadSchema(
                    schemaId="org.example.error.v1",
                    schema={
                        "type": "object",
                        "properties": {"code": {"const": "failed"}},
                        "required": ["code"],
                    },
                )
            }
        )
        invalid = body.model_copy(
            update={"error": body.error.model_copy(update={"code": "other"})}
        )
    else:
        invalid = body.model_copy(update={field: {"value": "not a number"}})
    validate_service_payloads(definition, body)
    with pytest.raises(ValueError, match="Service payload"):
        validate_service_payloads(definition, invalid)


@pytest.mark.asyncio
async def test_managed_request_rejects_bad_params_before_publish_and_bad_result_after_reply():
    descriptor = _descriptor()
    messages = dict(descriptor.supported_messages)
    messages["play"] = messages["play"].model_copy(
        update={
            "params_schema": _schema("track", "string"),
            "result_schema": _schema("count", "integer"),
        }
    )
    descriptor = replace(descriptor, supported_messages=messages)
    result = {"count": "bad"}

    async def request(**kwargs):
        return service_response_message(
            sender=descriptor.endpoint,
            sender_session_id=descriptor.session_id,
            recipient=_CLIENT_ADDRESS,
            recipient_session_id="client-session",
            subject=kwargs["subject"],
            in_reply_to="request",
            contract=kwargs["contract"],
            body=ServiceMessageBody(
                serviceNamespace=descriptor.namespace,
                name="play",
                intent="command",
                exchangePattern="request_reply",
                status="ok",
                result=result,
            ),
        )

    endpoint = SimpleNamespace(
        address=_CLIENT_ADDRESS,
        session_id="client-session",
        request=AsyncMock(side_effect=request),
    )
    services = _services(
        endpoint=endpoint,
        concord=SimpleNamespace(propose=Mock(return_value=_agreement())),
    )
    async with services.use(descriptor) as service:
        with pytest.raises(ValueError, match="Service payload"):
            await service.request("play", {"track": 12})
        endpoint.request.assert_not_awaited()
        with pytest.raises(ServiceUnavailable) as rejected:
            await service.request("play", {"track": "one"})
        assert rejected.value.code == "invalid_service_response"
        result = {"count": 1}
        assert (await service.request("play", {"track": "one"})).result == {"count": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["payload", "expired", "recipient", "session"])
async def test_provider_authorization_rejects_invalid_wire_requests(fault):
    protocol = _protocol()
    messages = dict(protocol.messages)
    messages["play"] = messages["play"].model_copy(
        update={"params_schema": _schema("track", "string")}
    )
    protocol = replace(protocol, messages=messages)
    async with (
        mock_deckr() as runtime,
        runtime.endpoint(SERVICE, session_id="service-session") as endpoint,
        runtime.endpoint(CONSUMER, session_id="consumer-session") as consumer,
        runtime.service_provider(
            endpoint, protocol, service_id="demo-home"
        ) as provider,
        runtime.concord.propose(
            consumer,
            ConcordAgreementSpec(
                profile=protocol.use_profile,
                participants=(CONSUMER, SERVICE),
                participant_sessions={str(SERVICE): "service-session"},
            ),
        ) as agreement,
    ):
        await _wait_for_valid(agreement)
        message = _request(agreement)
        assert (await provider.authorize(message)).body.params == {"track": "one"}
        if fault == "payload":
            message = message.model_copy(
                update={"body": dict(message.body) | {"params": {"track": 1}}}
            )
        elif fault == "expired":
            message = message.model_copy(
                update={"expires_at": datetime.now(UTC) - timedelta(seconds=1)}
            )
        elif fault == "session":
            message = message.model_copy(
                update={"recipient_session_id": "other-session"}
            )
        else:
            message = message.model_copy(
                update={
                    "recipient": message.recipient.model_copy(
                        update={"endpoint": CONSUMER}
                    )
                }
            )
        with pytest.raises(ServiceUseAuthorizationError) as rejected:
            await provider.authorize(message)
        assert rejected.value.code == (
            "invalid_service_payload" if fault == "payload" else "scope_mismatch"
        )
