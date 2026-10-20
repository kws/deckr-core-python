"""Wire round trips and runtime observations; shared cases live upstream."""

import json

import pytest
from message_bus_mocks import mock_deckr

from deckr.action_runtime import action_runtime_payload, validate_action_runtime_message
from deckr.actions.rendering import (
    BindingRenderBody,
    BindingRenderResultBody,
    RenderContent,
)
from deckr.contracts.messages import (
    HARDWARE_MESSAGES_LANE,
    DeckrMessage,
    endpoint_target,
    entity_subject,
)

TARGET = {"kind": "binding", "bindingId": "binding", "generation": 2}


def result_message():
    return DeckrMessage(
        lane="services",
        sender="controller:main",
        senderSessionId="c",
        recipient=endpoint_target("service:action-runtime.provider"),
        recipientSessionId="p",
        contract={"contractId": "agreement", "generation": 1},
        causationId="request",
        messageType="serviceMessage",
        subject=entity_subject(
            "service",
            serviceId="action-runtime.provider",
            namespace="dev.deckr.action_runtime.provider",
            name="binding_render_result",
        ),
        body={
            "serviceNamespace": "dev.deckr.action_runtime.provider",
            "name": "binding_render_result",
            "intent": "event",
            "exchangePattern": "one_way",
            "event": {
                "requestMessageId": "request",
                "target": TARGET,
                "requirementName": "display",
                "stage": "controller",
                "status": "dispatched",
                "commandMessageId": "command",
            },
        },
    )


def test_result_correlation_and_received_size_are_distinct_from_message_identity():
    message = result_message()
    validate_action_runtime_message(message)
    raw = json.dumps(message.to_dict(), indent=4).encode()
    received = DeckrMessage.from_bytes(raw)
    assert received == message
    assert received.wire_size_bytes == len(raw)
    assert message.wire_size_bytes < len(raw)
    assert "_received_size" not in received.to_dict()
    with pytest.raises(ValueError, match="causation"):
        validate_action_runtime_message(
            message.model_copy(update={"causation_id": "different"})
        )


def test_render_models_preserve_extension_data_and_reject_null_and_wrong_targets():
    body = {
        "target": TARGET,
        "requirementName": "display",
        "operation": "set",
        "content": {
            "format": "org.example.render.graph.v1",
            "data": {"input": [1, {"flag": None}]},
        },
    }
    typed = BindingRenderBody.from_wire(body)
    assert action_runtime_payload("binding_render", typed) == (body, None)
    with pytest.raises(TypeError):
        typed.content.data["input"] = []
    for invalid in [
        {**body, "content": None},
        {**body, "operation": "clear"},
        {
            **body,
            "target": {
                "kind": "page_session",
                "pageSessionId": "page",
                "generation": 1,
            },
        },
        {**body, "target": {**TARGET, "generation": True}},
    ]:
        with pytest.raises(ValueError):
            BindingRenderBody.from_wire(invalid)
    for data in [
        {"text": "hello", "font": "hidden"},
        {"text": None},
        {"text": "hello", "background": "red"},
    ]:
        with pytest.raises(ValueError):
            RenderContent(format="dev.deckr.render.text.v1", data=data)


def test_result_status_and_command_correlation_are_closed():
    valid = {
        "target": TARGET,
        "requirementName": "display",
        "requestMessageId": "request",
        "stage": "hardware",
        "status": "rejected",
        "code": "hardware_rejected",
        "commandMessageId": "command",
    }
    assert BindingRenderResultBody.from_wire(valid).to_dict() == valid
    for updates in [
        {"stage": "controller"},
        {"status": "dispatched"},
        {"code": "other"},
        {"commandMessageId": None},
    ]:
        with pytest.raises(ValueError):
            BindingRenderResultBody.from_wire(valid | updates)


@pytest.mark.asyncio
async def test_presupplied_hardware_message_id_is_validated_before_publish():
    async with mock_deckr() as runtime, runtime.endpoint("controller:main") as endpoint:
        args = dict(
            lane=HARDWARE_MESSAGES_LANE,
            recipient="hardware_manager:manager",
            subject=entity_subject("hardware", deviceId="device"),
            message_type="controlCommand",
            body={
                "deviceRef": {"managerId": "manager", "deviceId": "device"},
                "controlId": "key",
                "capabilityId": "raster",
                "commandType": "clear",
                "params": {},
            },
            contract={"contractId": "agreement", "generation": 1},
        )
        sent = await endpoint.send(**args, message_id="reserved-command")
        assert sent.message_id == "reserved-command"
        for invalid in ["", " padded ", 42]:
            with pytest.raises(ValueError):
                await endpoint.send(**args, message_id=invalid)
