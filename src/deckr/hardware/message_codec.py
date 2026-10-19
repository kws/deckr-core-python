"""Message codec."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from pydantic import JsonValue

from deckr.contracts.authority import ContractPointer
from deckr.contracts.messages import (
    HARDWARE_MESSAGES_LANE,
    DeckrMessage,
    EndpointAddress,
    EntitySubject,
    MessageTarget,
    controllers_broadcast,
    endpoint_target,
    entity_subject,
    hardware_manager_address,
)
from deckr.contracts.models import DeckrModel, JsonObject, thaw_json
from deckr.hardware.descriptors import CapabilityRef, DeviceRef, DeviceSourceReference
from deckr.hardware.message_bodies import (
    _HARDWARE_BODY_TYPES,
    CAPABILITY_STATE_CHANGED,
    CAPABILITY_STATE_REPLY,
    CAPABILITY_STATE_REQUEST,
    COMMAND_ACCEPTED,
    COMMAND_REJECTED,
    COMMAND_REPLY,
    CONTROL_COMMAND,
    CONTROL_INPUT,
    HARDWARE_BODY_BY_MESSAGE_TYPE,
    ControlCommandMessage,
    ControlInputMessage,
    HardwareMessageBody,
    _now_utc,
)


def hardware_subject_for_device(ref: DeviceRef) -> EntitySubject:
    return entity_subject(
        "hardware_device",
        managerId=ref.manager_id,
        deviceId=ref.device_id,
    )


def hardware_subject_for_capability(ref: CapabilityRef) -> EntitySubject:
    device = ref.device_ref
    if device is None:
        raise ValueError("capability subject requires deviceRef")
    identifiers = {
        "managerId": device.manager_id,
        "deviceId": device.device_id,
        "capabilityId": ref.capability_id,
    }
    if ref.control_id is not None:
        identifiers["controlId"] = ref.control_id
    return entity_subject(
        "hardware_capability",
        **identifiers,
    )


def hardware_capability_ref_from_subject(
    subject: EntitySubject,
) -> CapabilityRef | None:
    if subject.kind != "hardware_capability":
        return None
    ids = subject.identifiers
    manager_id = ids.get("managerId")
    device_id = ids.get("deviceId")
    control_id = ids.get("controlId")
    capability_id = ids.get("capabilityId")
    if manager_id is None or device_id is None or capability_id is None:
        return None
    return CapabilityRef(
        deviceRef=DeviceRef(managerId=manager_id, deviceId=device_id),
        controlId=control_id,
        capabilityId=capability_id,
    )


def hardware_body_for_type(
    message_type: str,
    body: HardwareMessageBody | Mapping[str, Any],
) -> HardwareMessageBody:
    body_type = HARDWARE_BODY_BY_MESSAGE_TYPE.get(message_type)
    if body_type is None:
        raise ValueError(f"Unsupported hardware message type {message_type!r}")
    if isinstance(body, _HARDWARE_BODY_TYPES):
        if not isinstance(body, body_type):
            raise TypeError(
                f"{message_type!r} requires body type {body_type.__name__}, "
                f"got {type(body).__name__}"
            )
        return body
    if isinstance(body, DeckrModel):
        raise TypeError(
            f"{message_type!r} requires body type {body_type.__name__}, "
            f"got {type(body).__name__}"
        )
    return body_type.from_wire(thaw_json(dict(body)))


def hardware_body_to_dict(body: HardwareMessageBody) -> dict[str, Any]:
    return body.model_dump(by_alias=True, exclude_none=True, mode="json")


def hardware_body_from_message(message: DeckrMessage) -> HardwareMessageBody:
    return hardware_body_for_type(message.message_type, message.body)


def hardware_device_ref_from_message(message: DeckrMessage) -> DeviceRef | None:
    """Return the manager-local device ref for capability-targeted hardware traffic."""
    if message.message_type in {
        CAPABILITY_STATE_CHANGED,
        CAPABILITY_STATE_REPLY,
        CAPABILITY_STATE_REQUEST,
        COMMAND_ACCEPTED,
        COMMAND_REJECTED,
        COMMAND_REPLY,
        CONTROL_INPUT,
        CONTROL_COMMAND,
    }:
        body = HARDWARE_BODY_BY_MESSAGE_TYPE[message.message_type].from_wire(
            thaw_json(dict(message.body))
        )
        return body.device_ref
    return None


def hardware_message(
    *,
    sender: str | EndpointAddress,
    sender_session_id: str,
    recipient: str | EndpointAddress | MessageTarget,
    recipient_session_id: str | None = None,
    message_type: str,
    body: HardwareMessageBody | Mapping[str, Any],
    subject: EntitySubject,
    in_reply_to: str | None = None,
    causation_id: str | None = None,
    contract: ContractPointer | Mapping[str, Any] | None = None,
) -> DeckrMessage:
    target = (
        recipient
        if not isinstance(recipient, str | EndpointAddress)
        else endpoint_target(recipient)
    )
    parsed_body = hardware_body_for_type(message_type, body)
    return DeckrMessage(
        lane=HARDWARE_MESSAGES_LANE,
        messageType=message_type,
        sender=sender,
        senderSessionId=sender_session_id,
        recipient=target,
        recipientSessionId=recipient_session_id,
        subject=subject,
        body=hardware_body_to_dict(parsed_body),
        inReplyTo=in_reply_to,
        causationId=causation_id,
        contract=contract,
    )


def control_input_message(
    *,
    manager_id: str,
    sender_session_id: str,
    device_id: str,
    fingerprint: str | None = None,
    control_id: str,
    capability_id: str,
    event_type: str,
    value: JsonValue | None = None,
    sequence: int | None = None,
    occurred_at: datetime | None = None,
    sources: tuple[DeviceSourceReference, ...] = (),
    contract: ContractPointer | Mapping[str, Any] | None = None,
) -> DeckrMessage:
    device_ref = DeviceRef(
        managerId=manager_id,
        deviceId=device_id,
        fingerprint=fingerprint,
    )
    body = ControlInputMessage(
        deviceRef=device_ref,
        controlId=control_id,
        capabilityId=capability_id,
        eventType=event_type,
        value=value,
        sequence=sequence,
        occurredAt=occurred_at or _now_utc(),
        sources=sources,
    )
    return hardware_message(
        sender=hardware_manager_address(manager_id),
        sender_session_id=sender_session_id,
        recipient=controllers_broadcast(),
        message_type=CONTROL_INPUT,
        body=body,
        subject=hardware_subject_for_capability(
            CapabilityRef(
                deviceRef=DeviceRef(managerId=manager_id, deviceId=device_id),
                controlId=control_id,
                capabilityId=capability_id,
            )
        ),
        contract=contract,
    )


def control_command_for_capability(
    *,
    controller_id: str,
    sender_session_id: str,
    ref: CapabilityRef,
    command_type: str,
    params: JsonObject | None = None,
    recipient_session_id: str | None = None,
    contract: ContractPointer | Mapping[str, Any] | None = None,
) -> DeckrMessage:
    device = ref.device_ref
    if device is None:
        raise ValueError("capability command requires deviceRef")
    control_id = ref.control_id
    return control_command_message(
        controller_id=controller_id,
        sender_session_id=sender_session_id,
        manager_id=device.manager_id,
        device_id=device.device_id,
        control_id=control_id,
        capability_id=ref.capability_id,
        command_type=command_type,
        params=params,
        recipient_session_id=recipient_session_id,
        contract=contract,
    )


def control_command_message(
    *,
    controller_id: str,
    sender_session_id: str,
    manager_id: str,
    device_id: str,
    capability_id: str,
    command_type: str,
    control_id: str | None = None,
    params: JsonObject | None = None,
    recipient_session_id: str | None = None,
    contract: ContractPointer | Mapping[str, Any] | None = None,
) -> DeckrMessage:
    device_ref = DeviceRef(managerId=manager_id, deviceId=device_id)
    body = ControlCommandMessage(
        deviceRef=device_ref,
        controlId=control_id,
        capabilityId=capability_id,
        commandType=command_type,
        params=dict(params or {}),
    )
    return hardware_message(
        sender=f"controller:{controller_id}",
        sender_session_id=sender_session_id,
        recipient=endpoint_target(hardware_manager_address(manager_id)),
        recipient_session_id=recipient_session_id,
        message_type=CONTROL_COMMAND,
        body=body,
        subject=hardware_subject_for_capability(
            CapabilityRef(
                deviceRef=device_ref,
                controlId=control_id,
                capabilityId=capability_id,
            )
        ),
        contract=contract,
    )
