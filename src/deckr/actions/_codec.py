"""Codec."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from deckr.actions._body import ActionMessageBody, _require_text
from deckr.actions.endpoints import (
    action_provider_address,
    require_provider_instance_id,
)
from deckr.actions.message_types import ACTION_BODY_BY_MESSAGE_TYPE
from deckr.actions.pages import OpenPageBody, ReplacePageBody
from deckr.contracts.authority import ContractPointer
from deckr.contracts.messages import (
    ACTIONS_LANE,
    BroadcastTarget,
    DeckrMessage,
    EndpointAddress,
    EndpointTarget,
    EntitySubject,
    MessageTarget,
    controller_address,
    endpoint_target,
    entity_subject,
    message_targets_endpoint,
)


def _target(
    recipient: str | EndpointAddress | MessageTarget,
) -> MessageTarget:
    if isinstance(recipient, EndpointTarget | BroadcastTarget):
        return recipient
    return endpoint_target(recipient)


def action_message(
    *,
    sender: str | EndpointAddress,
    sender_session_id: str,
    recipient: str | EndpointAddress | MessageTarget,
    recipient_session_id: str | None = None,
    message_type: str,
    body: ActionMessageBody | Mapping[str, Any] | None = None,
    subject: EntitySubject,
    in_reply_to: str | None = None,
    causation_id: str | None = None,
    contract: ContractPointer | Mapping[str, Any] | None = None,
) -> DeckrMessage:
    parsed_body = action_body_for_type(message_type, body or {})
    return DeckrMessage(
        lane=ACTIONS_LANE,
        messageType=message_type,
        sender=sender,
        senderSessionId=sender_session_id,
        recipient=_target(recipient),
        recipientSessionId=recipient_session_id,
        subject=subject,
        body=parsed_body.to_dict(),
        inReplyTo=in_reply_to,
        causationId=causation_id,
        contract=contract,
    )


def action_body_for_type(
    message_type: str,
    body: ActionMessageBody | Mapping[str, Any],
) -> ActionMessageBody:
    body_type = ACTION_BODY_BY_MESSAGE_TYPE.get(message_type)
    if body_type is None:
        raise ValueError(f"Unsupported action message type {message_type!r}")
    if isinstance(body, ActionMessageBody):
        if not isinstance(body, body_type):
            raise TypeError(
                f"{message_type!r} requires body type {body_type.__name__}, "
                f"got {type(body).__name__}"
            )
        return body
    return body_type.from_wire(body)


def action_body(message: DeckrMessage) -> ActionMessageBody:
    return action_body_for_type(message.message_type, message.body)


def action_body_dict(message: DeckrMessage) -> Mapping[str, Any]:
    return action_body(message).to_dict()


def action_message_for_provider(
    message: DeckrMessage,
    provider_instance_id: str,
) -> bool:
    return message_targets_endpoint(
        message, action_provider_address(provider_instance_id)
    )


def action_message_for_controller(
    message: DeckrMessage,
    controller_id: str | None = None,
) -> bool:
    if controller_id is None:
        return (
            message.recipient.endpoint_family == "controller"
            if isinstance(message.recipient, BroadcastTarget)
            else message.recipient.endpoint.family == "controller"
        )
    return message_targets_endpoint(message, controller_address(controller_id))


def context_subject(
    context_id: str,
    *,
    provider_instance_id: str | None = None,
    provider_id: str | None = None,
    config_id: str | None = None,
    action_instance_id: str | None = None,
    binding_id: str | None = None,
    page_session_id: str | None = None,
) -> EntitySubject:
    identifiers: dict[str, str] = {"contextId": context_id}
    if provider_instance_id is not None:
        identifiers["providerInstanceId"] = require_provider_instance_id(
            provider_instance_id,
            field_name="providerInstanceId",
        )
    if provider_id is not None:
        identifiers["providerId"] = _require_text(provider_id, field_name="provider id")
    if config_id is not None:
        identifiers["configId"] = config_id
    if action_instance_id is not None:
        identifiers["actionInstanceId"] = action_instance_id
    if binding_id is not None:
        identifiers["bindingId"] = binding_id
    if page_session_id is not None:
        identifiers["pageSessionId"] = page_session_id
    return entity_subject("context", **identifiers)


def subject_context_id(subject: EntitySubject) -> str | None:
    value = subject.identifiers.get("contextId")
    return str(value) if value is not None else None


def subject_config_id(subject: EntitySubject) -> str | None:
    value = subject.identifiers.get("configId")
    return str(value) if value is not None else None


def subject_action_instance_id(subject: EntitySubject) -> str | None:
    value = subject.identifiers.get("actionInstanceId")
    return str(value) if value is not None else None


def subject_binding_id(subject: EntitySubject) -> str | None:
    value = subject.identifiers.get("bindingId")
    return str(value) if value is not None else None


def subject_page_session_id(subject: EntitySubject) -> str | None:
    value = subject.identifiers.get("pageSessionId")
    return str(value) if value is not None else None


def subject_provider_instance_id(subject: EntitySubject) -> str | None:
    value = subject.identifiers.get("providerInstanceId")
    return str(value) if value is not None else None


def subject_provider_id(subject: EntitySubject) -> str | None:
    value = subject.identifiers.get("providerId")
    return str(value) if value is not None else None


def action_provider_instance_subject(
    provider_instance_id: str,
    *,
    provider_id: str,
) -> EntitySubject:
    return entity_subject(
        "action_provider_instance",
        providerInstanceId=require_provider_instance_id(
            provider_instance_id,
            field_name="providerInstanceId",
        ),
        providerId=_require_text(provider_id, field_name="provider id"),
    )


OpenPageBody.model_rebuild()


ReplacePageBody.model_rebuild()
