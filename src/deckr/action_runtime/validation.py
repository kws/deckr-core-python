"""Action Runtime checks against current, application-supplied observations.

Page validation must run again at commit while the application serializes page
transitions. These facts do not acquire or retain authority, or allocate pages.
"""

from collections.abc import Mapping

from pydantic import Field

from deckr.action_runtime.protocol import (
    ACTION_RUNTIME_SERVICE_NAMESPACE,
    CONTROLLER_TO_RUNTIME_MESSAGES,
    EVENT_MESSAGES,
    RUNTIME_TO_CONTROLLER_MESSAGES,
    action_runtime_body_from_service_message,
    action_runtime_provider_instance_id,
)
from deckr.actions.pages import (
    BindingCommandTarget,
    ClosePageBody,
    OpenPageBody,
    ReplacePageBody,
)
from deckr.actions.rendering import BindingRenderResultBody
from deckr.contracts.messages import ContractPointer, DeckrMessage
from deckr.contracts.models import DeckrModel, thaw_json
from deckr.hardware.descriptors import DeviceRef
from deckr.services import ServiceMessageBody


def validate_action_runtime_message(
    message: DeckrMessage, *, validate_payload: bool = True
) -> None:
    """Validate the typed payload, operation direction, and service subject."""
    body = ServiceMessageBody.from_wire(message.body)
    if (
        message.lane != "services"
        or message.message_type != "serviceMessage"
        or body.service_namespace != ACTION_RUNTIME_SERVICE_NAMESPACE
        or body.name
        not in CONTROLLER_TO_RUNTIME_MESSAGES | RUNTIME_TO_CONTROLLER_MESSAGES
        or body.exchange_pattern != "one_way"
        or body.intent != ("event" if body.name in EVENT_MESSAGES else "command")
        or message.contract is None
        or not message.sender_session_id
        or not message.recipient_session_id
        or message.in_reply_to is not None
        or any(value is not None for value in (body.status, body.result, body.error))
    ):
        raise ValueError("invalid Action Runtime service envelope")
    sender = str(message.sender)
    recipient = str(getattr(message.recipient, "endpoint", ""))
    runtime, controller = (
        (recipient, sender)
        if body.name in CONTROLLER_TO_RUNTIME_MESSAGES
        else (sender, recipient)
    )
    service_id = runtime.removeprefix("service:")
    provider_instance_id = action_runtime_provider_instance_id(service_id)
    if not runtime.startswith("service:action-runtime.") or not controller.startswith(
        "controller:"
    ):
        raise ValueError("Action Runtime endpoints do not match operation direction")
    if provider_instance_id is None:
        raise ValueError("invalid runtime provider instance ID")
    if message.subject.kind != "service" or dict(message.subject.identifiers) != {
        "serviceId": service_id,
        "namespace": ACTION_RUNTIME_SERVICE_NAMESPACE,
        "name": body.name,
    }:
        raise ValueError(
            "Action Runtime subject must identify the runtime service and operation"
        )
    if body.name in EVENT_MESSAGES:
        if body.event is None or body.params:
            raise ValueError(
                "Action Runtime events require event payloads and empty params"
            )
    elif body.event is not None:
        raise ValueError("Action Runtime commands cannot carry event payloads")
    if not validate_payload:
        return
    payload = action_runtime_body_from_service_message(body)
    if (
        isinstance(payload, BindingRenderResultBody)
        and message.causation_id != payload.request_message_id
    ):
        raise ValueError("render result causation must identify its request")
    for field in ("metadata", "binding", "page_session", "action_instance"):
        metadata = getattr(payload, field, None)
        if (
            metadata is not None
            and metadata.provider_instance_id != provider_instance_id
        ):
            raise ValueError(
                "Action Runtime metadata provider instance differs from the service"
            )


class PageTargetState(DeckrModel):
    """Retained target ownership and scope from the application's current state."""

    action_instance_id: str = Field(alias="actionInstanceId")
    active: bool
    generation: int = Field(ge=0, strict=True)
    contract: ContractPointer
    runtime_endpoint: str = Field(alias="runtimeEndpoint")
    runtime_session_id: str = Field(alias="runtimeSessionId")
    device_ref: DeviceRef = Field(alias="deviceRef")
    control_ids: tuple[str, ...] = Field(alias="controlIds")
    page_id: str | None = Field(default=None, alias="pageId")


class PageCommandContext(DeckrModel):
    """Current observations supplied by the owner of the page transition."""

    contract: ContractPointer
    controller_endpoint: str = Field(alias="controllerEndpoint")
    controller_session_id: str = Field(alias="controllerSessionId")
    runtime_endpoint: str = Field(alias="runtimeEndpoint")
    runtime_session_id: str = Field(alias="runtimeSessionId")
    local_authority: bool = Field(alias="localAuthority")
    hardware_authority: bool = Field(alias="hardwareAuthority")
    bindings: Mapping[str, PageTargetState] = Field(default_factory=dict)
    page_sessions: Mapping[str, PageTargetState] = Field(
        default_factory=dict, alias="pageSessions"
    )


def validate_page_command(
    message: DeckrMessage, context: PageCommandContext
) -> PageTargetState:
    """Return the retained target after validating current authority and scope."""
    validate_action_runtime_message(message)
    body = action_runtime_body_from_service_message(
        ServiceMessageBody.model_validate(thaw_json(message.body))
    )
    if not isinstance(body, OpenPageBody | ReplacePageBody | ClosePageBody):
        raise ValueError("message is not a page command")
    if (
        not context.local_authority
        or not context.hardware_authority
        or message.contract != context.contract
        or str(message.sender) != context.runtime_endpoint
        or message.sender_session_id != context.runtime_session_id
        or str(getattr(message.recipient, "endpoint", ""))
        != context.controller_endpoint
        or message.recipient_session_id != context.controller_session_id
    ):
        raise ValueError("page command has no current service or hardware authority")
    target = body.target
    current = (
        context.bindings.get(target.binding_id)
        if isinstance(target, BindingCommandTarget)
        else context.page_sessions.get(target.page_session_id)
    )
    if (
        current is None
        or not current.active
        or current.generation != target.generation
        or current.runtime_endpoint != context.runtime_endpoint
        or current.runtime_session_id != context.runtime_session_id
        or current.contract != context.contract
    ):
        raise ValueError(
            "page target is stale, inactive, or belongs to another agreement"
        )
    if isinstance(body, OpenPageBody | ReplacePageBody):
        if not {child.control_id for child in body.descriptor.bindings} <= set(
            current.control_ids
        ):
            raise ValueError(
                "page child is outside the retained device and claim scope"
            )
        if (
            isinstance(body, ReplacePageBody)
            and body.descriptor.page_id != current.page_id
        ):
            raise ValueError("replacement must preserve the retained page ID")
    return current
