"""Rendering admission against current observations, without a rendering engine."""

from __future__ import annotations

from collections.abc import Mapping

from pydantic import Field

from deckr.action_runtime.protocol import action_runtime_body_from_service_message
from deckr.action_runtime.validation import validate_action_runtime_message
from deckr.actions.bindings import BindingMetadata, MatchedCapability
from deckr.actions.rendering import (
    RENDER_IMAGE_FORMAT,
    RENDER_TEXT_FORMAT,
    BindingRenderBody,
    RenderingSupport,
    RenderTargetSupport,
)
from deckr.contracts.messages import ContractPointer, DeckrMessage
from deckr.contracts.models import DeckrModel
from deckr.services import ServiceMessageBody


class RenderRejection(ValueError):
    """A stable rendering admission code; unauthorized requests get no result."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class RenderBindingState(DeckrModel):
    binding: BindingMetadata
    rendering: RenderingSupport | None = None
    active: bool
    contract: ContractPointer
    runtime_endpoint: str
    runtime_session_id: str


class RenderCommandContext(DeckrModel):
    contract: ContractPointer
    controller_endpoint: str
    controller_session_id: str
    runtime_endpoint: str
    runtime_session_id: str
    local_authority: bool
    hardware_authority: bool
    bindings: Mapping[str, RenderBindingState] = Field(default_factory=dict)
    message_bytes: int = Field(ge=0, strict=True)


def render_match(binding: BindingMetadata, name: str) -> MatchedCapability:
    matches = [m for m in binding.matched_capabilities if m.requirement_name == name]
    if len(matches) != 1:
        raise RenderRejection("unsupported_target")
    match = matches[0]
    if (
        match.family != "dev.deckr.output.raster"
        or match.capability_type != "bitmap"
        or match.direction != "output"
        or not {"set_frame", "clear"} <= set(match.command_types)
        or match.capability.device_ref != binding.device_ref
        or match.capability.control_id != binding.control_ref.control_id
    ):
        raise RenderRejection("unsupported_target")
    return match


def validate_rendering_support(
    binding: BindingMetadata, support: RenderingSupport
) -> None:
    """Check matches and equal support for aliases of one physical output."""
    aliases: dict[str, dict] = {}
    for target in support.targets:
        match = render_match(binding, target.requirement_name)
        key = match.capability.capability_id
        shape = target.model_dump(
            by_alias=True, exclude={"requirement_name"}, exclude_none=True
        )
        if key in aliases and aliases[key] != shape:
            raise RenderRejection("unsupported_target")
        aliases[key] = shape


def validate_render_command(
    message: DeckrMessage, context: RenderCommandContext
) -> tuple[RenderBindingState, RenderTargetSupport]:
    """Validate current admission facts. Call again at final controller dispatch."""
    validate_action_runtime_message(message)
    body = action_runtime_body_from_service_message(
        ServiceMessageBody.from_wire(message.body)
    )
    if not isinstance(body, BindingRenderBody):
        raise ValueError("message is not binding_render")
    if (
        not context.local_authority
        or message.contract != context.contract
        or str(message.sender) != context.runtime_endpoint
        or message.sender_session_id != context.runtime_session_id
        or str(getattr(message.recipient, "endpoint", ""))
        != context.controller_endpoint
        or message.recipient_session_id != context.controller_session_id
    ):
        raise RenderRejection("unauthorized")
    current = context.bindings.get(body.target.binding_id)
    if (
        current is None
        or not current.active
        or not context.hardware_authority
        or current.binding.binding_id != body.target.binding_id
        or current.binding.output_generation != body.target.generation
        or current.contract != context.contract
        or current.runtime_endpoint != context.runtime_endpoint
        or current.runtime_session_id != context.runtime_session_id
    ):
        raise RenderRejection("target_not_current")
    support = current.rendering
    if support is None:
        raise RenderRejection("unsupported_target")
    validate_rendering_support(current.binding, support)
    target = next(
        (t for t in support.targets if t.requirement_name == body.requirement_name),
        None,
    )
    if target is None:
        raise RenderRejection("unsupported_target")
    if context.message_bytes > target.limits.max_message_bytes:
        raise RenderRejection("limit_exceeded")
    if body.content is not None:
        content = body.content
        selected = next((f for f in target.formats if f.id == content.format), None)
        if selected is None:
            raise RenderRejection("unsupported_format")
        if content.format == RENDER_IMAGE_FORMAT:
            if (
                selected.options is None
                or content.data["encoding"] not in selected.options["encodings"]
            ):
                raise RenderRejection("unsupported_format")
        elif content.format == RENDER_TEXT_FORMAT:
            if len(content.data["text"]) > target.limits.max_text_code_points:
                raise RenderRejection("limit_exceeded")
    return current, target
