"""Typed negative replies for hardware commands and state requests."""

from __future__ import annotations

from deckr.contracts.messages import DeckrMessage
from deckr.hardware import messages as hw_messages
from deckr.hardware.claims import _HardwareEndpoint


async def reject_hardware_command(
    endpoint: _HardwareEndpoint,
    envelope: DeckrMessage,
    body: hw_messages.ControlCommandMessage | hw_messages.CapabilityStateRequestMessage,
    *,
    reason: hw_messages.CommandRejectionReason,
) -> None:
    if envelope.contract is None:
        return
    if isinstance(body, hw_messages.ControlCommandMessage):
        reply_body = hw_messages.CommandRejectedMessage(
            deviceRef=body.device_ref,
            controlId=body.control_id,
            capabilityId=body.capability_id,
            commandType=body.command_type,
            reason=reason,
            message=f"Hardware command {reason}",
        )
        await endpoint.reply_to(
            envelope,
            message_type=hw_messages.COMMAND_REJECTED,
            body=hw_messages.hardware_body_to_dict(reply_body),
            subject=envelope.subject,
            causation_id=envelope.causation_id,
        )
        return
    reply_body = hw_messages.CapabilityStateReplyMessage(
        deviceRef=body.device_ref,
        controlId=body.control_id,
        capabilityId=body.capability_id,
        stateType=body.state_type,
        status="rejected" if reason != "unsupported" else "unsupported",
        error=f"Hardware state request {reason}",
    )
    await endpoint.reply_to(
        envelope,
        message_type=hw_messages.CAPABILITY_STATE_REPLY,
        body=hw_messages.hardware_body_to_dict(reply_body),
        subject=envelope.subject,
        causation_id=envelope.causation_id,
    )
