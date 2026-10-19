"""Typed hardware operations bound to one exact managed claim."""

from collections.abc import Mapping
from dataclasses import dataclass

from deckr.contracts.messages import (
    HARDWARE_MESSAGES_LANE,
    DeckrMessage,
    message_is_expired,
)
from deckr.hardware import messages as hw
from deckr.hardware.descriptors import CapabilityRef, DeviceRef
from deckr.hardware.state import HardwareUnavailable
from deckr.hardware.validation import (
    resolve_capability,
    validate_command,
    validate_input,
    validate_state_request,
    validate_value,
)


@dataclass(frozen=True, slots=True)
class HardwareEvent:
    message: DeckrMessage
    body: hw.ControlInputMessage | hw.CapabilityStateChangedMessage


class HardwareOperations:
    def _target(self, device_id, capability_id, control_id):
        descriptor = self.descriptors.get(device_id)
        if descriptor is None:
            raise HardwareUnavailable(
                "device_scope_mismatch", "Device is outside this claim"
            )
        return CapabilityRef(
            deviceRef=DeviceRef(
                managerId=self.manager_endpoint.endpoint_id,
                deviceId=device_id,
                fingerprint=descriptor.fingerprint,
            ),
            capabilityId=capability_id,
            controlId=control_id,
        )

    async def command(
        self,
        device_id: str,
        capability_id: str,
        command_type: str,
        *,
        control_id: str | None = None,
        params: Mapping[str, object] | None = None,
    ) -> DeckrMessage:
        self.require_authority()
        ref = self._target(device_id, capability_id, control_id)
        capability = resolve_capability(self.descriptors, ref)
        validate_command(capability, command_type, params or {})
        body = hw.ControlCommandMessage(
            deviceRef=ref.device_ref,
            controlId=control_id,
            capabilityId=capability_id,
            commandType=command_type,
            params=params or {},
        )
        self.require_authority()
        message = await self._endpoint.send(
            **self._message_args(ref, hw.CONTROL_COMMAND, body)
        )
        self.require_authority()
        return message

    async def request_state(
        self,
        device_id: str,
        capability_id: str,
        *,
        control_id: str | None = None,
        state_type: str | None = None,
        params: Mapping[str, object] | None = None,
        timeout_seconds: float = 2.0,
    ) -> hw.CapabilityStateReplyMessage:
        self.require_authority()
        ref = self._target(device_id, capability_id, control_id)
        capability = resolve_capability(self.descriptors, ref)
        validate_state_request(capability)
        body = hw.CapabilityStateRequestMessage(
            deviceRef=ref.device_ref,
            controlId=control_id,
            capabilityId=capability_id,
            stateType=state_type,
            params=params or {},
        )
        self.require_authority()
        reply = await self._endpoint.request(
            **self._message_args(ref, hw.CAPABILITY_STATE_REQUEST, body),
            timeout=timeout_seconds,
        )
        self.require_authority()
        parsed = self._authorize_message(reply)
        if (
            not isinstance(parsed, hw.CapabilityStateReplyMessage)
            or parsed.control_id != control_id
            or parsed.capability_id != capability_id
            or parsed.device_ref.device_id != device_id
            or parsed.state_type != state_type
        ):
            raise HardwareUnavailable(
                "invalid_hardware_response",
                "Reply does not match the requested capability",
            )
        if parsed.status == "ok":
            validate_value(capability, parsed.value)
        return parsed

    def _message_args(self, ref, message_type, body):
        return dict(
            lane=HARDWARE_MESSAGES_LANE,
            recipient=self.manager_endpoint,
            recipient_session_id=self.manager_session_id,
            contract=self._agreement.contract.pointer,
            subject=hw.hardware_subject_for_capability(ref),
            message_type=message_type,
            body=hw.hardware_body_to_dict(body),
        )

    def _authorize_message(self, message):
        self.require_authority()
        if (
            message.lane != HARDWARE_MESSAGES_LANE
            or message_is_expired(message)
            or message.contract != self._agreement.contract.pointer
            or message.sender != self.manager_endpoint
            or message.sender_session_id != self.manager_session_id
            or getattr(message.recipient, "endpoint", None) != self._endpoint.address
            or message.recipient_session_id != self._endpoint.session_id
        ):
            raise HardwareUnavailable(
                "hardware_authority_mismatch",
                "Message does not belong to this hardware claim",
            )
        try:
            body = hw.hardware_body_from_message(message)
        except (TypeError, ValueError) as exc:
            raise HardwareUnavailable("invalid_hardware_message", str(exc)) from exc
        ref = CapabilityRef(
            deviceRef=body.device_ref,
            controlId=body.control_id,
            capabilityId=body.capability_id,
        )
        if (
            ref.device_ref.manager_id != self.manager_endpoint.endpoint_id
            or message.subject != hw.hardware_subject_for_capability(ref)
        ):
            raise HardwareUnavailable(
                "hardware_scope_mismatch",
                "Message subject and device reference do not match",
            )
        capability = resolve_capability(self.descriptors, ref)
        if isinstance(body, hw.ControlInputMessage):
            validate_input(capability, body.event_type, body.value)
        elif isinstance(body, hw.CapabilityStateChangedMessage):
            if "emits" not in capability.access:
                raise HardwareUnavailable(
                    "unsupported_state_event",
                    "State events are not declared by the descriptor",
                )
            validate_value(capability, body.value)
        return body
