"""Managed hardware command validation and retained device authority."""

import anyio

from deckr.contracts.messages import HARDWARE_MESSAGES_LANE, DeckrMessage
from deckr.hardware import messages as hw_messages
from deckr.hardware.claim_policy import device_reference_matches
from deckr.hardware.claims import LiveHardwareClaim, logger
from deckr.hardware.command_context import HardwareCommandContext
from deckr.hardware.command_replies import reject_hardware_command
from deckr.hardware.descriptors import CapabilityRef
from deckr.hardware.state import HardwareFailure, HardwareUnavailable
from deckr.hardware.validation import (
    resolve_capability,
    validate_command,
    validate_state_request,
)


class HardwareManagerCommands:
    async def _command_subscription_loop(
        self, *, task_status=anyio.TASK_STATUS_IGNORED
    ) -> None:
        started = False
        try:
            async with self.endpoint.subscribe(HARDWARE_MESSAGES_LANE) as stream:
                task_status.started()
                started = True
                async for envelope in stream:
                    await self._handle_command(envelope)
        except Exception:
            if not started:
                raise
            logger.exception("Hardware manager command subscription failed")
        finally:
            if started and not self._closed:
                with anyio.CancelScope(shield=True):
                    self._observer.failure = HardwareFailure(
                        "hardware_subscription_closed",
                        "Hardware command subscription closed",
                    )
                    await self.stop()

    async def _handle_command(self, envelope: DeckrMessage) -> bool:
        ref = hw_messages.hardware_device_ref_from_message(envelope)
        if ref is None or ref.manager_id != self.manager_id:
            return False
        body = hw_messages.hardware_body_from_message(envelope)
        if not isinstance(
            body,
            hw_messages.ControlCommandMessage
            | hw_messages.CapabilityStateRequestMessage,
        ):
            return False
        if not device_reference_matches(ref, self.endpoint.address, self._devices):
            await reject_hardware_command(self.endpoint, envelope, body, reason="stale")
            return False
        claim = await self._command_claim(ref.device_id, envelope)
        if claim is None:
            await reject_hardware_command(
                self.endpoint, envelope, body, reason="unauthorized"
            )
            return False
        if envelope.sender_session_id != claim.controller_session_id:
            await reject_hardware_command(self.endpoint, envelope, body, reason="stale")
            return False
        target = CapabilityRef(
            deviceRef=body.device_ref,
            controlId=body.control_id,
            capabilityId=body.capability_id,
        )
        if envelope.subject != hw_messages.hardware_subject_for_capability(target):
            await reject_hardware_command(
                self.endpoint, envelope, body, reason="malformed"
            )
            return False
        try:
            capability = resolve_capability(self._devices, target)
            if isinstance(body, hw_messages.ControlCommandMessage):
                validate_command(capability, body.command_type, body.params)
            else:
                validate_state_request(capability)
        except HardwareUnavailable as exc:
            await reject_hardware_command(
                self.endpoint,
                envelope,
                body,
                reason=(
                    "malformed"
                    if exc.code == "invalid_capability_payload"
                    else "unsupported"
                ),
            )
            return False
        if self.command_handler is None:
            await reject_hardware_command(
                self.endpoint, envelope, body, reason="unsupported"
            )
            return False
        try:
            context = self._command_context(envelope, claim, ref.device_id)
            context.authorize()
            handled = await self.command_handler(context)
        except (
            HardwareUnavailable,
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
        ):
            handled = False
        if handled is None:
            await reject_hardware_command(
                self.endpoint, envelope, body, reason="unsupported"
            )
            return False
        if not handled:
            await reject_hardware_command(self.endpoint, envelope, body, reason="stale")
            return False
        return True

    def _command_context(
        self, envelope: DeckrMessage, claim: LiveHardwareClaim, device_id: str
    ) -> HardwareCommandContext:
        epoch = self._device_epochs[device_id]
        descriptor = self._devices[device_id]

        def authorize() -> None:
            current = self._claims_by_device.get(device_id)
            if (
                self._device_epochs.get(device_id) is not epoch
                or current is None
                or current.contract.pointer != claim.contract.pointer
                or current.controller_session_id != claim.controller_session_id
                or not self._claim_has_authority(claim)
            ):
                raise HardwareUnavailable(
                    "hardware_authority_lost", "Hardware command authority has ended"
                )

        return HardwareCommandContext(envelope, descriptor, authorize)

    async def _command_claim(
        self,
        device_id: str,
        envelope: DeckrMessage,
    ) -> LiveHardwareClaim | None:
        if self._closed:
            return None
        if envelope.contract is None:
            return None
        claim = self._claims_by_device.get(device_id)
        if claim is None or envelope.sender != claim.controller_endpoint:
            return None
        if (
            envelope.contract.contract_id != claim.contract.contract_id
            or envelope.contract.generation != claim.contract.generation
        ):
            return None
        return claim if self._claim_has_authority(claim) else None

    def _claim_has_authority(self, claim: LiveHardwareClaim) -> bool:
        managed = self._claim_manager.managed_contract(claim.contract)
        return (
            not self._closed
            and managed is not None
            and managed.local_authority
            and managed.validity.valid
        )
