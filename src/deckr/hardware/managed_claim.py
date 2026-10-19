"""One context owns one hardware agreement and its authorized operations."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from types import MappingProxyType
from typing import TYPE_CHECKING
from uuid import uuid4

import anyio

from deckr.concord import (
    ConcordAgreementSpec,
    ConcordConflict,
    ConcordLifecyclePhase,
    ConcordUnavailable,
)
from deckr.contracts.messages import message_is_expired
from deckr.core.util.guarded_stream import guarded_changes
from deckr.core.util.owned_tasks import OwnedTaskGroup
from deckr.hardware import messages as hw
from deckr.hardware._state import HardwareStateChannel
from deckr.hardware.claim_spec import HardwareClaimSpec
from deckr.hardware.operations import HardwareEvent, HardwareOperations
from deckr.hardware.profiles import (
    HARDWARE_CLAIM_PROFILE_ID,
    HardwareClaimDevice,
    HardwareClaimTerms,
)
from deckr.hardware.state import (
    HardwareClaimState,
    HardwareFailure,
    HardwarePhase,
    HardwareUnavailable,
)

if TYPE_CHECKING:
    from deckr.hardware.client import DeckrHardware


class ManagedHardwareClaim(HardwareOperations):
    def __init__(self, client: DeckrHardware, spec: HardwareClaimSpec) -> None:
        self._client = client
        self.spec = spec
        self._endpoint = client.endpoint
        first = spec.candidates[0]
        self.manager_endpoint = first.manager_endpoint
        self.manager_session_id = first.manager_session_id
        self.descriptors = MappingProxyType(
            {c.device_ref.device_id: c.descriptor for c in spec.candidates}
        )
        self._agreement = client._resources.concord.propose(
            self._endpoint,
            ConcordAgreementSpec(
                profile=HARDWARE_CLAIM_PROFILE_ID,
                participants=(self._endpoint.address, self.manager_endpoint),
                participant_sessions={
                    str(self.manager_endpoint): self.manager_session_id
                },
                terms=HardwareClaimTerms(
                    claimId=str(uuid4()),
                    controllerEndpoint=self._endpoint.address,
                    managerEndpoint=self.manager_endpoint,
                    devices=tuple(
                        HardwareClaimDevice(deviceRef=c.device_ref, instanceCount=1)
                        for c in spec.candidates
                    ),
                ),
            ),
        )
        self._tasks = OwnedTaskGroup(
            client._resources.task_group,
            not_started_message="Hardware client must be running",
        )
        self._phase = HardwarePhase.STARTING
        self._failure = None
        self._state = HardwareStateChannel(self._snapshot)
        self._send: anyio.abc.ObjectSendStream[HardwareEvent] | None = None
        self._receive: anyio.abc.ObjectReceiveStream[HardwareEvent] | None = None
        self._entered = False
        self._closed = False
        self._close_lock = anyio.Lock()
        self._close_started = False
        self._close_done = anyio.Event()
        self._enter_scope = None
        self._enter_task = None
        self._enter_done = anyio.Event()
        self._failed = anyio.Event()

    def _snapshot(self) -> HardwareClaimState:
        state = self._agreement.state
        authority = (
            not self._closed
            and not self._client.closed
            and self._failure is None
            and state.valid
        )
        phase = self._phase
        failure = self._failure
        if phase == HardwarePhase.READY and not authority:
            phase = HardwarePhase.UNAVAILABLE
            failure = failure or HardwareFailure(
                "claim_authority_lost", state.reason or "Hardware claim lost authority"
            )
        return HardwareClaimState(
            0,
            phase,
            state.contract.pointer if state.contract else None,
            authority,
            self.descriptors,
            failure,
        )

    @property
    def state(self) -> HardwareClaimState:
        return self._state.state

    def watch_state(
        self,
    ) -> AbstractAsyncContextManager[AsyncIterator[HardwareClaimState]]:
        return self._state.watch()

    async def wait_for_state(
        self,
        predicate: Callable[[HardwareClaimState], bool],
        *,
        timeout_seconds: float | None = None,
    ) -> HardwareClaimState:
        return await self._state.wait_for(predicate, timeout_seconds=timeout_seconds)

    def require_authority(self) -> None:
        state = self.state
        if self._client.closed or not state.local_authority:
            failure = state.failure or HardwareFailure(
                "claim_unavailable", "Hardware claim has no local authority"
            )
            raise HardwareUnavailable(failure.code, failure.message)

    async def __aenter__(self) -> ManagedHardwareClaim:
        if self._entered or self._closed:
            raise RuntimeError("Hardware claims cannot be re-entered")
        self._entered = True
        self._enter_task = anyio.get_current_task().id
        try:
            with anyio.CancelScope() as scope:
                self._enter_scope = scope
                with anyio.fail_after(self.spec.timeout_seconds):
                    self._client._admit(self)
                    for candidate in self.spec.candidates:
                        self._client.directory.require_current(candidate)
                    group = await self._tasks.start()
                    try:
                        await self._agreement.__aenter__()
                    except (ConcordConflict, ConcordUnavailable) as exc:
                        raise HardwareUnavailable(
                            "claim_negotiation_failed", str(exc)
                        ) from exc
                    self._client._router.add(self)
                    group.start_soon(self._watch_agreement)
                    group.start_soon(self._cleanup_failure)
                    state = await self._agreement.wait_for_state(
                        lambda s: s.valid
                        or s.phase
                        in {
                            ConcordLifecyclePhase.TERMINAL,
                            ConcordLifecyclePhase.UNAVAILABLE,
                            ConcordLifecyclePhase.CLOSED,
                        }
                    )
                    if not state.valid:
                        raise HardwareUnavailable(
                            "claim_negotiation_failed",
                            state.reason or "Hardware negotiation failed",
                        )
                    self._phase = HardwarePhase.READY
                    await self._state.publish()
                    self.require_authority()
                    return self
            raise anyio.ClosedResourceError
        except BaseException:
            await self.aclose()
            raise
        finally:
            self._enter_scope = None
            self._enter_done.set()

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def _watch_agreement(self):
        async with self._agreement.watch_state() as states:
            async for _state in states:
                await self._state.publish()

    async def _cleanup_failure(self):
        await self._failed.wait()
        await self._agreement.aclose()
        await self._state.publish()

    async def _fail(self, code, message):
        if self._failure is None:
            self._failure = HardwareFailure(code, message)
            self._phase = HardwarePhase.UNAVAILABLE
            self._failed.set()
            await self._state.publish()

    async def _deliver(self, message):
        if self._send is None:
            return
        try:
            body = self._authorize_message(message)
        except (HardwareUnavailable, TypeError, ValueError):
            return
        if not isinstance(
            body, hw.ControlInputMessage | hw.CapabilityStateChangedMessage
        ):
            return
        try:
            self._send.send_nowait(HardwareEvent(message, body))
        except anyio.WouldBlock:
            await self._fail(
                "input_overflow",
                "Hardware input consumer fell behind; the claim was revoked",
            )
        except (anyio.ClosedResourceError, anyio.BrokenResourceError):
            pass

    @asynccontextmanager
    async def events(self) -> AsyncIterator[AsyncIterator[HardwareEvent]]:
        self.require_authority()
        if self._send is not None:
            raise RuntimeError("A claim supports one ordered event consumer")
        send, receive = anyio.create_memory_object_stream[HardwareEvent](
            max_buffer_size=self.spec.event_buffer_size
        )
        self._send, self._receive = send, receive

        async def check():
            if self._send is not send:
                raise anyio.ClosedResourceError
            self.require_authority()

        async def incoming():
            async for event in receive:
                # Expiry applies at delivery, including time spent in our queue.
                if not message_is_expired(event.message):
                    yield event

        try:
            yield guarded_changes(incoming(), check=check, watch=self.watch_state)
        finally:
            self._send = self._receive = None
            with anyio.CancelScope(shield=True):
                await send.aclose()
                await receive.aclose()
                await self._state.publish()

    async def aclose(self) -> None:
        with anyio.CancelScope(shield=True):
            async with self._close_lock:
                if self._close_started:
                    first = False
                else:
                    first = True
                    self._close_started = True
                    self._closed = True
            if not first:
                if self._enter_task != anyio.get_current_task().id:
                    await self._close_done.wait()
                return
            try:
                self._phase = HardwarePhase.CLOSING
                await self._state.publish()
                self._client._router.remove(self)
                if (
                    self._enter_scope is not None
                    and self._enter_task != anyio.get_current_task().id
                ):
                    self._enter_scope.cancel()
                    await self._enter_done.wait()
                self._tasks.cancel()
                await self._agreement.aclose()
                await self._tasks.aclose()
                send, receive = self._send, self._receive
                if send is not None:
                    await send.aclose()
                if receive is not None:
                    await receive.aclose()
                self._phase = HardwarePhase.CLOSED
                await self._state.aclose()
            finally:
                self._client._forget(self)
                self._close_done.set()
