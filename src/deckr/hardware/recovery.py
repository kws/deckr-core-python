"""Opt-in successor negotiation above single-agreement hardware contexts."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from typing import TYPE_CHECKING

import anyio

from deckr.contracts.messages import DeckrMessage
from deckr.core.util.owned_tasks import OwnedTaskGroup
from deckr.hardware._state import HardwareStateChannel
from deckr.hardware.claim_policy import descriptor_requires_new_claim
from deckr.hardware.claim_spec import HardwareClaimSpec
from deckr.hardware.client_runtime import MAX_ENDPOINT_CLAIMS
from deckr.hardware.discovery import HardwareCandidate
from deckr.hardware.managed_claim import ManagedHardwareClaim
from deckr.hardware.message_bodies import CapabilityStateReplyMessage
from deckr.hardware.operations import HardwareEvent
from deckr.hardware.state import (
    HardwareClaimState,
    HardwareFailure,
    HardwarePhase,
    HardwareUnavailable,
)

if TYPE_CHECKING:
    from deckr.hardware.client import DeckrHardware

HardwareRecoveryPolicy = Callable[
    [HardwareClaimSpec, tuple[HardwareCandidate, ...]], HardwareClaimSpec | None
]


def same_hardware(
    previous: HardwareClaimSpec, candidates: tuple[HardwareCandidate, ...]
) -> HardwareClaimSpec | None:
    """Recover only the same manager/devices and materially unchanged descriptors."""
    selected = []
    for old in previous.candidates:
        matches = [
            new
            for new in candidates
            if new.manager_endpoint == old.manager_endpoint
            and new.device_ref.device_id == old.device_ref.device_id
            and not descriptor_requires_new_claim(old.descriptor, new.descriptor)
        ]
        # Competing live sessions are ambiguous; selection remains caller policy.
        if len(matches) != 1:
            return None
        selected.append(matches[0])
    if len({candidate.manager_session_id for candidate in selected}) != 1:
        return None
    return replace(previous, candidates=tuple(selected))


class ManagedHardwareUse:
    def __init__(
        self,
        client: DeckrHardware,
        spec: HardwareClaimSpec,
        *,
        recover: HardwareRecoveryPolicy | None,
    ) -> None:
        self._client = client
        self.spec = spec
        self._recover = recover
        self._claim = None
        self._phase = HardwarePhase.STARTING
        self._failure = None
        self._closed = False
        self._entered = False
        self._tasks = OwnedTaskGroup(
            client._resources.task_group,
            not_started_message="Hardware client must be running",
        )
        self._state = HardwareStateChannel(self._snapshot)
        self._close_lock = anyio.Lock()
        self._close_started = False
        self._close_done = anyio.Event()
        self._enter_scope: anyio.CancelScope | None = None
        self._enter_task: int | None = None
        self._enter_done = anyio.Event()

    def _snapshot(self) -> HardwareClaimState:
        claim_state = self._claim.state if self._claim is not None else None
        if (
            claim_state is not None
            and self._phase == HardwarePhase.READY
            and not self._closed
        ):
            return claim_state
        return HardwareClaimState(
            0,
            self._phase,
            None,
            False,
            {c.device_ref.device_id: c.descriptor for c in self.spec.candidates},
            self._failure,
        )

    @property
    def state(self) -> HardwareClaimState:
        return self._state.state

    @property
    def claim(self) -> ManagedHardwareClaim:
        if self._closed or self._phase != HardwarePhase.READY or self._claim is None:
            raise HardwareUnavailable(
                "hardware_reconnecting", "Hardware use has no ready claim"
            )
        self._claim.require_authority()
        return self._claim

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

    async def command(
        self,
        device_id: str,
        capability_id: str,
        command_type: str,
        *,
        control_id: str | None = None,
        params: Mapping[str, object] | None = None,
    ) -> DeckrMessage:
        claim = self.claim
        return await claim.command(
            device_id, capability_id, command_type, control_id=control_id, params=params
        )

    async def request_state(
        self,
        device_id: str,
        capability_id: str,
        *,
        control_id: str | None = None,
        state_type: str | None = None,
        params: Mapping[str, object] | None = None,
        timeout_seconds: float = 2.0,
    ) -> CapabilityStateReplyMessage:
        claim = self.claim
        return await claim.request_state(
            device_id,
            capability_id,
            control_id=control_id,
            state_type=state_type,
            params=params,
            timeout_seconds=timeout_seconds,
        )

    def events(self) -> AbstractAsyncContextManager[AsyncIterator[HardwareEvent]]:
        # Streams belong to one agreement and end on revocation. Callers reopen
        # after observing the successor, establishing their own routing state.
        return self.claim.events()

    async def __aenter__(self) -> ManagedHardwareUse:
        if self._entered or self._closed:
            raise RuntimeError("Hardware use contexts cannot be re-entered")
        self._entered = True
        self._client._require_open()
        if len(self._client._uses) >= MAX_ENDPOINT_CLAIMS:
            raise HardwareUnavailable(
                "use_limit", "Client supports at most 64 hardware uses"
            )
        self._client._uses.add(self)
        self._enter_task = anyio.get_current_task().id
        try:
            with anyio.CancelScope() as scope:
                self._enter_scope = scope
                group = await self._tasks.start()
                await group.start(self._run)
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

    async def _run(self, *, task_status=anyio.TASK_STATUS_IGNORED):
        started = False
        spec = self.spec
        delay = 0.1
        try:
            while not self._closed:
                try:
                    async with self._client.claim(spec) as claim:
                        self._claim = claim
                        self.spec = spec
                        self._phase = HardwarePhase.READY
                        self._failure = None
                        await self._state.publish()
                        if not started:
                            started = True
                            task_status.started()
                        delay = 0.1
                        async with claim.watch_state() as states:
                            async for state in states:
                                await self._state.publish()
                                if not state.local_authority:
                                    self._failure = state.failure
                                    break
                        self._claim = None
                        self._phase = (
                            HardwarePhase.RECONNECTING
                            if self._recover
                            else HardwarePhase.UNAVAILABLE
                        )
                        await self._state.publish()
                except (HardwareUnavailable, TimeoutError) as exc:
                    if not started:
                        raise
                    self._failure = HardwareFailure(
                        getattr(exc, "code", "claim_timeout"), str(exc)
                    )
                    await self._state.publish()
                if self._recover is None:
                    return
                if (
                    self._failure is not None
                    and self._failure.code == "hardware_subscription_closed"
                ):
                    # A terminated endpoint route needs a new client lifetime;
                    # negotiating more agreements cannot restore that route.
                    self._phase = HardwarePhase.UNAVAILABLE
                    await self._state.publish()
                    return
                try:
                    spec = await self._next_spec()
                except Exception as exc:
                    self._phase = HardwarePhase.UNAVAILABLE
                    self._failure = HardwareFailure(
                        getattr(exc, "code", "recovery_policy_failed"), str(exc)
                    )
                    await self._state.publish()
                    return
                await anyio.sleep(delay)
                delay = min(delay * 2, 5.0)
        except anyio.ClosedResourceError:
            if not started:
                raise
            self._phase = HardwarePhase.UNAVAILABLE
            self._failure = HardwareFailure("hardware_closed", "Hardware client closed")
            await self._state.publish()
        finally:
            self._claim = None

    async def _next_spec(self):
        async with self._client.directory.watch_state() as states:
            async for state in states:
                if state.current:
                    spec = self._recover(self.spec, state.candidates)
                    if spec is not None:
                        return spec
        raise HardwareUnavailable(
            "discovery_closed", "Hardware discovery closed during recovery"
        )

    async def aclose(self) -> None:
        with anyio.CancelScope(shield=True):
            async with self._close_lock:
                first = not self._close_started
                if first:
                    self._close_started = True
                    self._closed = True
            if not first:
                if self._enter_task != anyio.get_current_task().id:
                    await self._close_done.wait()
                return
            try:
                self._phase = HardwarePhase.CLOSING
                await self._state.publish()
                if (
                    self._enter_scope is not None
                    and self._enter_task != anyio.get_current_task().id
                ):
                    self._enter_scope.cancel()
                    await self._enter_done.wait()
                await self._tasks.aclose()
                self._phase = HardwarePhase.CLOSED
                await self._state.aclose()
            finally:
                self._client._uses.discard(self)
                self._close_done.set()
