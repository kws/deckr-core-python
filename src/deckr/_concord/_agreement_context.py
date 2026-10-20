"""One locally proposed Concord agreement and its authority lifecycle."""

from __future__ import annotations

import random
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace

import anyio

from deckr._concord._agreement_models import (
    CONCORD_AGREEMENT_LOST_PARTICIPANT_TOKEN_REASON,
    ConcordAgreementSpec,
    ConcordAgreementState,
    ConcordAgreementStatePredicate,
    ConcordEndpointSession,
    _terminal_status,
    logger,
)
from deckr._concord._lease import (
    CONCORD_LIFECYCLE_CLEANUP_SECONDS,
    ConcordParticipantLease,
    ConcordParticipantLeasePhase,
)
from deckr._concord._models import (
    ConcordConflict,
    ConcordConflictCode,
    ConcordLifecyclePhase,
    ConcordUnavailable,
    ContractHandle,
    ContractValidity,
    ContractValidityReason,
    ContractValidityStatus,
    participant_handle_matches,
    require_text,
)
from deckr._concord._owner import ConcordOwnerBootstrap
from deckr._concord._repository import ConcordRepository
from deckr._concord._tasks import ConcordTaskGroup
from deckr._concord._validation import ConcordSessionAssertions
from deckr._concord._view import ConcordView
from deckr.core.identity import ContractPointer, parse_endpoint_address
from deckr.core.util.anyio import CoalescedStateBroadcaster


class ConcordAgreementLease:
    """Async-context-owned agreement, local token, and state watcher."""

    def __init__(
        self,
        store: ConcordRepository,
        view: ConcordView,
        endpoint: ConcordEndpointSession,
        spec: ConcordAgreementSpec,
        task_group: anyio.abc.TaskGroup | None,
        owner_bootstrap: ConcordOwnerBootstrap,
    ) -> None:
        self._store = store
        self._view = view
        self._endpoint = endpoint
        self._owner_bootstrap = owner_bootstrap
        self.spec = spec
        self._contract: ContractHandle | None = None
        self._prepared_contract_id = str(uuid.uuid4())
        self._lease: ConcordParticipantLease | None = None
        self._state_bus = CoalescedStateBroadcaster[str](current=False)
        self._state = ConcordAgreementState(
            version=0,
            phase=ConcordLifecyclePhase.STARTING,
            current=False,
            contract=None,
            validity=None,
            local_authority=False,
        )
        self._tasks = ConcordTaskGroup(task_group)
        self._task_group: anyio.abc.TaskGroup | None = None
        self._entered = False
        self._closed = False
        self._owner_claimed = False
        self._cleanup_required = False
        self._cleanup_running = False
        self._cleanup_wakeup = anyio.Event()
        self._close_started = False
        self.cleanup_failure: Exception | None = None
        self._close_lock = anyio.Lock()
        self._close_done = anyio.Event()
        self._enter_cancel_scope: anyio.CancelScope | None = None
        self._enter_task_id: int | None = None
        self._enter_done = anyio.Event()

    @property
    def contract(self) -> ContractHandle:
        if self._contract is None:
            raise RuntimeError("Concord agreement context has not been entered")
        return self._contract

    @property
    def contract_id(self) -> str:
        return self.contract.contract_id

    @property
    def generation(self) -> int:
        return self.contract.generation

    @property
    def profile(self) -> str | None:
        return self.contract.profile

    @property
    def state(self) -> ConcordAgreementState:
        state = self._state
        if not state.local_authority or state.contract is None:
            return state
        validity = self._view.validate(state.contract)
        token = self._lease.token if self._lease is not None else None
        observed = validity.tokens.get(str(self._endpoint.address))
        held = (
            token is not None
            and observed is not None
            and participant_handle_matches(token, observed)
            and validity.status
            in {ContractValidityStatus.VALID, ContractValidityStatus.NOT_YET_FULFILLED}
        )
        if not held and self._lease is not None:
            self._lease.relinquish_local_authority()
        return replace(
            state,
            validity=validity,
            current=self._view.is_current(),
            local_authority=held,
        )

    @property
    def closed(self) -> bool:
        return self._closed

    async def __aenter__(self) -> ConcordAgreementLease:
        if self._entered:
            raise RuntimeError("Concord agreement contexts cannot be re-entered")
        self._entered = True
        with anyio.CancelScope() as scope:
            self._enter_cancel_scope = scope
            self._enter_task_id = anyio.get_current_task().id
            try:
                return await self._enter()
            except BaseException:
                if not self._close_started:
                    await self.aclose()
                raise
            finally:
                self._enter_cancel_scope = None
                self._enter_done.set()
        raise anyio.ClosedResourceError

    async def _enter(self) -> ConcordAgreementLease:
        participant = parse_endpoint_address(self._endpoint.address)
        session_id = require_text(
            self._endpoint.session_id,
            field_name="Concord endpoint session id",
        )
        if participant not in self.spec.participants:
            raise ValueError("agreement endpoint must be named by participants")
        if self.spec.created_by is not None and self.spec.created_by != participant:
            raise ValueError("agreement creator must match the local endpoint")
        expected_local_session = self.spec.participant_sessions.get(str(participant))
        if expected_local_session is not None and expected_local_session != session_id:
            raise ValueError(
                "agreement participant_sessions conflicts with the endpoint session"
            )

        participant_sessions = dict(self.spec.participant_sessions)
        participant_sessions[str(participant)] = session_id

        await self._view.wait_current()
        if self._closed:
            raise anyio.ClosedResourceError
        await self._owner_bootstrap.claim(
            participant,
            session_id,
            ContractPointer(
                contractId=self._prepared_contract_id,
                generation=1,
            ),
        )
        self._owner_claimed = True
        if self._closed:
            raise anyio.ClosedResourceError
        contract = await self._store.create_contract(
            self.spec.participants,
            contract_id=self._prepared_contract_id,
            generation=1,
            profile=self.spec.profile,
            terms=self.spec.terms,
            created_by=self.spec.created_by or participant,
            supersedes=self.spec.supersedes,
            participant_sessions=participant_sessions,
        )
        self._contract = contract
        if self._closed:
            raise anyio.ClosedResourceError
        await self._view.wait_contract_revision(contract.key, contract.revision)
        self._lease = ConcordParticipantLease(
            self._store,
            self._view,
            contract=contract,
            participant=participant,
            session_id=session_id,
        )
        self._task_group = await self._tasks.start()
        await self._task_group.start(self._watch_loop)
        try:
            await self._lease.attach()
        except ConcordUnavailable as exc:
            await self._publish(
                ConcordLifecyclePhase.UNAVAILABLE,
                validity=self._cached_validity(),
                local_authority=False,
                reason=str(exc),
            )
            return self
        except ConcordConflict as exc:
            await self._publish(
                ConcordLifecyclePhase.TERMINAL,
                validity=self._cached_validity(),
                local_authority=False,
                reason=str(exc),
            )
            return self
        if self._closed:
            raise anyio.ClosedResourceError
        self._task_group.start_soon(self._heartbeat_loop)
        await self._refresh_from_view()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def cancel(self, reason: str | None = None) -> bool:
        if self._closed:
            raise ConcordConflict(
                ConcordConflictCode.AGREEMENT_CLOSED,
                "Concord agreement is closed",
                key=self.contract.key if self._contract is not None else None,
            )
        try:
            cancelled = await self._store.cancel(
                self.contract,
                self._endpoint.address,
                reason=reason,
            )
        except (ConcordConflict, ConcordUnavailable):
            lease = self._lease
            if lease is not None:
                lease.relinquish_local_authority()
            self._retain_contract_cleanup(reason or "concord_agreement_cancel")
            await self._publish(
                ConcordLifecyclePhase.UNAVAILABLE,
                validity=self._cached_validity(),
                local_authority=False,
                reason="Concord cancellation outcome is unresolved",
            )
            raise
        await self._refresh_from_view(exact=True)
        return cancelled

    @asynccontextmanager
    async def watch_state(
        self,
    ) -> AsyncIterator[AsyncIterator[ConcordAgreementState]]:
        async with self._state_bus.subscribe(
            lambda _version, _current: self._state
        ) as subscription:

            async def stream() -> AsyncIterator[ConcordAgreementState]:
                yield subscription.initial
                async for _change in subscription:
                    try:
                        yield await self._state_bus.capture(
                            lambda _version, _current: self._state
                        )
                    except anyio.ClosedResourceError:
                        return

            yield stream()

    async def wait_for_state(
        self,
        predicate: ConcordAgreementStatePredicate,
        *,
        timeout_seconds: float | None = None,
    ) -> ConcordAgreementState:
        async def wait() -> ConcordAgreementState:
            async with self._state_bus.subscribe(
                lambda _version, _current: self._state
            ) as subscription:
                state = subscription.initial
                if predicate(state):
                    return state
                async for _change in subscription:
                    state = await self._state_bus.capture(
                        lambda _version, _current: self._state
                    )
                    if predicate(state):
                        return state
            raise anyio.ClosedResourceError

        if timeout_seconds is None:
            return await wait()
        with anyio.fail_after(timeout_seconds):
            return await wait()

    async def aclose(self) -> None:
        with anyio.CancelScope(shield=True):
            async with self._close_lock:
                if self._close_started:
                    first = False
                else:
                    first = True
                    self._close_started = True
                    self._closed = True
                    enter_scope = self._enter_cancel_scope
                    if enter_scope is not None:
                        enter_scope.cancel()
                    wait_for_enter = (
                        enter_scope is not None
                        and self._enter_task_id != anyio.get_current_task().id
                    )
            if not first:
                await self._close_done.wait()
                return
            try:
                self._tasks.cancel()
                lease = self._lease
                if lease is not None:
                    lease.relinquish_local_authority()
                await self._publish(
                    ConcordLifecyclePhase.CLOSING,
                    validity=self._state.validity,
                    local_authority=False,
                )
                with anyio.move_on_after(CONCORD_LIFECYCLE_CLEANUP_SECONDS) as cleanup:
                    if wait_for_enter:
                        await self._enter_done.wait()
                    self._task_group = None
                    async with anyio.create_task_group() as cleanup_group:
                        cleanup_group.start_soon(self._tasks.aclose)
                        cleanup_group.start_soon(self._cleanup_contract_on_close)
                        if lease is not None:
                            cleanup_group.start_soon(lease.aclose)
                if cleanup.cancelled_caught:
                    self.cleanup_failure = TimeoutError(
                        "Concord agreement cleanup timed out"
                    )
                elif lease is not None and lease.cleanup_failure is not None:
                    self.cleanup_failure = lease.cleanup_failure
                await self._publish(
                    ConcordLifecyclePhase.CLOSED,
                    validity=self._state.validity,
                    local_authority=False,
                )
                await self._state_bus.aclose()
                if self._owner_claimed:
                    await self._owner_bootstrap.release(
                        self._endpoint.address,
                        ContractPointer(
                            contractId=self._prepared_contract_id,
                            generation=1,
                        ),
                    )
                    self._owner_claimed = False
            finally:
                self._close_done.set()

    async def _cleanup_contract_on_close(self) -> None:
        contract = self._contract
        try:
            if contract is not None:
                await self._store.cancel(
                    contract,
                    self._endpoint.address,
                    reason="concord_agreement_context_closed",
                )
            else:
                await self._store.cancel_pointer(
                    ContractPointer(
                        contractId=self._prepared_contract_id,
                        generation=1,
                    ),
                    self._endpoint.address,
                    reason="concord_unresolved_contract_create_closed",
                )
        except (ConcordConflict, ConcordUnavailable) as exc:
            self.cleanup_failure = exc
            logger.debug(
                "Could not resolve Concord agreement during close",
                exc_info=True,
            )

    async def _watch_loop(
        self,
        *,
        task_status: anyio.abc.TaskStatus[None] = anyio.TASK_STATUS_IGNORED,
    ) -> None:
        async with self._view.subscribe(
            contract_id=self.contract.contract_id,
        ) as subscription:
            task_status.started()
            await self._refresh_from_view()
            async for change in subscription:
                if self._closed:
                    return
                if (
                    change.resnapshot_required
                    or not change.current
                    or self.contract.pointer in change.changed
                ):
                    self._cleanup_wakeup.set()
                    await self._refresh_from_view()

    async def _heartbeat_loop(self) -> None:
        lease = self._lease
        if lease is None:
            return
        await lease.heartbeat_loop()
        if self._closed:
            return
        if lease.state.phase == ConcordParticipantLeasePhase.UNAVAILABLE:
            await self._lose_authority(
                ConcordLifecyclePhase.UNAVAILABLE,
                lease.state.reason or "participant token refresh unavailable",
            )
        elif lease.state.phase == ConcordParticipantLeasePhase.TERMINAL:
            await self._lose_authority(
                ConcordLifecyclePhase.TERMINAL,
                lease.state.reason or CONCORD_AGREEMENT_LOST_PARTICIPANT_TOKEN_REASON,
            )

    async def _lose_authority(
        self,
        phase: ConcordLifecyclePhase,
        reason: str,
    ) -> None:
        validity = self._cached_validity()
        if (
            validity.status != ContractValidityStatus.UNAVAILABLE
            and not _terminal_status(validity.status)
        ):
            validity = ContractValidity(
                ContractValidityStatus.INVALID_TOKEN,
                contract=validity.contract,
                tokens=validity.tokens,
                reason=CONCORD_AGREEMENT_LOST_PARTICIPANT_TOKEN_REASON,
                reason_code=ContractValidityReason.TOKEN_LOCAL_AUTHORITY_LOST,
            )
        await self._publish(
            phase,
            validity=validity,
            local_authority=False,
            reason=reason,
        )
        # Local authority is already gone and publicly observable before any
        # remote cleanup is attempted. A blocked broker operation must never
        # hold the public agreement in READY.
        try:
            await self._store.cancel(
                self.contract,
                self._endpoint.address,
                reason=CONCORD_AGREEMENT_LOST_PARTICIPANT_TOKEN_REASON,
            )
        except (ConcordConflict, ConcordUnavailable):
            self._retain_contract_cleanup(
                CONCORD_AGREEMENT_LOST_PARTICIPANT_TOKEN_REASON
            )

    def _retain_contract_cleanup(self, reason: str) -> None:
        self._cleanup_required = True
        if self._cleanup_running or self._closed or self._task_group is None:
            return
        self._cleanup_running = True
        self._task_group.start_soon(self._cleanup_retry_loop, reason)

    async def _cleanup_retry_loop(self, reason: str) -> None:
        delay = 0.1
        try:
            while self._cleanup_required and not self._closed:
                try:
                    await self._store.cancel(
                        self.contract,
                        self._endpoint.address,
                        reason=reason,
                    )
                except ConcordConflict as exc:
                    if exc.code != ConcordConflictCode.REVISION_CHANGED:
                        return
                    self._cleanup_wakeup.set()
                except ConcordUnavailable:
                    pass
                else:
                    self._cleanup_required = False
                    lease = self._lease
                    if lease is not None:
                        try:
                            await lease.withdraw_retained()
                        except (ConcordConflict, ConcordUnavailable):
                            pass
                    return
                wakeup = self._cleanup_wakeup
                if wakeup.is_set():
                    self._cleanup_wakeup = anyio.Event()
                    delay = 0.1
                    continue
                with anyio.move_on_after(random.uniform(delay * 0.5, delay)):
                    await wakeup.wait()
                if wakeup.is_set():
                    if self._cleanup_wakeup is wakeup:
                        self._cleanup_wakeup = anyio.Event()
                    delay = 0.1
                else:
                    delay = min(delay * 2, 5.0)
        finally:
            self._cleanup_running = False

    async def _refresh_from_view(self, *, exact: bool = False) -> None:
        if self._contract is None:
            return
        if self._state.phase in {
            ConcordLifecyclePhase.UNAVAILABLE,
            ConcordLifecyclePhase.TERMINAL,
            ConcordLifecyclePhase.CLOSING,
            ConcordLifecyclePhase.CLOSED,
        }:
            return
        sessions = self._session_assertions()
        validity = (
            await self._store.validate_exact(
                self.contract,
                current_sessions=sessions,
            )
            if exact
            else self._view.validate(
                self.contract,
                current_sessions=sessions,
            )
        )
        lease = self._lease
        local_token = validity.tokens.get(str(self._endpoint.address))
        held = lease.token if lease is not None else None
        local_authority = (
            held is not None
            and local_token is not None
            and participant_handle_matches(held, local_token)
        )
        if validity.status == ContractValidityStatus.UNAVAILABLE and held is not None:
            assert lease is not None
            reason = validity.reason or "Concord view is unavailable"
            await lease.fail_closed_unavailable(reason)
            await self._lose_authority(ConcordLifecyclePhase.UNAVAILABLE, reason)
            return
        if held is not None and not local_authority:
            await self._lose_authority(
                ConcordLifecyclePhase.TERMINAL,
                CONCORD_AGREEMENT_LOST_PARTICIPANT_TOKEN_REASON,
            )
            return
        if _terminal_status(validity.status):
            assert lease is not None
            lease.relinquish_local_authority()
            await self._lose_authority(
                ConcordLifecyclePhase.TERMINAL,
                validity.reason or f"Concord contract is {validity.status.value}",
            )
            return
        phase = (
            ConcordLifecyclePhase.UNAVAILABLE
            if validity.status == ContractValidityStatus.UNAVAILABLE
            else ConcordLifecyclePhase.READY
        )
        await self._publish(
            phase,
            validity=validity,
            local_authority=local_authority,
            reason=validity.reason,
        )

    def _cached_validity(self) -> ContractValidity:
        return self._view.validate(
            self.contract,
            current_sessions=self._session_assertions(),
        )

    def _session_assertions(self) -> ConcordSessionAssertions:
        sessions = dict(self.spec.participant_sessions or {})
        sessions[str(self._endpoint.address)] = self._endpoint.session_id
        return ConcordSessionAssertions.from_mapping(sessions)

    async def _publish(
        self,
        phase: ConcordLifecyclePhase,
        *,
        validity: ContractValidity | None,
        local_authority: bool,
        reason: str | None = None,
    ) -> None:
        async with self._state_bus.lock:
            if self._state.phase == ConcordLifecyclePhase.CLOSED:
                return
            if self._state.phase in {
                ConcordLifecyclePhase.UNAVAILABLE,
                ConcordLifecyclePhase.TERMINAL,
                ConcordLifecyclePhase.CLOSING,
            } and phase not in {
                ConcordLifecyclePhase.CLOSING,
                ConcordLifecyclePhase.CLOSED,
            }:
                return
            if self._closed and phase not in {
                ConcordLifecyclePhase.CLOSING,
                ConcordLifecyclePhase.CLOSED,
            }:
                return
            version = self._state_bus.version + 1
            state = ConcordAgreementState(
                version=version,
                phase=phase,
                current=(
                    False
                    if phase
                    in {
                        ConcordLifecyclePhase.CLOSING,
                        ConcordLifecyclePhase.CLOSED,
                    }
                    else self._view.is_current()
                ),
                contract=self._contract,
                validity=validity,
                local_authority=local_authority,
                reason=reason,
            )
            self._state = state
            self._state_bus.publish_locked(("agreement",), current=state.current)
