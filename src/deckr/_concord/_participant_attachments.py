"""Per-pointer token ownership, fail-closed reconciliation and retained cleanup."""

from __future__ import annotations

import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import replace

import anyio

from deckr._concord._keys import concord_contract_key
from deckr._concord._lease import ConcordParticipantLease, ConcordParticipantLeasePhase
from deckr._concord._models import (
    ConcordConflict,
    ConcordConflictCode,
    ConcordLifecyclePhase,
    ConcordUnavailable,
    ContractHandle,
    ContractState,
    ContractValidity,
    ContractValidityStatus,
    participant_handle_matches,
)
from deckr._concord._participant_models import (
    CONCORD_MANAGED_LOST_PARTICIPANT_TOKEN_REASON,
    ConcordManagedContract,
    _pointer_from_key,
    _terminal_status,
)
from deckr._concord._participant_resources import ParticipantResources
from deckr._concord._repository import ConcordRepository
from deckr._concord._tasks import ConcordTaskGroup
from deckr._concord._validation import ConcordSessionAssertions
from deckr._concord._view import ConcordView
from deckr.core.identity import ContractPointer, EndpointAddress

logger = logging.getLogger(__name__)


class ParticipantAttachments:
    def __init__(
        self,
        store: ConcordRepository,
        view: ConcordView,
        *,
        resources: ParticipantResources,
        tasks: ConcordTaskGroup,
        participant: EndpointAddress,
        session_id: str,
        publish: Callable[..., Awaitable[None]],
        aggregate_phase: Callable[
            [bool], Awaitable[tuple[ConcordLifecyclePhase, str | None]]
        ],
    ) -> None:
        self._store = store
        self._view = view
        self._resources = resources
        self._tasks = tasks
        self.participant = participant
        self.session_id = session_id
        self._publish = publish
        self._aggregate_phase = aggregate_phase

    def _session_assertions(self) -> ConcordSessionAssertions:
        return ConcordSessionAssertions.from_mapping(
            {str(self.participant): self.session_id}
        )

    def _current_managed(
        self, managed: ConcordManagedContract
    ) -> ConcordManagedContract:
        """Check authority at use time, independently of selector/publication work."""
        validity = self._view.validate(
            managed.contract, current_sessions=self._session_assertions()
        )
        lease = self._resources.leases.get(managed.contract.key)
        held = lease.token if lease is not None else None
        observed = validity.tokens.get(str(self.participant))
        local_authority = (
            not self._resources.closed
            and held is not None
            and observed is not None
            and participant_handle_matches(held, observed)
            and validity.status
            in {
                ContractValidityStatus.VALID,
                ContractValidityStatus.NOT_YET_FULFILLED,
            }
        )
        if not local_authority and lease is not None:
            lease.relinquish_local_authority()
        return replace(managed, validity=validity, local_authority=local_authority)

    async def _reconcile_pointer(self, pointer: ContractPointer) -> None:
        lock = self._resources.pointer_locks.setdefault(pointer, anyio.Lock())
        async with lock:
            if self._resources.closed:
                return
            key = concord_contract_key(
                contract_id=pointer.contract_id,
                generation=pointer.generation,
            )
            async with self._resources.state_bus.lock:
                selected = pointer in self._resources.selected
            contract = self._view.get_contract(pointer)
            if contract is None:
                await self._drop_pointer(key)
                return
            record = self._view.record(contract)
            if record is None:
                await self._drop_pointer(key)
                return
            if (
                key not in self._resources.leases
                and pointer not in self._resources.retired_pointers
            ):
                shared = self._view.validate(contract)
                expected = record.participant_sessions.get(str(self.participant))
                if shared.status == ContractValidityStatus.UNAVAILABLE or (
                    expected is not None
                    and expected != self.session_id
                    and shared.status
                    in {
                        ContractValidityStatus.VALID,
                        ContractValidityStatus.NOT_YET_FULFILLED,
                    }
                ):
                    # A proposal addressed to another session, or an unavailable
                    # source, is not evidence of lost ownership in this context.
                    await self._drop_pointer(key)
                    return
                if (
                    shared.status
                    in {
                        ContractValidityStatus.VALID,
                        ContractValidityStatus.NOT_YET_FULFILLED,
                    }
                    and str(self.participant) in shared.tokens
                ):
                    # A stored token owned by another context is participation,
                    # not evidence that this context has lost its lease state.
                    await self._drop_pointer(key)
                    return
            if not selected:
                lease = await self._relinquish_pointer(key)
                await self._cancel_or_retain(
                    contract,
                    reason="concord_participant_policy_rejected",
                    lease=lease,
                )
                await self._drop_pointer(key)
                return
            expected_session = record.participant_sessions.get(str(self.participant))
            if expected_session is not None and expected_session != self.session_id:
                self._resources.retired_pointers.add(pointer)
                lease = await self._relinquish_pointer(key)
                await self._cancel_or_retain(
                    contract,
                    reason="concord_managed_session_mismatch",
                    lease=lease,
                )
                await self._drop_pointer(key)
                return
            validity = self._view.validate(
                contract,
                current_sessions=self._session_assertions(),
            )
            if validity.status == ContractValidityStatus.UNAVAILABLE:
                async with self._resources.state_bus.lock:
                    lease = self._resources.leases.get(key)
                    self._resources.managed.pop(key, None)
                    self._resources.failures[pointer] = (
                        ConcordLifecyclePhase.UNAVAILABLE,
                        validity.reason or "Concord view is unavailable",
                    )
                if lease is not None:
                    await lease.fail_closed_unavailable(
                        validity.reason or "Concord view is unavailable"
                    )
                await self._publish(
                    (pointer,),
                    phase=ConcordLifecyclePhase.UNAVAILABLE,
                    current=False,
                    reason=validity.reason or "Concord view is unavailable",
                )
                await self._cancel_or_retain(
                    contract,
                    reason="concord_managed_view_unavailable",
                    lease=lease,
                )
                return
            existing = validity.tokens.get(str(self.participant))
            if _terminal_status(validity.status):
                await self._relinquish_pointer(key)
                await self._cancel_terminal(contract, validity)
                await self._drop_pointer(key)
                return

            async with self._resources.state_bus.lock:
                lease = self._resources.leases.get(key)
            if existing is not None:
                self._resources.retired_pointers.add(pointer)
                if (
                    lease is None
                    or lease.token is None
                    or not participant_handle_matches(lease.token, existing)
                ):
                    lease = await self._relinquish_pointer(key)
                    await self._cancel_or_retain(
                        contract,
                        reason=CONCORD_MANAGED_LOST_PARTICIPANT_TOKEN_REASON,
                        lease=lease,
                    )
                    await self._drop_pointer(key)
                    return
            if lease is None:
                if pointer in self._resources.retired_pointers:
                    await self._cancel_or_retain(
                        contract,
                        reason=CONCORD_MANAGED_LOST_PARTICIPANT_TOKEN_REASON,
                    )
                    return
                lease = ConcordParticipantLease(
                    self._store,
                    self._view,
                    contract=contract,
                    participant=self.participant,
                    session_id=self.session_id,
                )
                async with self._resources.state_bus.lock:
                    self._resources.leases[key] = lease
                try:
                    await lease.attach()
                except ConcordUnavailable as exc:
                    async with self._resources.state_bus.lock:
                        self._resources.failures[pointer] = (
                            ConcordLifecyclePhase.UNAVAILABLE,
                            str(exc),
                        )
                    return
                except ConcordConflict:
                    # Losing admission before owning a token cannot justify
                    # cancelling a contract owned by another session.
                    await self._drop_pointer(key)
                    return
                self._resources.retired_pointers.add(pointer)
                self._tasks.group.start_soon(self._heartbeat, key, lease)

            validity = self._view.validate(
                contract,
                current_sessions=self._session_assertions(),
            )
            record = validity.contract or record
            async with self._resources.state_bus.lock:
                self._resources.managed[key] = ConcordManagedContract(
                    contract=contract,
                    record=record,
                    validity=validity,
                    local_authority=lease.token is not None,
                )
                self._resources.failures.pop(pointer, None)

    async def _heartbeat(self, key: str, lease: ConcordParticipantLease) -> None:
        await lease.heartbeat_loop()
        async with self._resources.state_bus.lock:
            active = (
                not self._resources.closed and self._resources.leases.get(key) is lease
            )
        if not active:
            return
        if lease.state.phase in {
            ConcordParticipantLeasePhase.UNAVAILABLE,
            ConcordParticipantLeasePhase.TERMINAL,
        }:
            if lease.state.phase == ConcordParticipantLeasePhase.UNAVAILABLE:
                reason = (
                    lease.state.reason or CONCORD_MANAGED_LOST_PARTICIPANT_TOKEN_REASON
                )
                async with self._resources.state_bus.lock:
                    self._resources.managed.pop(key, None)
                    self._resources.failures[lease.contract.pointer] = (
                        ConcordLifecyclePhase.UNAVAILABLE,
                        reason,
                    )
                await self._publish(
                    (lease.contract.pointer,),
                    phase=ConcordLifecyclePhase.UNAVAILABLE,
                    current=False,
                    reason=reason,
                )
                await self._cancel_or_retain(
                    lease.contract,
                    reason=CONCORD_MANAGED_LOST_PARTICIPANT_TOKEN_REASON,
                    lease=lease,
                )
                return
            await self._relinquish_pointer(key)
            await self._cancel_or_retain(
                lease.contract,
                reason=CONCORD_MANAGED_LOST_PARTICIPANT_TOKEN_REASON,
                lease=lease,
            )
            await self._drop_pointer(key)
            await self._publish(
                (lease.contract.pointer,),
                phase=ConcordLifecyclePhase.READY,
                current=self._view.is_current(),
            )

    async def _relinquish_pointer(
        self,
        key: str,
    ) -> ConcordParticipantLease | None:
        """Remove public authority before any pointer-scoped remote I/O."""

        pointer = _pointer_from_key(key)
        async with self._resources.state_bus.lock:
            lease = self._resources.leases.get(key)
            if lease is not None:
                lease.relinquish_local_authority()
            self._resources.managed.pop(key, None)
        phase, reason = await self._aggregate_phase(self._view.is_current())
        await self._publish(
            (pointer,),
            phase=phase,
            current=self._view.is_current(),
            reason=reason,
        )
        return lease

    async def _cancel_terminal(
        self,
        contract: ContractHandle,
        validity: ContractValidity,
    ) -> None:
        if validity.status not in {
            ContractValidityStatus.INVALID_TOKEN,
            ContractValidityStatus.MISSING_TOKEN,
            ContractValidityStatus.GENERATION_MISMATCH,
            ContractValidityStatus.SESSION_MISMATCH,
            ContractValidityStatus.TERMS_HASH_MISMATCH,
        }:
            return
        await self._cancel_or_retain(
            contract,
            reason=f"concord_managed_{validity.status.value}",
        )

    async def _drop_pointer(self, key: str) -> None:
        pointer = _pointer_from_key(key)
        async with self._resources.state_bus.lock:
            self._resources.managed.pop(key, None)
            lease = self._resources.leases.pop(key, None)
            if pointer not in self._resources.cleanup_contracts:
                self._resources.failures.pop(pointer, None)
                current = self._view.get_contract(pointer)
                if current is not None and current.state != ContractState.OPEN:
                    self._resources.retired_pointers.discard(pointer)
        if lease is not None and pointer not in self._resources.cleanup_leases:
            await lease.aclose()

    async def _cancel_or_retain(
        self,
        contract: ContractHandle,
        *,
        reason: str,
        lease: ConcordParticipantLease | None = None,
    ) -> bool:
        try:
            await self._store.cancel(
                contract,
                self.participant,
                reason=reason,
            )
        except (ConcordConflict, ConcordUnavailable) as exc:
            await self._retain_cleanup(
                contract,
                error=exc,
                reason=reason,
                lease=lease,
            )
            return False
        return True

    async def _retain_cleanup(
        self,
        contract: ContractHandle,
        *,
        error: ConcordConflict | ConcordUnavailable,
        reason: str,
        lease: ConcordParticipantLease | None,
    ) -> None:
        pointer = contract.pointer
        async with self._resources.state_bus.lock:
            self._resources.cleanup_contracts[pointer] = contract
            self._resources.cleanup_wakeups.setdefault(pointer, anyio.Event())
            if lease is not None:
                self._resources.cleanup_leases[pointer] = lease
            self._resources.failures[pointer] = (
                ConcordLifecyclePhase.UNAVAILABLE,
                str(error),
            )
            start_retry = pointer not in self._resources.cleanup_running
            if start_retry:
                self._resources.cleanup_running.add(pointer)
            task_group = self._resources.task_group
        if lease is not None:
            lease.relinquish_local_authority()
        if start_retry and task_group is not None:
            task_group.start_soon(self._cleanup_retry_loop, pointer, reason)

    async def _cleanup_retry_loop(
        self,
        pointer: ContractPointer,
        reason: str,
    ) -> None:
        delay = 0.1
        try:
            while not self._resources.closed:
                async with self._resources.state_bus.lock:
                    contract = self._resources.cleanup_contracts.get(pointer)
                    lease = self._resources.cleanup_leases.get(pointer)
                if contract is None:
                    return
                token_clean = lease is None
                if lease is not None:
                    try:
                        await lease.withdraw_retained()
                    except (ConcordConflict, ConcordUnavailable):
                        pass
                    else:
                        token_clean = True
                try:
                    await self._store.cancel(
                        contract,
                        self.participant,
                        reason=reason,
                    )
                except ConcordConflict as exc:
                    if exc.code != ConcordConflictCode.REVISION_CHANGED:
                        async with self._resources.state_bus.lock:
                            self._resources.failures[pointer] = (
                                ConcordLifecyclePhase.TERMINAL,
                                str(exc),
                            )
                        return
                except ConcordUnavailable as exc:
                    async with self._resources.state_bus.lock:
                        self._resources.failures[pointer] = (
                            ConcordLifecyclePhase.UNAVAILABLE,
                            str(exc),
                        )
                else:
                    if token_clean:
                        async with self._resources.state_bus.lock:
                            self._resources.cleanup_contracts.pop(pointer, None)
                            self._resources.cleanup_wakeups.pop(pointer, None)
                            self._resources.failures.pop(pointer, None)
                            self._resources.retired_pointers.discard(pointer)
                            completed_lease = self._resources.cleanup_leases.pop(
                                pointer, None
                            )
                        if completed_lease is not None:
                            await completed_lease.aclose(withdraw=False)
                        await self._publish(
                            (pointer,),
                            phase=ConcordLifecyclePhase.READY,
                            current=self._view.is_current(),
                        )
                        return
                async with self._resources.state_bus.lock:
                    wakeup = self._resources.cleanup_wakeups.setdefault(
                        pointer,
                        anyio.Event(),
                    )
                if wakeup.is_set():
                    async with self._resources.state_bus.lock:
                        if self._resources.cleanup_wakeups.get(pointer) is wakeup:
                            self._resources.cleanup_wakeups[pointer] = anyio.Event()
                    delay = 0.1
                    continue
                with anyio.move_on_after(random.uniform(delay * 0.5, delay)):
                    await wakeup.wait()
                if wakeup.is_set():
                    async with self._resources.state_bus.lock:
                        if self._resources.cleanup_wakeups.get(pointer) is wakeup:
                            self._resources.cleanup_wakeups[pointer] = anyio.Event()
                    delay = 0.1
                else:
                    delay = min(delay * 2, 5.0)
        finally:
            async with self._resources.state_bus.lock:
                self._resources.cleanup_running.discard(pointer)
