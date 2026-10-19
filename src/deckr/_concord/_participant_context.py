"""Concord participant admission, scheduling, and observable lifecycle state."""

from __future__ import annotations

from collections.abc import AsyncIterator, Collection
from contextlib import asynccontextmanager
from dataclasses import replace

import anyio

from deckr._concord._agreement import ConcordEndpointSession
from deckr._concord._lease import CONCORD_LIFECYCLE_CLEANUP_SECONDS
from deckr._concord._models import (
    ConcordConflict,
    ConcordLifecyclePhase,
    ConcordUnavailable,
    ContractHandle,
    ContractState,
)
from deckr._concord._participant_attachments import ParticipantAttachments
from deckr._concord._participant_models import (
    ConcordManagedContract,
    ConcordParticipantCandidate,
    ConcordParticipantPolicy,
    ConcordParticipantSelection,
    ConcordParticipantState,
    ConcordParticipantStatePredicate,
    logger,
)
from deckr._concord._participant_resources import ParticipantResources
from deckr._concord._repository import ConcordRepository
from deckr._concord._tasks import ConcordTaskGroup
from deckr._concord._validation import ConcordSessionAssertions
from deckr._concord._view import ConcordView, ConcordViewSnapshot
from deckr.core.identity import ContractPointer


class ConcordParticipant:
    """Watch-driven owner of one endpoint's selected Concord tokens."""

    def __init__(
        self,
        store: ConcordRepository,
        view: ConcordView,
        endpoint: ConcordEndpointSession,
        policy: ConcordParticipantPolicy,
        task_group: anyio.abc.TaskGroup | None,
    ) -> None:
        self._resources = ParticipantResources()
        self._store = store
        self._view = view
        self._endpoint = endpoint
        self.policy = policy
        self.participant = endpoint.address
        self.session_id = endpoint.session_id
        self.profile = policy.profile
        self._pointer_running: set[ContractPointer] = set()
        self._pointer_dirty: set[ContractPointer] = set()
        self._policy_failure: str | None = None
        self._state = ConcordParticipantState(
            version=0,
            phase=ConcordLifecyclePhase.STARTING,
            current=False,
            contracts=(),
        )
        self._tasks = ConcordTaskGroup(task_group)
        self._selector_pending_lock = anyio.Lock()
        self._selector_pending: ConcordViewSnapshot | None = None
        self._selector_waiters: list[anyio.Event] = []
        self._selector_wakeup = anyio.Event()
        self._entered = False
        self._close_started = False
        self._close_lock = anyio.Lock()
        self._close_done = anyio.Event()
        self._enter_cancel_scope: anyio.CancelScope | None = None
        self._enter_task_id: int | None = None
        self._enter_done = anyio.Event()
        self._attachments = ParticipantAttachments(
            store,
            view,
            resources=self._resources,
            tasks=self._tasks,
            participant=self.participant,
            session_id=self.session_id,
            publish=self._publish,
            aggregate_phase=self._aggregate_phase,
        )

    @property
    def state(self) -> ConcordParticipantState:
        current = self._state.current and self._view.is_current()
        return replace(self._state, current=current, contracts=self.managed_contracts)

    @property
    def managed_contracts(self) -> tuple[ConcordManagedContract, ...]:
        return tuple(
            self._attachments._current_managed(item) for item in self._state.contracts
        )

    def managed_contract(
        self,
        contract: ContractHandle | ContractPointer,
    ) -> ConcordManagedContract | None:
        pointer = contract.pointer if isinstance(contract, ContractHandle) else contract
        return next(
            (
                managed
                for managed in self.managed_contracts
                if managed.contract.pointer == pointer
            ),
            None,
        )

    async def __aenter__(self) -> ConcordParticipant:
        if self._entered:
            raise RuntimeError("Concord participant contexts cannot be re-entered")
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

    async def _enter(self) -> ConcordParticipant:
        await self._view.wait_current()
        if self._resources.closed:
            raise anyio.ClosedResourceError
        self._resources.task_group = await self._tasks.start()
        await self._resources.task_group.start(self._watch_loop)
        if self._resources.closed:
            raise anyio.ClosedResourceError
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def cancel(
        self,
        contract: ContractHandle | ContractPointer,
        *,
        reason: str | None = None,
    ) -> bool:
        if self._resources.closed:
            raise anyio.ClosedResourceError
        pointer = contract.pointer if isinstance(contract, ContractHandle) else contract
        managed = self.managed_contract(pointer)
        if managed is None:
            raise ValueError("Concord participant does not manage that contract")
        try:
            return await self._store.cancel(
                managed.contract,
                self.participant,
                reason=reason,
            )
        except (ConcordConflict, ConcordUnavailable) as exc:
            async with self._resources.state_bus.lock:
                lease = self._resources.leases.get(managed.contract.key)
                if lease is not None:
                    lease.relinquish_local_authority()
                self._resources.managed.pop(managed.contract.key, None)
            await self._attachments._retain_cleanup(
                managed.contract,
                error=exc,
                reason=reason or "concord_participant_cancel",
                lease=lease,
            )
            await self._publish(
                (pointer,),
                phase=ConcordLifecyclePhase.UNAVAILABLE,
                current=False,
                reason=str(exc),
            )
            raise

    async def notify_policy_changed(self) -> None:
        if self._resources.closed:
            raise anyio.ClosedResourceError
        await self._queue_selector_snapshot(
            await self._view.snapshot(
                self.profile,
                participant=self.participant,
                state=ContractState.OPEN,
            ),
            wait=True,
        )

    @asynccontextmanager
    async def watch_state(
        self,
    ) -> AsyncIterator[AsyncIterator[ConcordParticipantState]]:
        async with self._resources.state_bus.subscribe(
            lambda _version, _current: self.state
        ) as subscription:

            async def stream() -> AsyncIterator[ConcordParticipantState]:
                yield subscription.initial
                async for _change in subscription:
                    try:
                        yield await self._resources.state_bus.capture(
                            lambda _version, _current: self.state
                        )
                    except anyio.ClosedResourceError:
                        return

            yield stream()

    async def wait_for_state(
        self,
        predicate: ConcordParticipantStatePredicate,
        *,
        timeout_seconds: float | None = None,
    ) -> ConcordParticipantState:
        async def wait() -> ConcordParticipantState:
            async with self._resources.state_bus.subscribe(
                lambda _version, _current: self.state
            ) as subscription:
                state = subscription.initial
                if predicate(state):
                    return state
                async for _change in subscription:
                    state = await self._resources.state_bus.capture(
                        lambda _version, _current: self.state
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
                    self._resources.closed = True
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
                async with self._resources.state_bus.lock:
                    for lease in {
                        *self._resources.leases.values(),
                        *self._resources.cleanup_leases.values(),
                    }:
                        lease.relinquish_local_authority()
                    self._resources.managed.clear()
                    self._resources.selected = frozenset()
                await self._publish(
                    (),
                    phase=ConcordLifecyclePhase.CLOSING,
                    current=False,
                )
                with anyio.move_on_after(CONCORD_LIFECYCLE_CLEANUP_SECONDS):
                    if wait_for_enter:
                        await self._enter_done.wait()
                    async with self._resources.state_bus.lock:
                        leases = tuple(
                            {
                                *self._resources.leases.values(),
                                *self._resources.cleanup_leases.values(),
                            }
                        )
                        cleanup_by_pointer = {
                            lease.contract.pointer: lease.contract for lease in leases
                        }
                        cleanup_by_pointer.update(self._resources.cleanup_contracts)
                        cleanup_contracts = tuple(cleanup_by_pointer.values())
                    self._resources.task_group = None
                    async with anyio.create_task_group() as cleanup_group:
                        cleanup_group.start_soon(self._tasks.aclose)
                        for lease in leases:
                            cleanup_group.start_soon(lease.aclose)
                        for contract in cleanup_contracts:
                            cleanup_group.start_soon(self._close_contract, contract)
                async with self._resources.state_bus.lock:
                    self._resources.leases.clear()
                    self._resources.cleanup_leases.clear()
                    self._resources.managed.clear()
                    self._resources.selected = frozenset()
                    self._resources.failures.clear()
                    self._resources.cleanup_contracts.clear()
                    self._resources.cleanup_wakeups.clear()
                await self._publish(
                    (),
                    phase=ConcordLifecyclePhase.CLOSED,
                    current=False,
                )
                await self._resources.state_bus.aclose()
            finally:
                self._close_done.set()

    async def _close_contract(self, contract: ContractHandle) -> None:
        try:
            await self._store.cancel(
                contract,
                self.participant,
                reason="concord_participant_context_closed",
            )
        except (ConcordConflict, ConcordUnavailable):
            logger.debug(
                "Could not cancel retained Concord contract during close",
                exc_info=True,
            )

    async def _watch_loop(
        self,
        *,
        task_status: anyio.abc.TaskStatus[None] = anyio.TASK_STATUS_IGNORED,
    ) -> None:
        async with self._view.subscribe(
            self.profile,
            participant=self.participant,
            state=ContractState.OPEN,
        ) as subscription:
            await self._apply_snapshot(subscription.initial, raise_policy=True)
            self._tasks.group.start_soon(self._selector_worker)
            task_status.started()
            async for change in subscription:
                if self._resources.closed:
                    return
                snapshot = await self._view.snapshot(
                    self.profile,
                    participant=self.participant,
                    state=ContractState.OPEN,
                )
                await self._schedule_tracked_pointer_changes(change, snapshot.current)
                await self._queue_selector_snapshot(snapshot)

    async def _schedule_tracked_pointer_changes(
        self,
        change: object,
        current: bool,
    ) -> None:
        async with self._resources.state_bus.lock:
            tracked = {
                *self._resources.selected,
                *(
                    managed.contract.pointer
                    for managed in self._resources.managed.values()
                ),
                *(lease.contract.pointer for lease in self._resources.leases.values()),
                *self._resources.cleanup_contracts,
            }
        changed = getattr(change, "changed", frozenset())
        if getattr(change, "resnapshot_required", False) or not current:
            relevant = tracked
        else:
            relevant = tracked.intersection(changed)
        async with self._resources.state_bus.lock:
            wakeups = tuple(
                self._resources.cleanup_wakeups[pointer]
                for pointer in relevant
                if pointer in self._resources.cleanup_wakeups
            )
        for wakeup in wakeups:
            wakeup.set()
        for pointer in sorted(
            relevant,
            key=lambda item: (item.contract_id, item.generation),
        ):
            await self._schedule_pointer(pointer, current)

    async def _queue_selector_snapshot(
        self,
        snapshot: ConcordViewSnapshot,
        *,
        wait: bool = False,
    ) -> None:
        waiter = anyio.Event() if wait else None
        async with self._selector_pending_lock:
            if self._resources.closed:
                if wait:
                    raise anyio.ClosedResourceError
                return
            self._selector_pending = snapshot
            if waiter is not None:
                self._selector_waiters.append(waiter)
            self._selector_wakeup.set()
        if waiter is not None:
            await waiter.wait()

    async def _selector_worker(self) -> None:
        while not self._resources.closed:
            await self._selector_wakeup.wait()
            async with self._selector_pending_lock:
                snapshot = self._selector_pending
                waiters = tuple(self._selector_waiters)
                self._selector_pending = None
                self._selector_waiters.clear()
                self._selector_wakeup = anyio.Event()
            if snapshot is None:
                for waiter in waiters:
                    waiter.set()
                continue
            try:
                await self._apply_snapshot(snapshot)
            finally:
                for waiter in waiters:
                    waiter.set()

    async def _apply_snapshot(
        self,
        snapshot: ConcordViewSnapshot,
        *,
        raise_policy: bool = False,
    ) -> None:
        if self._resources.closed:
            return
        async with self._resources.state_bus.lock:
            managed_keys = frozenset(self._resources.managed)
        candidates = tuple(
            ConcordParticipantCandidate(
                contract=item.contract,
                record=item.record,
                validity=self._view.validate(
                    item.contract,
                    current_sessions=self._session_assertions(),
                ),
                managed=item.contract.key in managed_keys,
            )
            for item in snapshot.contracts
        )
        try:
            selected = self.policy.select(candidates)
            if hasattr(selected, "__await__"):
                selected = await selected
            if not isinstance(selected, ConcordParticipantSelection):
                raise TypeError(
                    "Concord participant selector must return "
                    "ConcordParticipantSelection"
                )
            candidate_pointers = {
                candidate.contract.pointer for candidate in candidates
            }
            unknown = selected.accepted - candidate_pointers
            if unknown:
                raise ValueError(
                    "Concord participant selector returned unknown pointers: "
                    f"{sorted((item.contract_id, item.generation) for item in unknown)}"
                )
        except BaseException as exc:
            if isinstance(exc, anyio.get_cancelled_exc_class()):
                raise
            await self._fail_policy(str(exc))
            if raise_policy:
                raise
            return

        async with self._resources.state_bus.lock:
            previous = self._resources.selected
            self._resources.selected = selected.accepted
            self._policy_failure = None
            managed_pointers = {
                managed.contract.pointer for managed in self._resources.managed.values()
            }
        pointers = candidate_pointers | previous | managed_pointers
        if raise_policy:
            async with anyio.create_task_group() as task_group:
                for pointer in sorted(
                    pointers,
                    key=lambda item: (item.contract_id, item.generation),
                ):
                    task_group.start_soon(
                        self._reconcile_and_publish,
                        pointer,
                        snapshot.current,
                    )
        else:
            for pointer in sorted(
                pointers,
                key=lambda item: (item.contract_id, item.generation),
            ):
                await self._schedule_pointer(pointer, snapshot.current)
        phase, reason = await self._aggregate_phase(snapshot.current)
        await self._publish(
            pointers,
            phase=phase,
            current=snapshot.current,
            reason=reason,
        )

    async def _schedule_pointer(
        self,
        pointer: ContractPointer,
        current: bool,
    ) -> None:
        async with self._resources.state_bus.lock:
            if pointer in self._pointer_running:
                self._pointer_dirty.add(pointer)
                return
            self._pointer_running.add(pointer)
            task_group = self._resources.task_group
        if task_group is not None:
            task_group.start_soon(self._pointer_worker, pointer, current)

    async def _pointer_worker(
        self,
        pointer: ContractPointer,
        current: bool,
    ) -> None:
        try:
            while not self._resources.closed:
                await self._reconcile_and_publish(pointer, current)
                async with self._resources.state_bus.lock:
                    if pointer not in self._pointer_dirty:
                        return
                    self._pointer_dirty.discard(pointer)
                current = self._view.is_current()
        finally:
            async with self._resources.state_bus.lock:
                self._pointer_running.discard(pointer)
                self._pointer_dirty.discard(pointer)
                self._resources.pointer_locks.pop(pointer, None)

    async def _reconcile_and_publish(
        self,
        pointer: ContractPointer,
        current: bool,
    ) -> None:
        await self._attachments._reconcile_pointer(pointer)
        phase, reason = await self._aggregate_phase(current)
        await self._publish(
            (pointer,),
            phase=phase,
            current=current and self._view.is_current(),
            reason=reason,
        )

    async def _fail_policy(self, reason: str) -> None:
        async with self._resources.state_bus.lock:
            leases = tuple(self._resources.leases.values())
            pointers = {
                *self._resources.selected,
                *(
                    managed.contract.pointer
                    for managed in self._resources.managed.values()
                ),
                *(lease.contract.pointer for lease in leases),
            }
            self._policy_failure = reason
            self._resources.managed.clear()
            self._resources.selected = frozenset()
            for lease in leases:
                lease.relinquish_local_authority()
        await self._publish(
            pointers,
            phase=ConcordLifecyclePhase.UNAVAILABLE,
            current=False,
            reason=reason,
        )
        # Policy execution happens in the one coalescing selector worker.  It
        # must never perform remote cleanup itself: one blocked contract store
        # would then prevent every other pointer from failing closed.  Emptying
        # the selected set makes the existing pointer-scoped workers cancel and
        # withdraw independently while retaining ambiguous cleanup ownership.
        for pointer in sorted(
            pointers,
            key=lambda item: (item.contract_id, item.generation),
        ):
            await self._schedule_pointer(pointer, self._view.is_current())

    def _session_assertions(self) -> ConcordSessionAssertions:
        return ConcordSessionAssertions.from_mapping(
            {str(self.participant): self.session_id}
        )

    async def _aggregate_phase(
        self,
        current: bool,
    ) -> tuple[ConcordLifecyclePhase, str | None]:
        if not current:
            return ConcordLifecyclePhase.UNAVAILABLE, "Concord view is unavailable"
        async with self._resources.state_bus.lock:
            policy_failure = self._policy_failure
            failures = tuple(self._resources.failures.values())
        if policy_failure is not None:
            return ConcordLifecyclePhase.UNAVAILABLE, policy_failure
        if failures:
            phase, reason = failures[0]
            return phase, reason
        return ConcordLifecyclePhase.READY, None

    async def _publish(
        self,
        changed: Collection[ContractPointer],
        *,
        phase: ConcordLifecyclePhase,
        current: bool,
        reason: str | None = None,
    ) -> None:
        async with self._resources.state_bus.lock:
            if self._state.phase == ConcordLifecyclePhase.CLOSED:
                return
            if self._resources.closed and phase not in {
                ConcordLifecyclePhase.CLOSING,
                ConcordLifecyclePhase.CLOSED,
            }:
                return
            version = self._resources.state_bus.version + 1
            contracts = tuple(
                self._resources.managed[key] for key in sorted(self._resources.managed)
            )
            state = ConcordParticipantState(
                version=version,
                phase=phase,
                current=current,
                contracts=contracts,
                reason=reason,
            )
            self._state = state
            self._resources.state_bus.publish_locked(changed, current=current)
