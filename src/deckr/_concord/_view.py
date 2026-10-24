from __future__ import annotations

from collections import OrderedDict
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass

import anyio

from deckr._concord._index import (
    ConcordIndex,
    _pointer_from_contract_key,
    _pointer_from_token_key,
)
from deckr._concord._keys import (
    concord_contract_key,
    concord_participant_token_key,
)
from deckr._concord._models import (
    ConcordContractState,
    ContractHandle,
    ContractRecord,
    ContractState,
    ContractValidity,
)
from deckr._concord._ports import ConcordMaterializedSourcePort
from deckr._concord._validation import (
    ConcordObservationState,
    ConcordSessionAssertions,
    ContractObservation,
    contract_observation_from_entry,
    evaluate_contract_validity,
    token_observation_from_entry,
)
from deckr.core.identity import ContractPointer, EndpointAddress
from deckr.core.kv import (
    KvEntry,
    KvMaterializedChange,
    KvMaterializedSnapshot,
    KvUnavailable,
)
from deckr.core.util.anyio import CoalescedStateBroadcaster, CoalescedStateSubscription


@dataclass(frozen=True, slots=True)
class ConcordViewSnapshot:
    version: int
    current: bool
    contracts: tuple[ConcordContractState, ...]


@dataclass(frozen=True, slots=True)
class _ValidationSnapshot:
    contract_key: str
    contract_entry: KvEntry | None
    token_entries: tuple[tuple[str, KvEntry | None], ...]
    generations: tuple[int, int]
    current: bool
    validity: ContractValidity


_VALIDATION_CACHE_CAPACITY = 128
_NO_SESSION_ASSERTIONS = ConcordSessionAssertions()


class ConcordView:
    """Indexed semantic projection over the contract and token KV sources."""

    def __init__(
        self,
        contract_source: ConcordMaterializedSourcePort,
        token_source: ConcordMaterializedSourcePort,
    ) -> None:
        self._index = ConcordIndex()
        self._validations: OrderedDict[
            tuple[ContractHandle, ConcordSessionAssertions], _ValidationSnapshot
        ] = OrderedDict()
        self.contract_source = contract_source
        self.token_source = token_source
        self._contract_observed_revision: dict[str, int] = {}
        self._token_observed_revision: dict[str, int] = {}
        self._contract_revision_condition = anyio.Condition()
        self._token_revision_condition = anyio.Condition()
        self._contract_current = False
        self._token_current = False
        self._ready = anyio.Event()
        self._broadcaster = CoalescedStateBroadcaster[ContractPointer](current=False)
        self._run_cancel_scope: anyio.CancelScope | None = None
        self._run_done = anyio.Event()
        self._closed = False

    @property
    def version(self) -> int:
        return self._broadcaster.version

    @property
    def source_generations(self) -> tuple[int, int]:
        return self.contract_source.generation, self.token_source.generation

    def is_current(self) -> bool:
        return (
            self._ready.is_set()
            and self._broadcaster.current
            and self.contract_source.is_current()
            and self.token_source.is_current()
        )

    async def wait_ready(self) -> None:
        await self._ready.wait()

    async def wait_current(self) -> None:
        while not self.is_current():
            if self._closed:
                raise KvUnavailable("Concord materialized view is closed")
            await self.contract_source.wait_current()
            await self.token_source.wait_current()
            if self.is_current():
                return
            await anyio.sleep(0)

    async def run(self) -> None:
        if self._closed:
            self._run_done.set()
            return
        try:
            with anyio.CancelScope() as cancel_scope:
                self._run_cancel_scope = cancel_scope
                if self._closed:
                    cancel_scope.cancel()
                async with anyio.create_task_group() as task_group:
                    task_group.start_soon(self._contract_loop)
                    task_group.start_soon(self._token_loop)
        finally:
            self._run_cancel_scope = None
            self._run_done.set()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        cancel_scope = self._run_cancel_scope
        if cancel_scope is not None:
            cancel_scope.cancel()
            with anyio.move_on_after(5, shield=True):
                await self._run_done.wait()
        self._validations.clear()
        await self._broadcaster.aclose()
        await self._notify_contract_revisions()
        await self._notify_token_revisions()

    async def _contract_loop(self) -> None:
        try:
            async with self.contract_source.subscribe() as changes:
                async for item in changes:
                    if isinstance(item, KvMaterializedSnapshot):
                        await self._install_contract_snapshot(item)
                    else:
                        await self._consume_contract_change(item)
        except anyio.ClosedResourceError:
            return

    async def _token_loop(self) -> None:
        try:
            async with self.token_source.subscribe() as changes:
                async for item in changes:
                    if isinstance(item, KvMaterializedSnapshot):
                        await self._install_token_snapshot(item)
                    else:
                        await self._consume_token_change(item)
        except anyio.ClosedResourceError:
            return

    async def _consume_contract_change(self, change: KvMaterializedChange) -> None:
        if change.resnapshot_required:
            await self._install_contract_snapshot(await self.contract_source.snapshot())
            return
        parsed = {
            key: self._index._parse_contract_entry(self.contract_source.get_cached(key))
            for key in change.changed_keys
        }
        async with self._broadcaster.lock:
            changed: set[ContractPointer] = set()
            for key, observation in parsed.items():
                pointer = self._index._contract_pointer_for_key_locked(key)
                self._index._remove_contract_key_locked(key)
                if observation is not None:
                    self._index._index_contract_observation_locked(observation)
                    pointer = pointer or _pointer_from_contract_key(key)
                if pointer is not None:
                    changed.add(pointer)
                revision = self.contract_source.revision_cached(key)
                if revision is not None:
                    self._contract_observed_revision[key] = revision
            self._contract_current = change.current
            self._publish_locked(changed)
        await self._notify_contract_revisions()

    async def _consume_token_change(self, change: KvMaterializedChange) -> None:
        if change.resnapshot_required:
            await self._install_token_snapshot(await self.token_source.snapshot())
            return
        parsed = {
            key: self._index._parse_token_entry(self.token_source.get_cached(key))
            for key in change.changed_keys
        }
        async with self._broadcaster.lock:
            changed: set[ContractPointer] = set()
            for key, observation in parsed.items():
                pointer = self._index._token_pointer_for_key_locked(key)
                self._index._remove_token_key_locked(key)
                if observation is not None:
                    self._index._index_token_observation_locked(observation)
                    pointer = pointer or _pointer_from_token_key(key)
                if pointer is not None:
                    changed.add(pointer)
                revision = self.token_source.revision_cached(key)
                if revision is not None:
                    self._token_observed_revision[key] = revision
            self._token_current = change.current
            self._publish_locked(changed)
        await self._notify_token_revisions()

    async def _install_contract_snapshot(
        self, snapshot: KvMaterializedSnapshot
    ) -> None:
        observations = tuple(
            observation
            for entry in snapshot.entries
            if (observation := self._index._parse_contract_entry(entry)) is not None
        )
        async with self._broadcaster.lock:
            previous_keys = set(self._index._contract_entries_by_key)
            previous = {
                pointer
                for key in previous_keys
                if (pointer := _pointer_from_contract_key(key)) is not None
            }
            self._index._clear_contract_indexes_locked()
            for observation in observations:
                self._index._index_contract_observation_locked(observation)
            self._contract_observed_revision = {
                entry.key: entry.revision for entry in snapshot.entries
            }
            for key in previous_keys - self._contract_observed_revision.keys():
                revision = self.contract_source.revision_cached(key)
                if revision is not None:
                    self._contract_observed_revision[key] = revision
            current = {
                pointer
                for key in self._index._contract_entries_by_key
                if (pointer := _pointer_from_contract_key(key)) is not None
            }
            self._contract_current = snapshot.current
            self._publish_locked(
                previous | current, resnapshot=len(previous | current) > 256
            )
        await self._notify_contract_revisions()

    async def _install_token_snapshot(self, snapshot: KvMaterializedSnapshot) -> None:
        observations = tuple(
            observation
            for entry in snapshot.entries
            if (observation := self._index._parse_token_entry(entry)) is not None
        )
        async with self._broadcaster.lock:
            previous_keys = set(self._index._token_entries_by_key)
            previous = {
                pointer
                for key in previous_keys
                if (pointer := _pointer_from_token_key(key)) is not None
            }
            self._index._clear_token_indexes_locked()
            for observation in observations:
                self._index._index_token_observation_locked(observation)
            self._token_observed_revision = {
                entry.key: entry.revision for entry in snapshot.entries
            }
            for key in previous_keys - self._token_observed_revision.keys():
                revision = self.token_source.revision_cached(key)
                if revision is not None:
                    self._token_observed_revision[key] = revision
            current = {
                pointer
                for key in self._index._token_entries_by_key
                if (pointer := _pointer_from_token_key(key)) is not None
            }
            self._token_current = snapshot.current
            self._publish_locked(
                previous | current, resnapshot=len(previous | current) > 256
            )
        await self._notify_token_revisions()

    async def wait_contract_revision(self, key: str, revision: int) -> None:
        while self._contract_observed_revision.get(key, 0) < revision:
            if self._closed:
                raise KvUnavailable(
                    "Concord materialized view closed before observing "
                    f"contract revision {revision} for {key!r}"
                )
            async with self._contract_revision_condition:
                if self._contract_observed_revision.get(key, 0) >= revision:
                    return
                await self._contract_revision_condition.wait()

    async def wait_token_revision(self, key: str, revision: int) -> None:
        while self._token_observed_revision.get(key, 0) < revision:
            if self._closed:
                raise KvUnavailable(
                    "Concord materialized view closed before observing "
                    f"token revision {revision} for {key!r}"
                )
            async with self._token_revision_condition:
                if self._token_observed_revision.get(key, 0) >= revision:
                    return
                await self._token_revision_condition.wait()

    async def _notify_contract_revisions(self) -> None:
        async with self._contract_revision_condition:
            self._contract_revision_condition.notify_all()

    async def _notify_token_revisions(self) -> None:
        async with self._token_revision_condition:
            self._token_revision_condition.notify_all()

    def _publish_locked(
        self,
        changed: set[ContractPointer],
        *,
        resnapshot: bool = False,
    ) -> None:
        current = self._contract_current and self._token_current
        current_changed = self._broadcaster.current != current
        if changed or current_changed or (current and not self._ready.is_set()):
            self._broadcaster.publish_locked(
                changed,
                current=current,
                resnapshot_required=resnapshot,
            )
        if current:
            self._ready.set()

    def get_contract(self, pointer: ContractPointer) -> ContractHandle | None:
        return self._index.get_contract(pointer)

    def record(self, contract: ContractHandle) -> ContractRecord | None:
        return self._index.record(contract)

    def contracts(
        self,
        profile: str | None = None,
        *,
        participant: EndpointAddress | None = None,
        state: ContractState | None = None,
        contract_id: str | None = None,
    ) -> tuple[ContractHandle, ...]:
        return self._index.contracts(
            profile, participant=participant, state=state, contract_id=contract_id
        )

    def validate(
        self,
        contract: ContractHandle,
        *,
        current_sessions: Mapping[str, str] | ConcordSessionAssertions | None = None,
    ) -> ContractValidity:
        assertions = (
            current_sessions
            if isinstance(current_sessions, ConcordSessionAssertions)
            else ConcordSessionAssertions.from_mapping(current_sessions)
            if current_sessions is not None
            else _NO_SESSION_ASSERTIONS
        )
        cache_key = (contract, assertions)
        cached = self._validations.get(cache_key)
        # Source health is checked on every read, even before the view's watch
        # tasks run. Entry identity fences writes/deletes before index delivery;
        # generation fences a recovered source that retained the same entries.
        current = self.is_current()
        generations = self.source_generations
        if (
            cached is not None
            and cached.current == current
            and cached.generations == generations
            and self.contract_source.get_cached(cached.contract_key)
            is cached.contract_entry
            and all(
                self.token_source.get_cached(key) is entry
                for key, entry in cached.token_entries
            )
        ):
            self._validations.move_to_end(cache_key)
            return cached.validity

        pointer = contract.pointer
        key = concord_contract_key(
            contract_id=pointer.contract_id,
            generation=pointer.generation,
        )
        entry = self.contract_source.get_cached(key)
        observation = contract_observation_from_entry(key, entry)
        participants = (
            observation.record.participants
            if observation.record is not None
            else contract.participants
        )
        token_entries = tuple(
            (participant, token_key, self.token_source.get_cached(token_key))
            for participant in sorted(participants, key=str)
            for token_key in (
                concord_participant_token_key(
                    contract_id=pointer.contract_id,
                    generation=pointer.generation,
                    participant=participant,
                ),
            )
        )
        tokens = tuple(
            token_observation_from_entry(token_key, participant, token_entry)
            for participant, token_key, token_entry in token_entries
        )
        if not current:
            observation = ContractObservation(
                observation.key,
                observation.revision,
                ConcordObservationState.UNAVAILABLE,
                record=observation.record,
                diagnostic="Concord materialized contract or token source is stale",
            )
        validity = evaluate_contract_validity(
            expected_key=contract.key,
            expected_pointer=pointer,
            contract=observation,
            tokens=tokens,
            session_assertions=assertions,
        )
        self._validations[cache_key] = _ValidationSnapshot(
            key,
            entry,
            tuple((key, entry) for _, key, entry in token_entries),
            generations,
            current,
            validity,
        )
        self._validations.move_to_end(cache_key)
        while len(self._validations) > _VALIDATION_CACHE_CAPACITY:
            self._validations.popitem(last=False)
        return validity

    def state_for(self, contract: ContractHandle) -> ConcordContractState | None:
        record = self._index.record(contract)
        if record is None:
            return None
        return ConcordContractState(
            contract=contract,
            record=record,
            validity=self.validate(contract),
        )

    def _snapshot_locked(
        self,
        version: int,
        current: bool,
        *,
        profile: str | None,
        participant: EndpointAddress | None,
        state: ContractState | None,
        contract_id: str | None,
    ) -> ConcordViewSnapshot:
        contracts = self._index.contracts(
            profile,
            contract_id=contract_id,
            participant=participant,
            state=state,
        )
        return ConcordViewSnapshot(
            version=version,
            current=current,
            contracts=tuple(
                contract_state
                for contract in contracts
                if (contract_state := self.state_for(contract)) is not None
            ),
        )

    async def snapshot(
        self,
        profile: str | None = None,
        *,
        participant: EndpointAddress | None = None,
        state: ContractState | None = None,
        contract_id: str | None = None,
    ) -> ConcordViewSnapshot:
        return await self._broadcaster.capture(
            lambda version, current: self._snapshot_locked(
                version,
                current,
                profile=profile,
                participant=participant,
                state=state,
                contract_id=contract_id,
            )
        )

    @asynccontextmanager
    async def subscribe(
        self,
        profile: str | None = None,
        *,
        participant: EndpointAddress | None = None,
        state: ContractState | None = None,
        contract_id: str | None = None,
    ) -> AsyncIterator[
        CoalescedStateSubscription[ContractPointer, ConcordViewSnapshot]
    ]:
        async with self._broadcaster.subscribe(
            lambda version, current: self._snapshot_locked(
                version,
                current,
                profile=profile,
                participant=participant,
                state=state,
                contract_id=contract_id,
            )
        ) as subscription:
            yield subscription
