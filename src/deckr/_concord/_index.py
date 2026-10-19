"""Canonical observation indexes; the Concord view owns synchronization."""

from __future__ import annotations

from deckr._concord._keys import (
    parse_concord_contract_key,
    parse_concord_participant_token_key,
)
from deckr._concord._models import (
    ContractHandle,
    ContractRecord,
    ContractState,
    ParticipantHandle,
    ParticipantTokenRecord,
    contract_handle,
    participant_handle,
)
from deckr.core.identity import ContractPointer, EndpointAddress
from deckr.core.kv import (
    KvEntry,
)


class ConcordIndex:
    def __init__(self) -> None:
        self._contract_entries_by_key: dict[str, KvEntry] = {}
        self._invalid_contracts_by_key: dict[str, tuple[KvEntry, str]] = {}
        self._contract_records_by_key: dict[str, ContractRecord] = {}
        self._contract_handles_by_key: dict[str, ContractHandle] = {}
        self._contract_keys_by_pointer: dict[tuple[str, int], str] = {}
        self._contract_keys_by_contract_id: dict[str, set[str]] = {}
        self._contract_keys_by_profile: dict[str | None, set[str]] = {}
        self._contract_keys_by_participant: dict[str, set[str]] = {}
        self._contract_keys_by_state: dict[ContractState, set[str]] = {}
        self._token_entries_by_key: dict[str, KvEntry] = {}
        self._invalid_tokens_by_key: dict[str, tuple[KvEntry, str]] = {}
        self._token_records_by_key: dict[str, ParticipantTokenRecord] = {}
        self._tokens_by_key: dict[str, ParticipantHandle] = {}
        self._token_keys_by_contract: dict[tuple[str, int], set[str]] = {}
        self._token_key_by_contract_participant: dict[tuple[str, int, str], str] = {}

    @staticmethod
    def _parse_contract_entry(
        entry: KvEntry | None,
    ) -> tuple[KvEntry, ContractRecord | None, str] | None:
        if entry is None:
            return None
        parsed = parse_concord_contract_key(entry.key)
        if parsed is None:
            return entry, None, "contract key is not a Concord contract key"
        try:
            record = ContractRecord.from_wire(entry.value)
        except (TypeError, ValueError) as exc:
            return entry, None, str(exc)
        if (record.contract_id, record.generation) != parsed:
            return entry, None, "contract key and record identity differ"
        return entry, record, ""

    @staticmethod
    def _parse_token_entry(
        entry: KvEntry | None,
    ) -> tuple[KvEntry, ParticipantTokenRecord | None, str] | None:
        if entry is None:
            return None
        parsed = parse_concord_participant_token_key(entry.key)
        if parsed is None:
            return entry, None, "token key is not a Concord participant token key"
        try:
            record = ParticipantTokenRecord.from_wire(entry.value)
        except (TypeError, ValueError) as exc:
            return entry, None, str(exc)
        if (
            record.contract_id != parsed[0]
            or record.generation != parsed[1]
            or record.participant != parsed[2]
        ):
            return entry, None, "token key and record identity differ"
        return entry, record, ""

    def _index_contract_observation_locked(
        self,
        observation: tuple[KvEntry, ContractRecord | None, str],
    ) -> None:
        entry, record, diagnostic = observation
        self._contract_entries_by_key[entry.key] = entry
        if record is None:
            self._invalid_contracts_by_key[entry.key] = (entry, diagnostic)
            return
        handle = contract_handle(entry.key, record, entry.revision)
        self._contract_records_by_key[entry.key] = record
        self._contract_handles_by_key[entry.key] = handle
        self._contract_keys_by_pointer[(record.contract_id, record.generation)] = (
            entry.key
        )
        self._contract_keys_by_contract_id.setdefault(record.contract_id, set()).add(
            entry.key
        )
        self._contract_keys_by_profile.setdefault(record.profile, set()).add(entry.key)
        self._contract_keys_by_state.setdefault(record.state, set()).add(entry.key)
        for participant in record.participants:
            self._contract_keys_by_participant.setdefault(str(participant), set()).add(
                entry.key
            )

    def _index_token_observation_locked(
        self,
        observation: tuple[KvEntry, ParticipantTokenRecord | None, str],
    ) -> None:
        entry, record, diagnostic = observation
        self._token_entries_by_key[entry.key] = entry
        if record is None:
            self._invalid_tokens_by_key[entry.key] = (entry, diagnostic)
            return
        token = participant_handle(entry.key, record, entry.revision)
        self._token_records_by_key[entry.key] = record
        self._tokens_by_key[entry.key] = token
        pointer = (record.contract_id, record.generation)
        self._token_keys_by_contract.setdefault(pointer, set()).add(entry.key)
        self._token_key_by_contract_participant[
            (record.contract_id, record.generation, str(record.participant))
        ] = entry.key

    def _remove_contract_key_locked(self, key: str) -> None:
        record = self._contract_records_by_key.pop(key, None)
        self._contract_entries_by_key.pop(key, None)
        self._invalid_contracts_by_key.pop(key, None)
        self._contract_handles_by_key.pop(key, None)
        if record is None:
            return
        self._contract_keys_by_pointer.pop(
            (record.contract_id, record.generation), None
        )
        _discard_index_key(self._contract_keys_by_contract_id, record.contract_id, key)
        _discard_index_key(self._contract_keys_by_profile, record.profile, key)
        _discard_index_key(self._contract_keys_by_state, record.state, key)
        for participant in record.participants:
            _discard_index_key(
                self._contract_keys_by_participant, str(participant), key
            )

    def _remove_token_key_locked(self, key: str) -> None:
        record = self._token_records_by_key.pop(key, None)
        self._token_entries_by_key.pop(key, None)
        self._invalid_tokens_by_key.pop(key, None)
        self._tokens_by_key.pop(key, None)
        if record is None:
            return
        _discard_index_key(
            self._token_keys_by_contract,
            (record.contract_id, record.generation),
            key,
        )
        self._token_key_by_contract_participant.pop(
            (record.contract_id, record.generation, str(record.participant)),
            None,
        )

    def _clear_contract_indexes_locked(self) -> None:
        self._contract_entries_by_key.clear()
        self._invalid_contracts_by_key.clear()
        self._contract_records_by_key.clear()
        self._contract_handles_by_key.clear()
        self._contract_keys_by_pointer.clear()
        self._contract_keys_by_contract_id.clear()
        self._contract_keys_by_profile.clear()
        self._contract_keys_by_participant.clear()
        self._contract_keys_by_state.clear()

    def _clear_token_indexes_locked(self) -> None:
        self._token_entries_by_key.clear()
        self._invalid_tokens_by_key.clear()
        self._token_records_by_key.clear()
        self._tokens_by_key.clear()
        self._token_keys_by_contract.clear()
        self._token_key_by_contract_participant.clear()

    def _contract_pointer_for_key_locked(self, key: str) -> ContractPointer | None:
        handle = self._contract_handles_by_key.get(key)
        return handle.pointer if handle is not None else _pointer_from_contract_key(key)

    def _token_pointer_for_key_locked(self, key: str) -> ContractPointer | None:
        token = self._tokens_by_key.get(key)
        if token is not None:
            return ContractPointer(
                contractId=token.contract_id, generation=token.generation
            )
        return _pointer_from_token_key(key)

    def get_contract(self, pointer: ContractPointer) -> ContractHandle | None:
        key = self._contract_keys_by_pointer.get(
            (pointer.contract_id, pointer.generation)
        )
        return self._contract_handles_by_key.get(key) if key is not None else None

    def record(self, contract: ContractHandle) -> ContractRecord | None:
        record = self._contract_records_by_key.get(contract.key)
        if record is None:
            return None
        if (
            record.contract_id != contract.contract_id
            or record.generation != contract.generation
        ):
            return None
        return record

    def contracts(
        self,
        profile: str | None = None,
        *,
        contract_id: str | None = None,
        participant: EndpointAddress | None = None,
        state: ContractState | None = None,
    ) -> tuple[ContractHandle, ...]:
        keys = self._selected_keys_locked(
            profile=profile,
            contract_id=contract_id,
            participant=participant,
            state=state,
        )
        return tuple(
            self._contract_handles_by_key[key]
            for key in sorted(keys)
            if key in self._contract_handles_by_key
        )

    def _selected_keys_locked(
        self,
        *,
        profile: str | None,
        contract_id: str | None,
        participant: EndpointAddress | None,
        state: ContractState | None,
    ) -> set[str]:
        candidates: list[set[str]] = []
        if profile is not None:
            candidates.append(set(self._contract_keys_by_profile.get(profile, ())))
        if contract_id is not None:
            candidates.append(
                set(self._contract_keys_by_contract_id.get(contract_id, ()))
            )
        if participant is not None:
            candidates.append(
                set(self._contract_keys_by_participant.get(str(participant), ()))
            )
        if state is not None:
            candidates.append(set(self._contract_keys_by_state.get(state, ())))
        if not candidates:
            return set(self._contract_handles_by_key)
        keys = min(candidates, key=len).copy()
        for candidate in candidates:
            keys.intersection_update(candidate)
        return keys


def _pointer_from_contract_key(key: str) -> ContractPointer | None:
    parsed = parse_concord_contract_key(key)
    if parsed is None:
        return None
    return ContractPointer(contractId=parsed[0], generation=parsed[1])


def _pointer_from_token_key(key: str) -> ContractPointer | None:
    parsed = parse_concord_participant_token_key(key)
    if parsed is None:
        return None
    return ContractPointer(contractId=parsed[0], generation=parsed[1])


def _discard_index_key(index: dict[object, set[str]], value: object, key: str) -> None:
    keys = index.get(value)
    if keys is None:
        return
    keys.discard(key)
    if not keys:
        index.pop(value, None)
