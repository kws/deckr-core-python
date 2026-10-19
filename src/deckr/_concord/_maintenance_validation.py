"""Maintenance validation."""

from __future__ import annotations

from deckr._concord._keys import concord_token_cleanup_key
from deckr._concord._maintenance_models import (
    STALE_OPEN_CONTRACT_STATUSES,
    ConcordStaleObservationRecord,
    ConcordTokenCleanupRecord,
)
from deckr._concord._models import (
    ConcordConflict,
    ConcordConflictCode,
    ContractHandle,
    ContractRecord,
    ContractValidity,
    ContractValidityStatus,
    ParticipantTokenRecord,
    contract_handle_has_canonical_identity,
)
from deckr.core.identity import ContractPointer
from deckr.core.kv import KvEntry

ConcordTokenMaintenanceEntry = tuple[KvEntry, ParticipantTokenRecord]


def _open_contract_validity_is_stale(validity: ContractValidity) -> bool:
    if validity.status in STALE_OPEN_CONTRACT_STATUSES:
        return True
    return (
        validity.status == ContractValidityStatus.NOT_YET_FULFILLED
        and not validity.tokens
    )


def _assert_contract_handle(contract: ContractHandle) -> ContractPointer:
    pointer = ContractPointer(
        contractId=contract.contract_id,
        generation=contract.generation,
    )
    if not contract_handle_has_canonical_identity(contract):
        raise ConcordConflict(
            ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
            f"Concord contract {contract.key!r} changed identity",
            key=contract.key,
            expected_pointer=pointer,
        )
    return pointer


def _parse_contract_entry(
    entry: KvEntry,
    key: str,
    pointer: ContractPointer,
) -> ContractRecord:
    if entry.key != key:
        raise ConcordConflict(
            ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
            f"Concord KV entry key {entry.key!r} differs from requested key {key!r}",
            key=key,
            expected_pointer=pointer,
        )
    try:
        record = ContractRecord.from_wire(entry.value)
    except (TypeError, ValueError) as exc:
        raise ConcordConflict(
            ConcordConflictCode.CONTRACT_INVALID,
            f"Concord contract {key!r} is malformed: {exc}",
            key=key,
            expected_pointer=pointer,
        ) from exc
    if (
        record.contract_id != pointer.contract_id
        or record.generation != pointer.generation
    ):
        raise ConcordConflict(
            ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
            f"Concord contract {key!r} changed identity",
            key=key,
            expected_pointer=pointer,
            observed_pointer=ContractPointer(
                contractId=record.contract_id,
                generation=record.generation,
            ),
        )
    return record


def _assert_contract_replacement_identity(
    current: ContractRecord,
    replacement: ContractRecord,
    pointer: ContractPointer,
    key: str,
) -> None:
    if (
        replacement.contract_id == current.contract_id == pointer.contract_id
        and replacement.generation == current.generation == pointer.generation
    ):
        return
    raise ConcordConflict(
        ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
        f"Concord contract {key!r} replacement changed identity",
        key=key,
        expected_pointer=pointer,
    )


def _parse_stale_observation_entry(
    entry: KvEntry,
    *,
    key: str,
    contract_id: str,
    generation: int,
) -> ConcordStaleObservationRecord:
    pointer = ContractPointer(contractId=contract_id, generation=generation)
    if entry.key != key:
        raise ConcordConflict(
            ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
            "Concord stale observation entry key differs from the requested key",
            key=key,
            expected_pointer=pointer,
        )
    try:
        record = ConcordStaleObservationRecord.from_wire(entry.value)
    except (TypeError, ValueError) as exc:
        raise ConcordConflict(
            ConcordConflictCode.CONTRACT_INVALID,
            f"Concord stale observation {key!r} is malformed: {exc}",
            key=key,
            expected_pointer=pointer,
        ) from exc
    if record.contract_id != contract_id or record.generation != generation:
        raise ConcordConflict(
            ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
            "Concord stale observation key and record identity differ",
            key=key,
            expected_pointer=pointer,
            observed_pointer=ContractPointer(
                contractId=record.contract_id,
                generation=record.generation,
            ),
        )
    return record


def _parse_token_cleanup_entry(
    entry: KvEntry,
    *,
    key: str,
    contract_id: str,
    generation: int,
) -> ConcordTokenCleanupRecord:
    pointer = ContractPointer(contractId=contract_id, generation=generation)
    canonical_key = concord_token_cleanup_key(
        contract_id=contract_id,
        generation=generation,
    )
    if entry.key != key or key != canonical_key:
        raise ConcordConflict(
            ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
            "Concord token-cleanup marker key differs from the requested key",
            key=key,
            expected_pointer=pointer,
        )
    try:
        record = ConcordTokenCleanupRecord.from_wire(entry.value)
    except (TypeError, ValueError) as exc:
        raise ConcordConflict(
            ConcordConflictCode.CONTRACT_INVALID,
            f"Concord token-cleanup marker {key!r} is malformed: {exc}",
            key=key,
            expected_pointer=pointer,
        ) from exc
    if record.contract_id != contract_id or record.generation != generation:
        raise ConcordConflict(
            ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
            "Concord token-cleanup marker key and record identity differ",
            key=key,
            expected_pointer=pointer,
            observed_pointer=ContractPointer(
                contractId=record.contract_id,
                generation=record.generation,
            ),
        )
    return record
