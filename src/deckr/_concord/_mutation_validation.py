"""Mutation validation."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

from deckr._concord._models import (
    ConcordConflict,
    ConcordConflictCode,
    ConcordUnavailable,
    ContractHandle,
    ContractRecord,
    ParticipantHandle,
    ParticipantTokenRecord,
    contract_handle_has_canonical_identity,
    participant_handle_has_canonical_identity,
    token_matches_handle,
)
from deckr._concord._ports import ExactConcordKvPort
from deckr._concord._validation import ConcordSessionAssertions
from deckr.core.identity import ContractPointer, EndpointAddress
from deckr.core.kv import KvEntry


def _now_utc() -> datetime:
    return datetime.now(UTC)


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


def _assert_participant_handle(handle: ParticipantHandle) -> ContractPointer:
    pointer = ContractPointer(
        contractId=handle.contract_id,
        generation=handle.generation,
    )
    if not participant_handle_has_canonical_identity(handle):
        raise _token_identity_conflict(handle, pointer)
    return pointer


def _parse_contract_entry(
    entry: KvEntry,
    key: str,
    pointer: ContractPointer,
) -> ContractRecord:
    _assert_entry_key(entry, key, token=False, pointer=pointer)
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
        observed = ContractPointer(
            contractId=record.contract_id,
            generation=record.generation,
        )
        raise ConcordConflict(
            ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
            f"Concord contract {key!r} changed identity",
            key=key,
            expected_pointer=pointer,
            observed_pointer=observed,
        )
    return record


def _parse_token_entry(
    entry: KvEntry,
    *,
    key: str,
    contract_pointer: ContractPointer,
    participant: EndpointAddress,
) -> ParticipantTokenRecord:
    _assert_entry_key(entry, key, token=True, pointer=contract_pointer)
    try:
        token = ParticipantTokenRecord.from_wire(entry.value)
    except (TypeError, ValueError) as exc:
        raise ConcordConflict(
            ConcordConflictCode.TOKEN_INVALID,
            f"Concord participant token {key!r} is malformed: {exc}",
            key=key,
            expected_pointer=contract_pointer,
        ) from exc
    if (
        token.contract_id != contract_pointer.contract_id
        or token.generation != contract_pointer.generation
        or token.participant != participant
    ):
        raise ConcordConflict(
            ConcordConflictCode.TOKEN_IDENTITY_MISMATCH,
            "Concord participant token changed owner",
            key=key,
            expected_pointer=contract_pointer,
        )
    return token


def _parse_token_entry_for_handle(
    entry: KvEntry,
    handle: ParticipantHandle,
    pointer: ContractPointer,
) -> ParticipantTokenRecord:
    token = _parse_token_entry(
        entry,
        key=handle.key,
        contract_pointer=pointer,
        participant=handle.participant,
    )
    if not token_matches_handle(token, handle):
        raise _token_identity_conflict(handle, pointer)
    return token


def _assert_entry_key(
    entry: KvEntry,
    key: str,
    *,
    token: bool,
    pointer: ContractPointer | None = None,
) -> None:
    if entry.key == key:
        return
    raise ConcordConflict(
        (
            ConcordConflictCode.TOKEN_IDENTITY_MISMATCH
            if token
            else ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH
        ),
        f"Concord KV entry key {entry.key!r} differs from requested key {key!r}",
        key=key,
        expected_pointer=pointer,
    )


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


def _assert_token_replacement_identity(
    current: ParticipantTokenRecord,
    replacement: ParticipantTokenRecord,
    handle: ParticipantHandle,
    pointer: ContractPointer,
) -> None:
    if token_matches_handle(current, handle) and token_matches_handle(
        replacement, handle
    ):
        return
    raise _token_identity_conflict(handle, pointer)


def _token_identity_conflict(
    handle: ParticipantHandle,
    pointer: ContractPointer,
) -> ConcordConflict:
    return ConcordConflict(
        ConcordConflictCode.TOKEN_IDENTITY_MISMATCH,
        "Concord participant token changed owner",
        key=handle.key,
        expected_pointer=pointer,
    )


def _token_matches_attach_request(
    token: ParticipantTokenRecord,
    *,
    record: ContractRecord,
    participant: EndpointAddress,
    session_id: str,
    token_id: str | None,
) -> bool:
    return (
        token.contract_id == record.contract_id
        and token.generation == record.generation
        and token.participant == participant
        and token.session_id == session_id
        and token_id is not None
        and token.token_id == token_id
        and token.terms_hash == record.terms_hash
    )


async def _recover_exact_entry_after_unavailable(
    store: ExactConcordKvPort,
    key: str,
    original: ConcordUnavailable,
) -> KvEntry | None:
    """Resolve a possibly committed mutation without allocating new identity."""

    try:
        return await store.get_exact(key)
    except ConcordUnavailable as recovery_error:
        raise original from recovery_error


def _freeze_session_assertions(
    value: Mapping[str, str] | ConcordSessionAssertions | None,
) -> ConcordSessionAssertions:
    if isinstance(value, ConcordSessionAssertions):
        return value
    return ConcordSessionAssertions.from_mapping(value)


async def read_contract_entry(
    exact: ExactConcordKvPort, pointer: ContractPointer, key: str
) -> tuple[KvEntry, ContractRecord]:
    entry = await exact.get_exact(key)
    if entry is None:
        raise ConcordConflict(
            ConcordConflictCode.CONTRACT_MISSING,
            f"Concord contract {key!r} is missing",
            key=key,
            expected_pointer=pointer,
        )
    return entry, _parse_contract_entry(entry, key, pointer)


# Bound competing contract attachment CAS attempts before relinquishing ownership.
MAX_ATTACHED_PARTICIPANT_CAS_ATTEMPTS = 8
