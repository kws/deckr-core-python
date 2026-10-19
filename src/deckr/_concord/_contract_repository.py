"""ContractRepository."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import Any

from deckr._concord._keys import canonical_json_hash, concord_contract_key
from deckr._concord._models import (
    ConcordConflict,
    ConcordConflictCode,
    ConcordUnavailable,
    ContractHandle,
    ContractRecord,
    ContractState,
    contract_handle,
    require_text,
)
from deckr._concord._mutation_validation import (
    _assert_contract_handle,
    _assert_contract_replacement_identity,
    _assert_entry_key,
    _now_utc,
    _parse_contract_entry,
    _recover_exact_entry_after_unavailable,
    read_contract_entry,
)
from deckr._concord._ports import ExactConcordKvPort
from deckr.core.identity import ContractPointer, EndpointAddress, parse_endpoint_address
from deckr.core.wire import WireModel


class ContractRepository:
    def __init__(self, contract_exact: ExactConcordKvPort) -> None:
        self.contract_exact = contract_exact

    async def create_contract(
        self,
        participants: tuple[str | EndpointAddress, ...] | list[str | EndpointAddress],
        *,
        contract_id: str | None = None,
        generation: int = 1,
        profile: str | None = None,
        terms: Mapping[str, Any] | WireModel | None = None,
        created_by: str | EndpointAddress | None = None,
        supersedes: ContractPointer | Mapping[str, Any] | None = None,
        participant_sessions: Mapping[str | EndpointAddress, str] | None = None,
    ) -> ContractHandle:
        parsed_participants = tuple(
            sorted((parse_endpoint_address(item) for item in participants), key=str)
        )
        dumped_terms = (
            terms.model_dump(by_alias=True, exclude_none=True, mode="json")
            if isinstance(terms, WireModel)
            else terms
        )
        terms_hash = (
            canonical_json_hash(dumped_terms) if dumped_terms is not None else None
        )
        record = ContractRecord(
            contractId=contract_id or str(uuid.uuid4()),
            generation=generation,
            participants=parsed_participants,
            attachedParticipants=(),
            state=ContractState.OPEN,
            profile=profile,
            termsHash=terms_hash,
            terms=dumped_terms,
            createdBy=parse_endpoint_address(created_by)
            if created_by is not None
            else None,
            participantSessions={
                str(parse_endpoint_address(participant)): require_text(
                    session_id,
                    field_name="Concord participant session id",
                )
                for participant, session_id in (participant_sessions or {}).items()
            },
            createdAt=_now_utc(),
            supersedes=supersedes,
        )
        key = concord_contract_key(
            contract_id=record.contract_id,
            generation=record.generation,
        )
        try:
            entry = await self.contract_exact.create(key, record)
        except ConcordUnavailable as exc:
            recovered = await _recover_exact_entry_after_unavailable(
                self.contract_exact,
                key,
                exc,
            )
            if recovered is None:
                raise
            recovered_record = _parse_contract_entry(
                recovered,
                key,
                ContractPointer(
                    contractId=record.contract_id,
                    generation=record.generation,
                ),
            )
            if recovered_record != record:
                raise ConcordConflict(
                    ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
                    "Concord contract create outcome conflicts with the prepared record",
                    key=key,
                    expected_pointer=ContractPointer(
                        contractId=record.contract_id,
                        generation=record.generation,
                    ),
                ) from exc
            entry = recovered
        _assert_entry_key(entry, key, token=False)
        return contract_handle(key, record, entry.revision)

    async def contract_record(self, contract: ContractHandle) -> ContractRecord | None:
        pointer = _assert_contract_handle(contract)
        entry = await self.contract_exact.get_exact(contract.key)
        if entry is None:
            return None
        return _parse_contract_entry(entry, contract.key, pointer)

    async def cancel_pointer(
        self,
        pointer: ContractPointer,
        participant: str | EndpointAddress,
        *,
        reason: str | None = None,
    ) -> bool:
        """Best-effort fenced cancellation when create acknowledgement was lost."""

        key = concord_contract_key(
            contract_id=pointer.contract_id,
            generation=pointer.generation,
        )
        entry = await self.contract_exact.get_exact(key)
        if entry is None:
            return False
        record = _parse_contract_entry(entry, key, pointer)
        return await self.cancel(
            contract_handle(key, record, entry.revision),
            participant,
            reason=reason,
        )

    async def mark_participant_attached(
        self,
        contract: ContractHandle,
        participant: EndpointAddress,
    ) -> int:
        """Perform one fenced attached-participant CAS attempt."""

        pointer = _assert_contract_handle(contract)
        contract_key = concord_contract_key(
            contract_id=pointer.contract_id,
            generation=pointer.generation,
        )
        current, record = await read_contract_entry(
            self.contract_exact, pointer, contract_key
        )
        if record.state == ContractState.CANCELLED:
            raise ConcordConflict(
                ConcordConflictCode.CONTRACT_CANCELLED,
                f"Concord contract {contract_key!r} is cancelled",
                key=contract_key,
                expected_pointer=pointer,
            )
        if participant not in record.participants:
            raise ConcordConflict(
                ConcordConflictCode.PARTICIPANT_NOT_NAMED,
                "participant is not named by the Concord contract",
                key=contract_key,
                expected_pointer=pointer,
            )
        if participant in record.attached_participants:
            return current.revision
        attached = tuple(sorted((*record.attached_participants, participant), key=str))
        updated = record.model_copy(update={"attached_participants": attached})
        _assert_contract_replacement_identity(record, updated, pointer, contract_key)
        try:
            updated_entry = await self.contract_exact.update(
                contract_key,
                updated,
                revision=current.revision,
            )
        except ConcordUnavailable as exc:
            recovered = await _recover_exact_entry_after_unavailable(
                self.contract_exact,
                contract_key,
                exc,
            )
            if recovered is None:
                raise
            recovered_record = _parse_contract_entry(
                recovered,
                contract_key,
                pointer,
            )
            if recovered_record != updated:
                raise
            return recovered.revision
        except ConcordConflict as exc:
            if exc.code != ConcordConflictCode.REVISION_CHANGED:
                raise
            message = (
                "Concord contract attached-participant revision changed during "
                f"the fenced CAS attempt: {exc}"
            )
            raise ConcordConflict(
                ConcordConflictCode.REVISION_CHANGED,
                message,
                key=contract_key,
                expected_pointer=pointer,
            ) from exc
        return updated_entry.revision

    async def cancel(
        self,
        contract: ContractHandle,
        participant: str | EndpointAddress,
        *,
        reason: str | None = None,
    ) -> bool:
        pointer = _assert_contract_handle(contract)
        current = await self.contract_exact.get_exact(contract.key)
        if current is None:
            return False
        record = _parse_contract_entry(current, contract.key, pointer)
        if record.state == ContractState.CANCELLED:
            return False
        parsed_participant = parse_endpoint_address(participant)
        if parsed_participant not in record.participants:
            raise ConcordConflict(
                ConcordConflictCode.PARTICIPANT_NOT_NAMED,
                "participant is not named by the Concord contract",
                key=contract.key,
                expected_pointer=pointer,
            )
        cancelled = record.model_copy(
            update={
                "state": ContractState.CANCELLED,
                "cancelled_by": parsed_participant,
                "cancelled_at": _now_utc(),
                "cancel_revision": current.revision,
                "cancel_reason": reason,
            }
        )
        _assert_contract_replacement_identity(record, cancelled, pointer, contract.key)
        try:
            await self.contract_exact.update(
                contract.key,
                cancelled,
                revision=current.revision,
            )
        except ConcordUnavailable as exc:
            recovered = await _recover_exact_entry_after_unavailable(
                self.contract_exact,
                contract.key,
                exc,
            )
            if recovered is None:
                raise
            recovered_record = _parse_contract_entry(
                recovered,
                contract.key,
                pointer,
            )
            if recovered_record != cancelled:
                raise
        return True
