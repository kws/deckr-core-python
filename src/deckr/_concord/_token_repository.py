"""ParticipantTokenRepository."""

from __future__ import annotations

import uuid

from deckr._concord._keys import concord_contract_key, concord_participant_token_key
from deckr._concord._models import (
    ConcordConflict,
    ConcordConflictCode,
    ConcordUnavailable,
    ContractHandle,
    ContractState,
    ParticipantHandle,
    ParticipantTokenRecord,
    participant_handle,
)
from deckr._concord._mutation_validation import (
    _assert_contract_handle,
    _assert_entry_key,
    _assert_participant_handle,
    _assert_token_replacement_identity,
    _parse_token_entry,
    _parse_token_entry_for_handle,
    _recover_exact_entry_after_unavailable,
    _token_identity_conflict,
    _token_matches_attach_request,
    read_contract_entry,
)
from deckr._concord._ports import ExactConcordKvPort
from deckr.core.identity import EndpointAddress, parse_endpoint_address


class ParticipantTokenRepository:
    def __init__(
        self, contract_exact: ExactConcordKvPort, token_exact: ExactConcordKvPort
    ) -> None:
        self.contract_exact = contract_exact
        self.token_exact = token_exact

    async def prepare_attach(
        self,
        contract: ContractHandle,
        participant: str | EndpointAddress,
        session_id: str,
        *,
        token_id: str | None = None,
    ) -> ParticipantHandle:
        """Create or recover the caller-owned token before contract attachment.

        Returning the exact handle transfers cleanup ownership to the caller.
        The token is not Concord authority until ``mark_participant_attached``
        succeeds and cached/exact validation observes the complete pair.
        """

        pointer = _assert_contract_handle(contract)
        _current, record = await read_contract_entry(
            self.contract_exact, pointer, contract.key
        )
        if record.state == ContractState.CANCELLED:
            raise ConcordConflict(
                ConcordConflictCode.CONTRACT_CANCELLED,
                f"Concord contract {contract.key!r} is cancelled",
                key=contract.key,
                expected_pointer=pointer,
            )
        parsed_participant = parse_endpoint_address(participant)
        if parsed_participant not in record.participants:
            raise ConcordConflict(
                ConcordConflictCode.PARTICIPANT_NOT_NAMED,
                "participant is not named by the Concord contract",
                key=contract.key,
                expected_pointer=pointer,
            )
        expected_session = record.participant_sessions.get(str(parsed_participant))
        if expected_session is not None and expected_session != session_id:
            raise ConcordConflict(
                ConcordConflictCode.SESSION_MISMATCH,
                "participant session does not match the contract session fence",
                key=contract.key,
                expected_pointer=pointer,
            )
        key = concord_participant_token_key(
            contract_id=record.contract_id,
            generation=record.generation,
            participant=parsed_participant,
        )
        if parsed_participant in record.attached_participants:
            token_entry = await self.token_exact.get_exact(key)
            if token_entry is not None:
                existing_token = _parse_token_entry(
                    token_entry,
                    key=key,
                    contract_pointer=pointer,
                    participant=parsed_participant,
                )
                if _token_matches_attach_request(
                    existing_token,
                    record=record,
                    participant=parsed_participant,
                    session_id=session_id,
                    token_id=token_id,
                ):
                    return participant_handle(key, existing_token, token_entry.revision)
            raise ConcordConflict(
                ConcordConflictCode.PARTICIPANT_ALREADY_ATTACHED,
                "Concord participant is already attached",
                key=key,
                expected_pointer=pointer,
            )
        ttl = await self.token_exact.ttl_seconds()
        token = ParticipantTokenRecord(
            contractId=record.contract_id,
            generation=record.generation,
            participant=parsed_participant,
            sessionId=session_id,
            tokenId=token_id or str(uuid.uuid4()),
            refreshSeq=1,
            ttlSeconds=ttl,
            termsHash=record.terms_hash,
        )
        try:
            entry = await self.token_exact.create(key, token, ttl=token.ttl_seconds)
        except ConcordUnavailable as exc:
            recovered = await _recover_exact_entry_after_unavailable(
                self.token_exact,
                key,
                exc,
            )
            if recovered is None:
                raise
            recovered_token = _parse_token_entry(
                recovered,
                key=key,
                contract_pointer=pointer,
                participant=parsed_participant,
            )
            if recovered_token != token:
                raise ConcordConflict(
                    ConcordConflictCode.TOKEN_IDENTITY_MISMATCH,
                    "Concord token create outcome conflicts with the prepared token",
                    key=key,
                    expected_pointer=pointer,
                ) from exc
            token = recovered_token
            entry = recovered
        except ConcordConflict as exc:
            if exc.code != ConcordConflictCode.KEY_ALREADY_EXISTS:
                raise
            token_entry = await self.token_exact.get_exact(key)
            if token_entry is None:
                raise ConcordConflict(
                    ConcordConflictCode.TOKEN_ALREADY_EXISTS,
                    "Concord participant token changed during attach",
                    key=key,
                    expected_pointer=pointer,
                ) from exc
            observed = _parse_token_entry(
                token_entry,
                key=key,
                contract_pointer=pointer,
                participant=parsed_participant,
            )
            if not _token_matches_attach_request(
                observed,
                record=record,
                participant=parsed_participant,
                session_id=session_id,
                token_id=token_id,
            ):
                raise ConcordConflict(
                    ConcordConflictCode.TOKEN_ALREADY_EXISTS,
                    "Concord participant token already exists",
                    key=key,
                    expected_pointer=pointer,
                ) from exc
            token = observed
            entry = token_entry
        else:
            _assert_entry_key(entry, key, token=True, pointer=pointer)
        return participant_handle(key, token, entry.revision)

    async def refresh(self, handle: ParticipantHandle) -> ParticipantHandle:
        pointer = _assert_participant_handle(handle)
        contract_key = concord_contract_key(
            contract_id=pointer.contract_id,
            generation=pointer.generation,
        )
        _, contract = await read_contract_entry(
            self.contract_exact, pointer, contract_key
        )
        if contract.state == ContractState.CANCELLED:
            raise ConcordConflict(
                ConcordConflictCode.CONTRACT_CANCELLED,
                "Concord contract is cancelled",
                key=contract_key,
                expected_pointer=pointer,
            )
        token_entry = await self.token_exact.get_exact(handle.key)
        if token_entry is None:
            raise ConcordConflict(
                ConcordConflictCode.TOKEN_MISSING,
                "Concord participant token is missing",
                key=handle.key,
                expected_pointer=pointer,
            )
        token = _parse_token_entry_for_handle(token_entry, handle, pointer)
        if token.terms_hash != contract.terms_hash:
            raise _token_identity_conflict(handle, pointer)
        ttl = await self.token_exact.ttl_seconds()
        refreshed = token.model_copy(
            update={"refresh_seq": token.refresh_seq + 1, "ttl_seconds": ttl}
        )
        _assert_token_replacement_identity(token, refreshed, handle, pointer)
        try:
            entry = await self.token_exact.update(
                handle.key,
                refreshed,
                revision=token_entry.revision,
                ttl=refreshed.ttl_seconds,
            )
        except ConcordUnavailable as exc:
            recovered = await _recover_exact_entry_after_unavailable(
                self.token_exact,
                handle.key,
                exc,
            )
            if recovered is None:
                raise
            latest = _parse_token_entry_for_handle(recovered, handle, pointer)
            if latest != refreshed:
                raise
            return participant_handle(handle.key, latest, recovered.revision)
        except ConcordConflict as exc:
            if exc.code != ConcordConflictCode.REVISION_CHANGED:
                raise
            latest_entry = await self.token_exact.get_exact(handle.key)
            if latest_entry is None:
                raise ConcordConflict(
                    ConcordConflictCode.TOKEN_MISSING,
                    "Concord participant token is missing",
                    key=handle.key,
                    expected_pointer=pointer,
                ) from exc
            latest = _parse_token_entry_for_handle(latest_entry, handle, pointer)
            if latest.terms_hash != contract.terms_hash:
                raise _token_identity_conflict(handle, pointer) from exc
            raise ConcordConflict(
                ConcordConflictCode.REVISION_CHANGED,
                "Concord participant token changed during exact refresh",
                key=handle.key,
                expected_pointer=pointer,
            ) from exc
        _assert_entry_key(entry, handle.key, token=True, pointer=pointer)
        return participant_handle(handle.key, refreshed, entry.revision)

    async def validate_participant_handle(
        self,
        handle: ParticipantHandle,
    ) -> ParticipantHandle:
        pointer = _assert_participant_handle(handle)
        contract_key = concord_contract_key(
            contract_id=pointer.contract_id,
            generation=pointer.generation,
        )
        _, contract = await read_contract_entry(
            self.contract_exact, pointer, contract_key
        )
        if contract.state == ContractState.CANCELLED:
            raise ConcordConflict(
                ConcordConflictCode.CONTRACT_CANCELLED,
                "Concord contract is cancelled",
                key=contract_key,
                expected_pointer=pointer,
            )
        token_entry = await self.token_exact.get_exact(handle.key)
        if token_entry is None:
            raise ConcordConflict(
                ConcordConflictCode.TOKEN_MISSING,
                "Concord participant token is missing",
                key=handle.key,
                expected_pointer=pointer,
            )
        token = _parse_token_entry_for_handle(token_entry, handle, pointer)
        if token.terms_hash != contract.terms_hash:
            raise _token_identity_conflict(handle, pointer)
        return participant_handle(handle.key, token, token_entry.revision)

    async def withdraw(self, handle: ParticipantHandle) -> bool:
        pointer = _assert_participant_handle(handle)
        token_entry = await self.token_exact.get_exact(handle.key)
        if token_entry is None:
            return False
        _parse_token_entry_for_handle(token_entry, handle, pointer)
        try:
            await self.token_exact.delete(handle.key, revision=token_entry.revision)
        except ConcordUnavailable as exc:
            recovered = await _recover_exact_entry_after_unavailable(
                self.token_exact,
                handle.key,
                exc,
            )
            if recovered is not None:
                _parse_token_entry_for_handle(recovered, handle, pointer)
                raise
        return True

    async def withdraw_identity(
        self,
        contract: ContractHandle,
        participant: str | EndpointAddress,
        session_id: str,
        token_id: str,
    ) -> bool:
        """Withdraw a prepared token whose create result could not be observed."""

        pointer = _assert_contract_handle(contract)
        parsed_participant = parse_endpoint_address(participant)
        key = concord_participant_token_key(
            contract_id=pointer.contract_id,
            generation=pointer.generation,
            participant=parsed_participant,
        )
        entry = await self.token_exact.get_exact(key)
        if entry is None:
            return False
        token = _parse_token_entry(
            entry,
            key=key,
            contract_pointer=pointer,
            participant=parsed_participant,
        )
        if (
            token.session_id != session_id
            or token.token_id != token_id
            or token.terms_hash != contract.terms_hash
        ):
            raise ConcordConflict(
                ConcordConflictCode.TOKEN_IDENTITY_MISMATCH,
                "prepared Concord token changed owner before cleanup",
                key=key,
                expected_pointer=pointer,
            )
        try:
            await self.token_exact.delete(key, revision=entry.revision)
        except ConcordUnavailable as exc:
            recovered = await _recover_exact_entry_after_unavailable(
                self.token_exact,
                key,
                exc,
            )
            if recovered is not None:
                raise
        return True
