"""Repository."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import anyio

from deckr._concord._adapters import (
    concord_bucket_adapters,
)
from deckr._concord._contract_repository import ContractRepository
from deckr._concord._keys import concord_contract_key, concord_participant_token_key
from deckr._concord._models import (
    ConcordConflict,
    ConcordConflictCode,
    ConcordUnavailable,
    ContractHandle,
    ContractRecord,
    ContractValidity,
    ParticipantHandle,
)
from deckr._concord._mutation_validation import (
    MAX_ATTACHED_PARTICIPANT_CAS_ATTEMPTS as MAX_ATTACHED_PARTICIPANT_CAS_ATTEMPTS,
    _freeze_session_assertions,
)
from deckr._concord._token_repository import ParticipantTokenRepository
from deckr._concord._validation import (
    ConcordObservationState,
    ConcordSessionAssertions,
    ContractObservation,
    ParticipantTokenObservation,
    contract_observation_from_entry,
    evaluate_contract_validity,
    token_observation_from_entry,
)
from deckr.core.identity import ContractPointer, EndpointAddress
from deckr.core.kv_source import KvMaterializedSource
from deckr.core.wire import WireModel


class ConcordRepository:
    def __init__(
        self,
        contract_bucket: KvMaterializedSource | Any,
        token_bucket: KvMaterializedSource | Any,
    ) -> None:
        contract = concord_bucket_adapters(contract_bucket)
        token = concord_bucket_adapters(token_bucket)
        self.contract_exact = contract.exact
        self.contract_source = contract.source
        self.token_exact = token.exact
        self.token_source = token.source

        self._tokens = ParticipantTokenRepository(self.contract_exact, self.token_exact)

        self._contracts = ContractRepository(self.contract_exact)

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
        return await self._contracts.create_contract(
            participants,
            contract_id=contract_id,
            generation=generation,
            profile=profile,
            terms=terms,
            created_by=created_by,
            supersedes=supersedes,
            participant_sessions=participant_sessions,
        )

    async def contract_record(self, contract: ContractHandle) -> ContractRecord | None:
        return await self._contracts.contract_record(contract)

    async def cancel_pointer(
        self,
        pointer: ContractPointer,
        participant: str | EndpointAddress,
        *,
        reason: str | None = None,
    ) -> bool:
        return await self._contracts.cancel_pointer(pointer, participant, reason=reason)

    async def attach(
        self,
        contract: ContractHandle,
        participant: str | EndpointAddress,
        session_id: str,
        *,
        token_id: str | None = None,
    ) -> ParticipantHandle:
        token = await self.prepare_attach(
            contract,
            participant,
            session_id,
            token_id=token_id,
        )
        try:
            for _attempt in range(MAX_ATTACHED_PARTICIPANT_CAS_ATTEMPTS):
                try:
                    await self.mark_participant_attached(
                        contract,
                        token.participant,
                    )
                except ConcordConflict as exc:
                    if exc.code != ConcordConflictCode.REVISION_CHANGED:
                        raise
                    if _attempt + 1 == MAX_ATTACHED_PARTICIPANT_CAS_ATTEMPTS:
                        raise
                    continue
                return token
        except BaseException:
            # The raw/conformance helper never abandons a token that was created
            # before the attached-participant CAS completed. Managed lifecycle
            # code uses ``prepare_attach`` directly so it can retain the same
            # fenced handle while waiting for relevant view progress.
            with anyio.CancelScope(shield=True):
                try:
                    await self.withdraw(token)
                except (ConcordConflict, ConcordUnavailable):
                    pass
            raise
        raise AssertionError("unreachable participant attach state")

    async def prepare_attach(
        self,
        contract: ContractHandle,
        participant: str | EndpointAddress,
        session_id: str,
        *,
        token_id: str | None = None,
    ) -> ParticipantHandle:
        return await self._tokens.prepare_attach(
            contract, participant, session_id, token_id=token_id
        )

    async def mark_participant_attached(
        self, contract: ContractHandle, participant: EndpointAddress
    ) -> int:
        return await self._contracts.mark_participant_attached(contract, participant)

    async def refresh(self, handle: ParticipantHandle) -> ParticipantHandle:
        return await self._tokens.refresh(handle)

    async def validate_participant_handle(
        self, handle: ParticipantHandle
    ) -> ParticipantHandle:
        return await self._tokens.validate_participant_handle(handle)

    async def withdraw(self, handle: ParticipantHandle) -> bool:
        return await self._tokens.withdraw(handle)

    async def withdraw_identity(
        self,
        contract: ContractHandle,
        participant: str | EndpointAddress,
        session_id: str,
        token_id: str,
    ) -> bool:
        return await self._tokens.withdraw_identity(
            contract, participant, session_id, token_id
        )

    async def cancel(
        self,
        contract: ContractHandle,
        participant: str | EndpointAddress,
        *,
        reason: str | None = None,
    ) -> bool:
        return await self._contracts.cancel(contract, participant, reason=reason)

    async def validate_exact(
        self,
        contract: ContractHandle,
        *,
        current_sessions: Mapping[str, str] | ConcordSessionAssertions | None = None,
    ) -> ContractValidity:
        assertions = _freeze_session_assertions(current_sessions)
        pointer = ContractPointer(
            contractId=contract.contract_id,
            generation=contract.generation,
        )
        key = concord_contract_key(
            contract_id=pointer.contract_id,
            generation=pointer.generation,
        )
        try:
            entry = await self.contract_exact.get_exact(key)
        except ConcordUnavailable as exc:
            contract_observation = ContractObservation(
                key,
                None,
                ConcordObservationState.UNAVAILABLE,
                diagnostic=str(exc),
            )
            token_observations: tuple[ParticipantTokenObservation, ...] = ()
        else:
            contract_observation = contract_observation_from_entry(key, entry)
            participants = (
                contract_observation.record.participants
                if contract_observation.record is not None
                else contract.participants
            )
            token_observations = await self._exact_token_observations(
                pointer,
                participants,
            )
        return evaluate_contract_validity(
            expected_key=contract.key,
            expected_pointer=pointer,
            contract=contract_observation,
            tokens=token_observations,
            session_assertions=assertions,
        )

    async def _exact_token_observations(
        self,
        pointer: ContractPointer,
        participants: tuple[EndpointAddress, ...],
    ) -> tuple[ParticipantTokenObservation, ...]:
        observations: list[ParticipantTokenObservation] = []
        for participant in sorted(participants, key=str):
            key = concord_participant_token_key(
                contract_id=pointer.contract_id,
                generation=pointer.generation,
                participant=participant,
            )
            try:
                entry = await self.token_exact.get_exact(key)
            except ConcordUnavailable as exc:
                observations.append(
                    ParticipantTokenObservation(
                        key,
                        None,
                        participant,
                        ConcordObservationState.UNAVAILABLE,
                        diagnostic=str(exc),
                    )
                )
            else:
                observations.append(
                    token_observation_from_entry(key, participant, entry)
                )
        return tuple(observations)
