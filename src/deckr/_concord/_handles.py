"""Handles."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from deckr._concord._keys import concord_contract_key, concord_participant_token_key
from deckr._concord._records import ContractRecord, ParticipantTokenRecord
from deckr._concord._types import (
    ContractState,
    ContractValidityReason,
    ContractValidityStatus,
)
from deckr.core.identity import ContractPointer, EndpointAddress


@dataclass(frozen=True, slots=True)
class ContractHandle:
    key: str
    contract_id: str
    generation: int
    participants: tuple[EndpointAddress, ...]
    attached_participants: tuple[EndpointAddress, ...]
    revision: int
    state: ContractState
    profile: str | None = None
    terms_hash: str | None = None

    @property
    def pointer(self) -> ContractPointer:
        return ContractPointer(contractId=self.contract_id, generation=self.generation)


@dataclass(frozen=True, slots=True)
class ParticipantHandle:
    key: str
    contract_id: str
    generation: int
    participant: EndpointAddress
    session_id: str
    token_id: str
    revision: int
    refresh_seq: int
    ttl_seconds: int
    terms_hash: str | None = None


@dataclass(frozen=True, slots=True)
class ContractValidity:
    status: ContractValidityStatus
    contract: ContractRecord | None = None
    tokens: Mapping[str, ParticipantHandle] = field(default_factory=dict)
    reason: str | None = None
    reason_code: ContractValidityReason | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "tokens", MappingProxyType(dict(self.tokens)))

    @property
    def valid(self) -> bool:
        return self.status == ContractValidityStatus.VALID


@dataclass(frozen=True, slots=True)
class ConcordContractState:
    """One cached contract record and its materialized validity."""

    contract: ContractHandle
    record: ContractRecord
    validity: ContractValidity


def contract_handle(key: str, record: ContractRecord, revision: int) -> ContractHandle:
    return ContractHandle(
        key=key,
        contract_id=record.contract_id,
        generation=record.generation,
        participants=record.participants,
        attached_participants=record.attached_participants,
        revision=revision,
        state=record.state,
        profile=record.profile,
        terms_hash=record.terms_hash,
    )


def participant_handle(
    key: str,
    record: ParticipantTokenRecord,
    revision: int,
) -> ParticipantHandle:
    return ParticipantHandle(
        key=key,
        contract_id=record.contract_id,
        generation=record.generation,
        participant=record.participant,
        session_id=record.session_id,
        token_id=record.token_id,
        revision=revision,
        refresh_seq=record.refresh_seq,
        ttl_seconds=record.ttl_seconds,
        terms_hash=record.terms_hash,
    )


def contract_handle_has_canonical_identity(handle: ContractHandle) -> bool:
    return handle.key == concord_contract_key(
        contract_id=handle.contract_id,
        generation=handle.generation,
    )


def participant_handle_has_canonical_identity(handle: ParticipantHandle) -> bool:
    return handle.key == concord_participant_token_key(
        contract_id=handle.contract_id,
        generation=handle.generation,
        participant=handle.participant,
    )


def token_matches_handle(
    token: ParticipantTokenRecord,
    handle: ParticipantHandle,
) -> bool:
    return (
        token.contract_id == handle.contract_id
        and token.generation == handle.generation
        and token.participant == handle.participant
        and token.session_id == handle.session_id
        and token.token_id == handle.token_id
        and token.terms_hash == handle.terms_hash
    )


def participant_handle_matches(
    current: ParticipantHandle,
    updated: ParticipantHandle,
) -> bool:
    return (
        updated.key == current.key
        and updated.contract_id == current.contract_id
        and updated.generation == current.generation
        and updated.participant == current.participant
        and updated.session_id == current.session_id
        and updated.token_id == current.token_id
        and updated.terms_hash == current.terms_hash
    )
