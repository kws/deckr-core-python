"""Participant models."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass
from typing import Protocol

from deckr._concord._keys import parse_concord_contract_key
from deckr._concord._models import (
    ConcordLifecyclePhase,
    ContractHandle,
    ContractRecord,
    ContractValidity,
    ContractValidityStatus,
    require_text,
)
from deckr.core.identity import ContractPointer

CONCORD_MANAGED_LOST_PARTICIPANT_TOKEN_REASON = "concord_managed_lost_participant_token"


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ConcordParticipantCandidate:
    contract: ContractHandle
    record: ContractRecord
    validity: ContractValidity
    managed: bool


@dataclass(frozen=True, slots=True)
class ConcordParticipantSelection:
    accepted: frozenset[ContractPointer]

    def __init__(self, accepted: Collection[ContractPointer]) -> None:
        object.__setattr__(self, "accepted", frozenset(accepted))


class ConcordParticipantSelector(Protocol):
    def __call__(
        self,
        candidates: tuple[ConcordParticipantCandidate, ...],
    ) -> ConcordParticipantSelection | Awaitable[ConcordParticipantSelection]: ...


@dataclass(frozen=True, slots=True)
class ConcordParticipantPolicy:
    profile: str | None
    select: ConcordParticipantSelector

    def __post_init__(self) -> None:
        if self.profile is not None:
            require_text(self.profile, field_name="Concord participant profile")
        if not callable(self.select):
            raise TypeError("Concord participant selector must be callable")


@dataclass(frozen=True, slots=True)
class ConcordManagedContract:
    contract: ContractHandle
    record: ContractRecord
    validity: ContractValidity
    local_authority: bool


@dataclass(frozen=True, slots=True)
class ConcordParticipantState:
    version: int
    phase: ConcordLifecyclePhase
    current: bool
    contracts: tuple[ConcordManagedContract, ...]
    reason: str | None = None


ConcordParticipantStatePredicate = Callable[[ConcordParticipantState], bool]


def _terminal_status(status: ContractValidityStatus) -> bool:
    return status not in {
        ContractValidityStatus.VALID,
        ContractValidityStatus.NOT_YET_FULFILLED,
        ContractValidityStatus.UNAVAILABLE,
    }


def _pointer_from_key(key: str) -> ContractPointer:
    parsed = parse_concord_contract_key(key)
    if parsed is None:
        raise ValueError(f"invalid Concord contract key: {key!r}")
    contract_id, generation = parsed
    return ContractPointer(contractId=contract_id, generation=generation)
