"""Agreement models."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Protocol

from deckr._concord._models import (
    ConcordLifecyclePhase,
    ContractHandle,
    ContractValidity,
    ContractValidityStatus,
    require_text,
)
from deckr.core.identity import ContractPointer, EndpointAddress, parse_endpoint_address
from deckr.core.wire import WireModel, freeze_json

CONCORD_AGREEMENT_LOST_PARTICIPANT_TOKEN_REASON = (
    "concord_agreement_lost_participant_token"
)


logger = logging.getLogger(__name__)


class ConcordEndpointSession(Protocol):
    @property
    def address(self) -> EndpointAddress: ...

    @property
    def session_id(self) -> str: ...


ConcordAgreementStatePredicate = Callable[["ConcordAgreementState"], bool]


@dataclass(frozen=True, slots=True)
class ConcordAgreementSpec:
    """Intent for one fresh owner-side Concord contract."""

    profile: str | None
    participants: tuple[str | EndpointAddress, ...] | list[str | EndpointAddress]
    terms: Mapping[str, Any] | WireModel | None = None
    supersedes: ContractPointer | Mapping[str, Any] | None = None
    participant_sessions: Mapping[str | EndpointAddress, str] | None = None
    created_by: str | EndpointAddress | None = None

    def __post_init__(self) -> None:
        participants = tuple(
            sorted(
                (parse_endpoint_address(item) for item in self.participants),
                key=str,
            )
        )
        if not participants:
            raise ValueError("Concord agreements require at least one participant")
        if len({str(item) for item in participants}) != len(participants):
            raise ValueError("Concord agreement participants must be unique")
        if self.profile is not None:
            require_text(self.profile, field_name="Concord agreement profile")
        supersedes = (
            self.supersedes
            if isinstance(self.supersedes, ContractPointer)
            else ContractPointer.model_validate(self.supersedes)
            if self.supersedes is not None
            else None
        )
        terms = (
            self.terms.model_dump(by_alias=True, exclude_none=True, mode="json")
            if isinstance(self.terms, WireModel)
            else self.terms
        )
        sessions = {
            str(parse_endpoint_address(key)): require_text(
                value,
                field_name="Concord participant session id",
            )
            for key, value in (self.participant_sessions or {}).items()
        }
        if not set(sessions) <= {str(item) for item in participants}:
            raise ValueError("participant_sessions keys must be named participants")
        object.__setattr__(self, "participants", participants)
        object.__setattr__(
            self,
            "terms",
            freeze_json(terms) if terms is not None else None,
        )
        object.__setattr__(self, "supersedes", supersedes)
        object.__setattr__(
            self,
            "participant_sessions",
            MappingProxyType(sessions),
        )
        object.__setattr__(
            self,
            "created_by",
            parse_endpoint_address(self.created_by)
            if self.created_by is not None
            else None,
        )


@dataclass(frozen=True, slots=True)
class ConcordAgreementState:
    version: int
    phase: ConcordLifecyclePhase
    current: bool
    contract: ContractHandle | None
    validity: ContractValidity | None
    local_authority: bool
    reason: str | None = None

    @property
    def valid(self) -> bool:
        validity_contract = (
            self.validity.contract if self.validity is not None else None
        )
        return (
            self.phase == ConcordLifecyclePhase.READY
            and self.current
            and self.local_authority
            and self.contract is not None
            and self.validity is not None
            and self.validity.valid
            and validity_contract is not None
            and validity_contract.contract_id == self.contract.contract_id
            and validity_contract.generation == self.contract.generation
        )


def _terminal_status(status: ContractValidityStatus) -> bool:
    return status not in {
        ContractValidityStatus.VALID,
        ContractValidityStatus.NOT_YET_FULFILLED,
        ContractValidityStatus.UNAVAILABLE,
    }
