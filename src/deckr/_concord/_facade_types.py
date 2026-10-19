"""Facade types."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

from deckr._concord._models import ConcordContractState, ContractValidityStatus
from deckr.core.identity import ContractPointer

logger = logging.getLogger(__name__)


def _contract_lifecycle_log_level(profile: str | None) -> int:
    del profile
    return logging.INFO


def _contract_terminal_log_level(profile: str | None) -> int:
    del profile
    return logging.INFO


def _contract_invalid_log_level(
    profile: str | None,
    status: ContractValidityStatus | None,
) -> int:
    del profile, status
    return logging.WARNING


def _now_utc() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class ConcordWatchSnapshot:
    version: int
    current: bool
    contracts: tuple[ConcordContractState, ...]


@dataclass(frozen=True, slots=True)
class ConcordWatchChange:
    version: int
    current: bool
    contracts: tuple[ConcordContractState, ...]
    changed_pointers: frozenset[ContractPointer]
    resnapshot_required: bool
