"""Maintenance models."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import Field, field_serializer, field_validator

from deckr._concord._models import (
    ConcordConflict,
    ConcordUnavailable,
    ContractValidityStatus,
    require_text,
)
from deckr.core.wire import WireModel

CONCORD_STALE_OBSERVATION_SCHEMA_ID = "dev.deckr.concord.stale-observation.v1"


CONCORD_TOKEN_CLEANUP_SCHEMA_ID = "dev.deckr.concord.token-cleanup.v1"


DEFAULT_CONCORD_REAPER_STALE_GRACE_SECONDS = 900


DEFAULT_CONCORD_REAPER_CANCELLED_RETENTION_SECONDS = 3600


DEFAULT_CONCORD_REAPER_SCAN_INTERVAL_SECONDS = 60


CONCORD_MAINTENANCE_ACTOR = "concord:maintenance"


CONCORD_REAPER_STALE_CONTRACT_REASON = "concord_reaper_stale_contract"


_MAX_OBSERVATION_CAS_ATTEMPTS = 8


_MAX_TOKEN_CLEANUP_SCAN_ATTEMPTS = 8


logger = logging.getLogger("deckr.concord")


def _now_utc() -> datetime:
    return datetime.now(UTC)


class ConcordStaleObservationRecord(WireModel):
    schema_id: Literal[CONCORD_STALE_OBSERVATION_SCHEMA_ID] = Field(
        default=CONCORD_STALE_OBSERVATION_SCHEMA_ID,
        alias="schema",
    )
    contract_id: str = Field(alias="contractId")
    generation: int
    first_observed_stale_at: datetime = Field(alias="firstObservedStaleAt")
    status: ContractValidityStatus
    reason: str | None = None
    contract_revision: int | None = Field(default=None, alias="contractRevision")

    @field_validator("contract_id")
    @classmethod
    def _validate_contract_id(cls, value: str) -> str:
        return require_text(value, field_name="contract id")

    @field_validator("generation")
    @classmethod
    def _validate_generation(cls, value: int) -> int:
        if value < 1:
            raise ValueError("generation must be greater than zero")
        return value

    @field_validator("first_observed_stale_at")
    @classmethod
    def _validate_first_observed_stale_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("firstObservedStaleAt must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("reason")
    @classmethod
    def _validate_reason(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return require_text(value, field_name="Concord stale observation reason")

    @field_validator("contract_revision")
    @classmethod
    def _validate_contract_revision(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("contractRevision must be non-negative")
        return value

    @field_serializer("first_observed_stale_at")
    def _serialize_first_observed_stale_at(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


class ConcordTokenCleanupRecord(WireModel):
    """Durable ownership of post-contract participant-token cleanup."""

    schema_id: Literal[CONCORD_TOKEN_CLEANUP_SCHEMA_ID] = Field(
        default=CONCORD_TOKEN_CLEANUP_SCHEMA_ID,
        alias="schema",
    )
    contract_id: str = Field(alias="contractId")
    generation: int
    contract_revision: int = Field(alias="contractRevision")
    terms_hash: str | None = Field(default=None, alias="termsHash")
    cleanup_started_at: datetime = Field(alias="cleanupStartedAt")

    @field_validator("contract_id")
    @classmethod
    def _validate_contract_id(cls, value: str) -> str:
        return require_text(value, field_name="contract id")

    @field_validator("generation")
    @classmethod
    def _validate_generation(cls, value: int) -> int:
        if value < 1:
            raise ValueError("generation must be greater than zero")
        return value

    @field_validator("contract_revision")
    @classmethod
    def _validate_contract_revision(cls, value: int) -> int:
        if value < 0:
            raise ValueError("contractRevision must be non-negative")
        return value

    @field_validator("terms_hash")
    @classmethod
    def _validate_terms_hash(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return require_text(value, field_name="Concord cleanup terms hash")

    @field_validator("cleanup_started_at")
    @classmethod
    def _validate_cleanup_started_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("cleanupStartedAt must be timezone-aware")
        return value.astimezone(UTC)

    @field_serializer("cleanup_started_at")
    def _serialize_cleanup_started_at(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


class ConcordReaperConfig(WireModel):
    stale_grace_seconds: float = Field(
        default=float(DEFAULT_CONCORD_REAPER_STALE_GRACE_SECONDS),
        ge=0,
        alias="staleGraceSeconds",
    )
    cancelled_retention_seconds: float = Field(
        default=float(DEFAULT_CONCORD_REAPER_CANCELLED_RETENTION_SECONDS),
        ge=0,
        alias="cancelledRetentionSeconds",
    )
    scan_interval_seconds: float = Field(
        default=float(DEFAULT_CONCORD_REAPER_SCAN_INTERVAL_SECONDS),
        gt=0,
        alias="scanIntervalSeconds",
    )
    log_label: str = Field(default="ConcordReaper", alias="logLabel")

    @field_validator("log_label")
    @classmethod
    def _validate_log_label(cls, value: str) -> str:
        return require_text(value, field_name="Concord reaper log label")


@dataclass(frozen=True, slots=True)
class ConcordMaintenanceDeletionResult:
    """Outcome of one retained-deletion attempt.

    ``contract_deleted`` reports the physical contract CAS performed by this
    call. Only ``cleanup_completed`` means the generation has no listed token
    keys and its durable cleanup marker was cleared. ``cleanup_pending`` means
    a later maintenance scan owns the remaining work.
    """

    contract_deleted: bool = False
    cleanup_completed: bool = False
    cleanup_pending: bool = False
    deleted_token_key_count: int = 0


@dataclass(frozen=True, slots=True)
class ConcordReaperScanResult:
    scanned_contract_count: int = 0
    stale_observation_count: int = 0
    stale_observations_created: int = 0
    stale_observations_cleared: int = 0
    contracts_cancelled: int = 0
    contracts_deleted: int = 0
    token_keys_deleted: int = 0
    token_cleanups_completed: int = 0
    token_cleanups_pending: int = 0


@dataclass(frozen=True, slots=True)
class _ConcordTokenCleanupAttempt:
    completed: bool
    deleted_token_key_count: int = 0
    failure: ConcordConflict | ConcordUnavailable | None = None


STALE_OPEN_CONTRACT_STATUSES = frozenset(
    {
        ContractValidityStatus.MISSING_TOKEN,
        ContractValidityStatus.INVALID_TOKEN,
        ContractValidityStatus.SESSION_MISMATCH,
        ContractValidityStatus.TERMS_HASH_MISMATCH,
        ContractValidityStatus.GENERATION_MISMATCH,
        ContractValidityStatus.INVALID_CONTRACT,
    }
)


def _empty_reaper_counts() -> dict[str, int]:
    return {
        "stale_observations_created": 0,
        "stale_observations_cleared": 0,
        "contracts_cancelled": 0,
        "contracts_deleted": 0,
        "token_keys_deleted": 0,
        "token_cleanups_completed": 0,
    }
