"""Model rules."""

from __future__ import annotations

from enum import StrEnum

from pydantic import Field, field_validator

from deckr.core.identity import ContractPointer
from deckr.core.wire import WireModel

CONCORD_CONTRACT_SCHEMA_ID = "dev.deckr.concord.contract.v1"


CONCORD_PARTICIPANT_TOKEN_SCHEMA_ID = "dev.deckr.concord.participant-token.v1"


class ConcordConflictCode(StrEnum):
    KEY_ALREADY_EXISTS = "key_already_exists"
    REVISION_CHANGED = "revision_changed"
    CONTRACT_MISSING = "contract_missing"
    CONTRACT_CANCELLED = "contract_cancelled"
    CONTRACT_INVALID = "contract_invalid"
    CONTRACT_IDENTITY_MISMATCH = "contract_identity_mismatch"
    PARTICIPANT_NOT_NAMED = "participant_not_named"
    PARTICIPANT_ALREADY_ATTACHED = "participant_already_attached"
    SESSION_MISMATCH = "session_mismatch"
    TOKEN_ALREADY_EXISTS = "token_already_exists"
    TOKEN_MISSING = "token_missing"
    TOKEN_INVALID = "token_invalid"
    TOKEN_IDENTITY_MISMATCH = "token_identity_mismatch"
    LEASE_CLOSED = "lease_closed"
    AGREEMENT_CLOSED = "agreement_closed"


class ConcordUnavailableCode(StrEnum):
    STORE_UNAVAILABLE = "store_unavailable"
    SOURCE_STALE = "source_stale"
    TTL_METADATA_MISSING = "ttl_metadata_missing"
    TTL_INVALID = "ttl_invalid"


class ConcordLifecyclePhase(StrEnum):
    """Current phase of one owned Concord lifecycle context."""

    STARTING = "starting"
    READY = "ready"
    UNAVAILABLE = "unavailable"
    TERMINAL = "terminal"
    CLOSING = "closing"
    CLOSED = "closed"


class ConcordConflict(RuntimeError):
    """A typed terminal conflict while reading or mutating Concord authority."""

    def __init__(
        self,
        code: ConcordConflictCode,
        message: str,
        *,
        key: str | None = None,
        expected_pointer: ContractPointer | None = None,
        observed_pointer: ContractPointer | None = None,
    ) -> None:
        if not isinstance(code, ConcordConflictCode):
            raise TypeError("ConcordConflict code must be ConcordConflictCode")
        super().__init__(message)
        self.code = code
        self.message = message
        self.key = key
        self.expected_pointer = expected_pointer
        self.observed_pointer = observed_pointer


class ConcordUnavailable(RuntimeError):
    """A typed failure to obtain safe Concord store or source state."""

    def __init__(
        self,
        code: ConcordUnavailableCode,
        message: str,
        *,
        bucket: str | None = None,
        key: str | None = None,
    ) -> None:
        if not isinstance(code, ConcordUnavailableCode):
            raise TypeError("ConcordUnavailable code must be ConcordUnavailableCode")
        super().__init__(message)
        self.code = code
        self.message = message
        self.bucket = bucket
        self.key = key


class ContractState(StrEnum):
    OPEN = "open"
    CANCELLED = "cancelled"


class ContractValidityStatus(StrEnum):
    VALID = "valid"
    NOT_YET_FULFILLED = "not_yet_fulfilled"
    CANCELLED = "cancelled"
    MISSING_CONTRACT = "missing_contract"
    INVALID_CONTRACT = "invalid_contract"
    INVALID_TOKEN = "invalid_token"
    MISSING_TOKEN = "missing_token"
    GENERATION_MISMATCH = "generation_mismatch"
    SESSION_MISMATCH = "session_mismatch"
    TERMS_HASH_MISMATCH = "terms_hash_mismatch"
    UNAVAILABLE = "unavailable"


class ContractValidityReason(StrEnum):
    SOURCE_UNAVAILABLE = "source_unavailable"
    CONTRACT_MISSING = "contract_missing"
    CONTRACT_MALFORMED = "contract_malformed"
    CONTRACT_KEY_MISMATCH = "contract_key_mismatch"
    CONTRACT_POINTER_MISMATCH = "contract_pointer_mismatch"
    CONTRACT_CANCELLED = "contract_cancelled"
    PARTICIPANT_NOT_ATTACHED = "participant_not_attached"
    TOKEN_MISSING = "token_missing"
    TOKEN_MALFORMED = "token_malformed"
    TOKEN_KEY_MISMATCH = "token_key_mismatch"
    TOKEN_CONTRACT_MISMATCH = "token_contract_mismatch"
    TOKEN_GENERATION_MISMATCH = "token_generation_mismatch"
    TOKEN_PARTICIPANT_MISMATCH = "token_participant_mismatch"
    TOKEN_TERMS_HASH_MISMATCH = "token_terms_hash_mismatch"
    TOKEN_SESSION_MISMATCH = "token_session_mismatch"
    TOKEN_LOCAL_AUTHORITY_LOST = "token_local_authority_lost"


def require_text(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if value.strip() != value:
        raise ValueError(
            f"{field_name} must not contain leading or trailing whitespace"
        )
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    return value


class TokenObservation(WireModel):
    generation: int
    refresh_seq: int | None = Field(default=None, alias="refreshSeq")
    revision: int | None = None
    token_hash: str | None = Field(default=None, alias="tokenHash")

    @field_validator("generation")
    @classmethod
    def _validate_generation(cls, value: int) -> int:
        if value < 1:
            raise ValueError("generation must be greater than zero")
        return value

    @field_validator("refresh_seq", "revision")
    @classmethod
    def _validate_optional_non_negative(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("token observation values must be non-negative")
        return value
