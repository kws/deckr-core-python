"""Records."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from pydantic import (
    AwareDatetime,
    Field,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from deckr._concord._keys import canonical_json_hash
from deckr._concord._types import (
    CONCORD_CONTRACT_SCHEMA_ID,
    CONCORD_PARTICIPANT_TOKEN_SCHEMA_ID,
    ContractState,
    TokenObservation,
    require_text,
)
from deckr.core.identity import ContractPointer, EndpointAddress, parse_endpoint_address
from deckr.core.wire import JsonObject, WireModel, freeze_json, thaw_json


class ContractRecord(WireModel):
    schema_id: Literal[CONCORD_CONTRACT_SCHEMA_ID] = Field(
        default=CONCORD_CONTRACT_SCHEMA_ID,
        alias="schema",
    )
    contract_id: str = Field(alias="contractId")
    generation: int
    participants: tuple[EndpointAddress, ...]
    attached_participants: tuple[EndpointAddress, ...] = Field(
        alias="attachedParticipants"
    )
    state: ContractState = ContractState.OPEN
    profile: str | None = None
    terms_hash: str | None = Field(default=None, alias="termsHash")
    terms: JsonObject | None = None
    created_by: EndpointAddress | None = Field(default=None, alias="createdBy")
    participant_sessions: Mapping[
        str,
        Annotated[str, StringConstraints(min_length=1)],
    ] = Field(
        default_factory=dict,
        alias="participantSessions",
    )
    created_at: AwareDatetime | None = Field(default=None, alias="createdAt")
    cancelled_by: EndpointAddress | Literal["concord:maintenance"] | None = Field(
        default=None,
        alias="cancelledBy",
    )
    cancelled_at: AwareDatetime | None = Field(default=None, alias="cancelledAt")
    cancel_revision: int | None = Field(default=None, alias="cancelRevision", ge=0)
    cancel_reason: str | None = Field(default=None, alias="cancelReason")
    supersedes: ContractPointer | None = None

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

    @field_validator("participants", mode="after")
    @classmethod
    def _validate_participants(
        cls,
        value: tuple[EndpointAddress, ...],
    ) -> tuple[EndpointAddress, ...]:
        if not value:
            raise ValueError("Concord contracts require at least one participant")
        strings = [str(item) for item in value]
        if len(strings) != len(set(strings)):
            raise ValueError("Concord contract participants must be unique")
        if strings != sorted(strings):
            raise ValueError("Concord contract participants must be canonicalized")
        return value

    @field_validator("attached_participants", mode="after")
    @classmethod
    def _validate_attached_participants(
        cls,
        value: tuple[EndpointAddress, ...],
    ) -> tuple[EndpointAddress, ...]:
        strings = [str(item) for item in value]
        if len(strings) != len(set(strings)):
            raise ValueError("Concord attached participants must be unique")
        if strings != sorted(strings):
            raise ValueError("Concord attached participants must be canonicalized")
        return value

    @field_validator("profile", "terms_hash", "cancel_reason")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return require_text(value, field_name="Concord contract field")

    @field_validator("terms", mode="before")
    @classmethod
    def _thaw_terms(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("terms", mode="after")
    @classmethod
    def _freeze_terms(cls, value: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
        return freeze_json(value) if value is not None else None

    @field_serializer("terms")
    def _serialize_terms(
        self, value: Mapping[str, Any] | None
    ) -> dict[str, Any] | None:
        return thaw_json(value) if value is not None else None

    @field_validator("participant_sessions", mode="before")
    @classmethod
    def _thaw_participant_sessions(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("participant_sessions", mode="after")
    @classmethod
    def _freeze_participant_sessions(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        normalized = {
            str(parse_endpoint_address(participant)): require_text(
                session_id,
                field_name="Concord participant session id",
            )
            for participant, session_id in value.items()
        }
        return freeze_json(dict(sorted(normalized.items())))

    @field_serializer("participant_sessions")
    def _serialize_participant_sessions(
        self,
        value: Mapping[str, str],
    ) -> dict[str, str]:
        return dict(value)

    @field_serializer("created_at", "cancelled_at")
    def _serialize_datetime(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    @model_validator(mode="after")
    def _validate_terms_hash(self) -> ContractRecord:
        if self.terms is None:
            return self
        if self.terms_hash is None:
            raise ValueError("Concord contract terms require termsHash")
        if self.terms_hash != canonical_json_hash(self.terms):
            raise ValueError("Concord contract termsHash does not match terms")
        if self.profile is not None:
            profile = self.terms.get("profile")
            if profile is not None and profile != self.profile:
                raise ValueError("Concord contract profile must match terms.profile")
        return self

    @model_validator(mode="after")
    def _validate_attached_participants_subset(self) -> ContractRecord:
        participants = {str(item) for item in self.participants}
        attached = {str(item) for item in self.attached_participants}
        if not attached <= participants:
            raise ValueError("attachedParticipants must be a subset of participants")
        if not set(self.participant_sessions) <= participants:
            raise ValueError("participantSessions keys must be named participants")
        return self

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


class ParticipantTokenRecord(WireModel):
    schema_id: Literal[CONCORD_PARTICIPANT_TOKEN_SCHEMA_ID] = Field(
        default=CONCORD_PARTICIPANT_TOKEN_SCHEMA_ID,
        alias="schema",
    )
    contract_id: str = Field(alias="contractId")
    generation: int
    participant: EndpointAddress
    session_id: str = Field(alias="sessionId")
    token_id: str = Field(alias="tokenId")
    refresh_seq: int = Field(alias="refreshSeq")
    ttl_seconds: int = Field(alias="ttlSeconds")
    terms_hash: str | None = Field(default=None, alias="termsHash")
    contract_hash: str | None = Field(default=None, alias="contractHash")
    observed: Mapping[str, TokenObservation] = Field(default_factory=dict)

    @field_validator("contract_id", "session_id", "token_id")
    @classmethod
    def _validate_identity(cls, value: str) -> str:
        return require_text(value, field_name="Concord participant token identity")

    @field_validator("terms_hash", "contract_hash")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return require_text(value, field_name="Concord participant token field")

    @field_validator("generation")
    @classmethod
    def _validate_generation(cls, value: int) -> int:
        if value < 1:
            raise ValueError("generation must be greater than zero")
        return value

    @field_validator("refresh_seq")
    @classmethod
    def _validate_refresh_seq(cls, value: int) -> int:
        if value < 1:
            raise ValueError("refreshSeq must be greater than zero")
        return value

    @field_validator("ttl_seconds")
    @classmethod
    def _validate_ttl_seconds(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("ttlSeconds must be greater than zero")
        return value

    @field_validator("observed", mode="after")
    @classmethod
    def _freeze_observed(
        cls,
        value: Mapping[str, TokenObservation],
    ) -> Mapping[str, TokenObservation]:
        return freeze_json(value)

    @field_serializer("observed")
    def _serialize_observed(
        self,
        value: Mapping[str, TokenObservation],
    ) -> dict[str, dict[str, Any]]:
        return {
            key: item.model_dump(by_alias=True, exclude_none=True, mode="json")
            for key, item in value.items()
        }

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")
