"""Generic Beacon wire records, selectors, and immutable advertisement intent."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, Protocol, TypeVar

from pydantic import AwareDatetime, Field, field_serializer, field_validator

from deckr.core.identity import EndpointAddress
from deckr.core.keys import decode_key_token, encode_key_token
from deckr.core.kv import KvConflict
from deckr.core.wire import JsonObject, WireModel, freeze_json, thaw_json

BEACON_ADVERTISEMENT_SCHEMA_ID = "dev.deckr.beacon.advertisement.v1"


class _BeaconAdvertisementMissing(KvConflict):
    pass


class _BeaconAdvertisementOwnerChanged(KvConflict):
    pass


class CandidateStatus(StrEnum):
    CANDIDATE = "candidate"
    MISSING = "missing"
    SCHEMA_INVALID = "schema_invalid"
    FEATURE_MISMATCH = "feature_mismatch"
    SESSION_MISMATCH = "session_mismatch"
    UNAVAILABLE = "unavailable"


class AdvertisementSelector(Protocol):
    def accepts(self, advertisement: AdvertisementRecord) -> bool: ...


AdvertisementFilter = Callable[["AdvertisementRecord"], bool] | AdvertisementSelector


T = TypeVar("T")


class BeaconEndpointSession(Protocol):
    address: EndpointAddress
    session_id: str


def _require_text(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if value.strip() != value:
        raise ValueError(
            f"{field_name} must not contain leading or trailing whitespace"
        )
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    return value


def beacon_advertisement_key(*, feature_id: str, advertisement_id: str) -> str:
    return ".".join(
        (
            "advertisements",
            "by_feature",
            encode_key_token(feature_id),
            encode_key_token(advertisement_id),
        )
    )


def parse_beacon_advertisement_key(key: str) -> tuple[str, str] | None:
    parts = key.split(".")
    if len(parts) != 4 or parts[:2] != ["advertisements", "by_feature"]:
        return None
    try:
        return decode_key_token(parts[2]), decode_key_token(parts[3])
    except (ValueError, UnicodeError):
        return None


def beacon_feature_prefix(feature_id: str) -> str:
    return ".".join(("advertisements", "by_feature", encode_key_token(feature_id), ""))


class BeaconProtocol(WireModel):
    namespace: str
    version: str

    @field_validator("namespace", "version")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _require_text(value, field_name="Beacon protocol field")


class AdvertisementRecord(WireModel):
    schema_id: Literal[BEACON_ADVERTISEMENT_SCHEMA_ID] = Field(
        default=BEACON_ADVERTISEMENT_SCHEMA_ID,
        alias="schema",
    )
    advertisement_id: str = Field(alias="advertisementId")
    feature_id: str = Field(alias="featureId")
    advertiser: EndpointAddress
    endpoint: EndpointAddress
    session_id: str = Field(alias="sessionId")
    refresh_seq: int = Field(alias="refreshSeq")
    ttl_seconds: int = Field(alias="ttlSeconds")
    protocol: BeaconProtocol | None = None
    operations: tuple[str, ...] = Field(default_factory=tuple)
    labels: Mapping[str, str] = Field(default_factory=dict)
    hints: JsonObject = Field(default_factory=dict)
    payload: JsonObject | None = None
    created_at: AwareDatetime | None = Field(default=None, alias="createdAt")
    updated_at: AwareDatetime | None = Field(default=None, alias="updatedAt")

    @field_validator("advertisement_id", "feature_id", "session_id")
    @classmethod
    def _validate_identity(cls, value: str) -> str:
        return _require_text(value, field_name="Beacon advertisement identity")

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

    @field_validator("operations")
    @classmethod
    def _validate_operations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            _require_text(item, field_name="Beacon operation") for item in value
        )

    @field_validator("labels", mode="after")
    @classmethod
    def _freeze_labels(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        return freeze_json(
            {
                _require_text(key, field_name="Beacon label key"): _require_text(
                    item,
                    field_name="Beacon label value",
                )
                for key, item in value.items()
            }
        )

    @field_validator("hints", "payload", mode="before")
    @classmethod
    def _thaw_json_object(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("hints", "payload", mode="after")
    @classmethod
    def _freeze_json_object(
        cls,
        value: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        return freeze_json(value) if value is not None else None

    @field_serializer("labels")
    def _serialize_labels(self, value: Mapping[str, str]) -> dict[str, str]:
        return thaw_json(value)

    @field_serializer("hints", "payload")
    def _serialize_json_object(
        self,
        value: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        return thaw_json(value) if value is not None else None

    @field_serializer("created_at", "updated_at")
    def _serialize_datetime(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


@dataclass(frozen=True, slots=True)
class _AdvertisementHandle:
    key: str
    advertisement_id: str
    feature_id: str
    advertiser: EndpointAddress
    endpoint: EndpointAddress
    session_id: str
    revision: int
    refresh_seq: int


@dataclass(frozen=True, slots=True)
class Candidate:
    key: str
    advertisement: AdvertisementRecord
    revision: int
    observed_at: datetime

    @property
    def endpoint(self) -> EndpointAddress:
        return self.advertisement.endpoint


@dataclass(frozen=True, slots=True)
class BeaconWatchSnapshot:
    version: int
    current: bool
    candidates: tuple[Candidate, ...]


@dataclass(frozen=True, slots=True)
class BeaconWatchChange:
    version: int
    current: bool
    candidates: tuple[Candidate, ...]
    changed_keys: frozenset[str]
    resnapshot_required: bool


BeaconDirectoryParser = Callable[[Candidate], T | list[T] | tuple[T, ...] | None]


BeaconDirectoryPredicate = Callable[[T], bool]


BeaconDirectorySelector = Callable[[Collection[T]], T | None]


@dataclass(frozen=True, slots=True)
class BeaconAdvertisementSpec:
    """Input specification for a core-owned Beacon advertisement lifecycle."""

    feature_id: str
    protocol: Mapping[str, str] | BeaconProtocol | None = None
    operations: Sequence[str] = ()
    labels: Mapping[str, str] | None = None
    hints: Mapping[str, Any] | None = None
    payload: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        feature_id = _require_text(self.feature_id, field_name="Beacon feature id")
        operations = tuple(
            _require_text(item, field_name="Beacon operation")
            for item in self.operations
        )
        object.__setattr__(self, "feature_id", feature_id)
        object.__setattr__(self, "operations", operations)
        object.__setattr__(
            self,
            "protocol",
            self.protocol
            if isinstance(self.protocol, BeaconProtocol) or self.protocol is None
            else freeze_json(self.protocol),
        )
        object.__setattr__(
            self,
            "labels",
            None if self.labels is None else freeze_json(self.labels),
        )
        object.__setattr__(
            self,
            "hints",
            None if self.hints is None else freeze_json(self.hints),
        )
        object.__setattr__(
            self,
            "payload",
            None if self.payload is None else freeze_json(self.payload),
        )
