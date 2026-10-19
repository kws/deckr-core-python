"""Transport-neutral JSON KV records, policies, and wire encoding."""

from __future__ import annotations

import json
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Literal, Protocol

from anyio.abc import ObjectReceiveStream

from deckr.core.wire import WireModel, freeze_json, thaw_json


class KvConflict(RuntimeError):
    """Raised when a KV create or revision-checked write fails."""


class KvUnavailable(RuntimeError):
    """Raised when a JSON KV bucket cannot answer safely."""


class KvViewStatus(StrEnum):
    STARTING = "starting"
    READY = "ready"
    STALE = "stale"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class KvBucketPolicy:
    bucket: str
    ttl_seconds: float | None
    allow_write_ttl: bool = False
    description: str = "JSON KV bucket"

    def __post_init__(self) -> None:
        if not self.bucket:
            raise ValueError("bucket must not be empty")
        if self.ttl_seconds is not None and self.ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be greater than zero")
        if self.allow_write_ttl and self.ttl_seconds is None:
            raise ValueError("allow_write_ttl requires ttl_seconds")


@dataclass(frozen=True, slots=True)
class KvEntry:
    bucket: str
    key: str
    value: Mapping[str, Any]
    revision: int


@dataclass(frozen=True, slots=True)
class KvChange:
    bucket: str
    key: str
    revision: int
    operation: Literal["put", "delete", "expire"]
    entry: KvEntry | None = None
    marker_reason: str | None = None


@dataclass(frozen=True, slots=True)
class KvWatchBarrier:
    """Broker stream high-water captured before a raw KV watch was opened."""

    revision: int


@dataclass(frozen=True, slots=True)
class KvMaterializedSnapshot:
    version: int
    current: bool
    entries: tuple[KvEntry, ...]


@dataclass(frozen=True, slots=True)
class KvMaterializedChange:
    version: int
    current: bool
    changed_keys: frozenset[str]
    resnapshot_required: bool


def kv_value(value: Mapping[str, Any] | WireModel) -> Mapping[str, Any]:
    if isinstance(value, WireModel):
        return freeze_json(
            value.model_dump(by_alias=True, exclude_none=True, mode="json")
        )
    return freeze_json(dict(value))


def kv_payload(value: Mapping[str, Any]) -> bytes:
    return json.dumps(thaw_json(value), separators=(",", ":")).encode("utf-8")


class JsonKvBucket(Protocol):
    """Exact JSON KV operations required by protocol and extension runtimes."""

    @property
    def bucket(self) -> str: ...
    async def ttl_seconds(self) -> float | None: ...
    async def get(self, key: str) -> KvEntry | None: ...
    async def items(self, prefix: str = "") -> tuple[KvEntry, ...]: ...
    async def create(
        self,
        key: str,
        value: Mapping[str, Any] | WireModel,
        *,
        ttl: float | None = None,
    ) -> KvEntry: ...
    async def put(
        self,
        key: str,
        value: Mapping[str, Any] | WireModel,
        *,
        revision: int | None = None,
        ttl: float | None = None,
    ) -> KvEntry: ...
    async def update(
        self,
        key: str,
        value: Mapping[str, Any] | WireModel,
        *,
        revision: int,
        ttl: float | None = None,
    ) -> KvEntry: ...
    async def delete(self, key: str, *, revision: int | None = None) -> int | None: ...
    def watch(
        self, prefix: str = ""
    ) -> AbstractAsyncContextManager[
        ObjectReceiveStream[KvChange | KvWatchBarrier]
    ]: ...
