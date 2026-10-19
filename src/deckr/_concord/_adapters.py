"""Translate exact KV failures and materialized sources into Concord ports."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import anyio

from deckr._concord._models import (
    ConcordConflict,
    ConcordConflictCode,
    ConcordUnavailable,
    ConcordUnavailableCode,
)
from deckr._concord._ports import (
    ConcordMaintenanceScanPort,
    ConcordMaterializedSourcePort,
    ConcordRawMaintenanceStorePort,
    ExactConcordKvPort,
)
from deckr.core.kv import KvConflict, KvEntry, KvUnavailable
from deckr.core.kv_source import KvMaterializedSource
from deckr.core.wire import WireModel


def _normalize_materialized_bucket(value: KvMaterializedSource | Any) -> Any:
    if _is_materialized_bucket(value):
        return value
    return KvMaterializedSource(bucket=value)


def _is_materialized_bucket(value: Any) -> bool:
    return all(
        hasattr(value, name)
        for name in (
            "start",
            "wait_ready",
            "is_ready",
            "is_current",
            "wait_current",
            "exact_bucket",
            "get_cached",
            "items_cached",
            "revision_cached",
            "snapshot",
            "subscribe",
            "wait_for_revision",
        )
    )


class _ExactKvAdapter:
    def __init__(self, bucket: Any, *, source: Any | None = None) -> None:
        self._bucket = bucket
        self._source = source

    @property
    def bucket(self) -> str:
        return str(self._bucket.bucket)

    async def get_exact(self, key: str) -> KvEntry | None:
        get_exact = getattr(self._bucket, "get_exact", None)
        get_value = get_exact if get_exact is not None else self._bucket.get
        try:
            return await get_value(key)
        except (
            KvUnavailable,
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
        ) as exc:
            raise ConcordUnavailable(
                ConcordUnavailableCode.STORE_UNAVAILABLE,
                str(exc),
                bucket=self.bucket,
                key=key,
            ) from exc

    async def create(
        self,
        key: str,
        value: Mapping[str, Any] | WireModel,
        *,
        ttl: float | None = None,
    ) -> KvEntry:
        try:
            entry = await self._bucket.create(key, value, ttl=ttl)
            await self._wait_observed(key, entry.revision)
            return entry
        except KvConflict as exc:
            raise ConcordConflict(
                ConcordConflictCode.KEY_ALREADY_EXISTS,
                str(exc),
                key=key,
            ) from exc
        except (
            KvUnavailable,
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
        ) as exc:
            raise ConcordUnavailable(
                ConcordUnavailableCode.STORE_UNAVAILABLE,
                str(exc),
                bucket=self.bucket,
                key=key,
            ) from exc

    async def update(
        self,
        key: str,
        value: Mapping[str, Any] | WireModel,
        *,
        revision: int,
        ttl: float | None = None,
    ) -> KvEntry:
        try:
            entry = await self._bucket.update(
                key,
                value,
                revision=revision,
                ttl=ttl,
            )
            await self._wait_observed(key, entry.revision)
            return entry
        except KvConflict as exc:
            raise ConcordConflict(
                ConcordConflictCode.REVISION_CHANGED,
                str(exc),
                key=key,
            ) from exc
        except (
            KvUnavailable,
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
        ) as exc:
            raise ConcordUnavailable(
                ConcordUnavailableCode.STORE_UNAVAILABLE,
                str(exc),
                bucket=self.bucket,
                key=key,
            ) from exc

    async def delete(self, key: str, *, revision: int) -> int | None:
        try:
            marker_revision = await self._bucket.delete(key, revision=revision)
            if marker_revision is not None:
                await self._wait_observed(key, marker_revision)
            return marker_revision
        except KvConflict as exc:
            raise ConcordConflict(
                ConcordConflictCode.REVISION_CHANGED,
                str(exc),
                key=key,
            ) from exc
        except (
            KvUnavailable,
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
        ) as exc:
            raise ConcordUnavailable(
                ConcordUnavailableCode.STORE_UNAVAILABLE,
                str(exc),
                bucket=self.bucket,
                key=key,
            ) from exc

    async def _wait_observed(self, key: str, revision: int) -> None:
        if self._source is None:
            return
        await self._source.wait_for_revision(key, revision)

    async def ttl_seconds(self) -> int:
        ttl_seconds = getattr(self._bucket, "ttl_seconds", None)
        if ttl_seconds is None:
            raise ConcordUnavailable(
                ConcordUnavailableCode.TTL_METADATA_MISSING,
                f"Concord KV bucket {self.bucket!r} does not expose TTL metadata",
                bucket=self.bucket,
            )
        try:
            value = ttl_seconds()
            if hasattr(value, "__await__"):
                value = await value
        except KvUnavailable as exc:
            raise ConcordUnavailable(
                ConcordUnavailableCode.STORE_UNAVAILABLE,
                str(exc),
                bucket=self.bucket,
            ) from exc
        except (TypeError, ValueError, OverflowError) as exc:
            raise ConcordUnavailable(
                ConcordUnavailableCode.TTL_INVALID,
                f"Concord participant token bucket {self.bucket!r} TTL is invalid",
                bucket=self.bucket,
            ) from exc
        if value is None:
            raise ConcordUnavailable(
                ConcordUnavailableCode.TTL_METADATA_MISSING,
                f"Concord participant token bucket {self.bucket!r} must be TTL-bound",
                bucket=self.bucket,
            )
        try:
            ttl = float(value)
            rounded = int(ttl)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ConcordUnavailable(
                ConcordUnavailableCode.TTL_INVALID,
                f"Concord participant token bucket {self.bucket!r} TTL is invalid",
                bucket=self.bucket,
            ) from exc
        if ttl <= 0 or abs(ttl - rounded) > 0.001:
            raise ConcordUnavailable(
                ConcordUnavailableCode.TTL_INVALID,
                f"Concord participant token bucket {self.bucket!r} TTL must be positive whole seconds",
                bucket=self.bucket,
            )
        return rounded


class _MaterializedSourceAdapter:
    def __init__(self, bucket: Any) -> None:
        self._bucket = bucket

    @property
    def bucket(self) -> str:
        return str(self._bucket.bucket)

    @property
    def generation(self) -> int:
        return self._bucket.generation

    @property
    def version(self) -> int:
        return int(getattr(self._bucket, "version", 0))

    def start(self, task_group: anyio.abc.TaskGroup) -> None:
        self._bucket.start(task_group)

    def is_ready(self) -> bool:
        return bool(self._bucket.is_ready())

    def is_current(self) -> bool:
        return bool(self._bucket.is_current())

    async def wait_ready(self) -> None:
        try:
            await self._bucket.wait_ready()
        except KvUnavailable as exc:
            raise ConcordUnavailable(
                ConcordUnavailableCode.SOURCE_STALE,
                str(exc),
                bucket=self.bucket,
            ) from exc

    async def wait_current(self) -> None:
        try:
            await self._bucket.wait_current()
        except KvUnavailable as exc:
            raise ConcordUnavailable(
                ConcordUnavailableCode.SOURCE_STALE,
                str(exc),
                bucket=self.bucket,
            ) from exc

    async def aclose(self) -> None:
        await self._bucket.aclose()

    def get_cached(self, key: str) -> KvEntry | None:
        return self._bucket.get_cached(key)

    def items_cached(self, prefix: str = "") -> tuple[KvEntry, ...]:
        return self._bucket.items_cached(prefix)

    def revision_cached(self, key: str) -> int | None:
        revision_cached = getattr(self._bucket, "revision_cached", None)
        return None if revision_cached is None else revision_cached(key)

    async def snapshot(self):
        return await self._bucket.snapshot()

    @asynccontextmanager
    async def subscribe(self):
        async with self._bucket.subscribe() as changes:
            yield changes


class _MaintenanceScanAdapter:
    def __init__(self, bucket: Any) -> None:
        self._exact = _ExactKvAdapter(bucket)
        self._bucket = bucket

    @property
    def bucket(self) -> str:
        return self._exact.bucket

    async def items_exact(self, prefix: str = "") -> tuple[KvEntry, ...]:
        items_exact = getattr(self._bucket, "items_exact", None)
        items = items_exact if items_exact is not None else self._bucket.items
        try:
            return await items(prefix)
        except KvUnavailable as exc:
            raise ConcordUnavailable(
                ConcordUnavailableCode.STORE_UNAVAILABLE,
                str(exc),
                bucket=self.bucket,
            ) from exc

    async def get_exact(self, key: str) -> KvEntry | None:
        return await self._exact.get_exact(key)

    async def create(
        self,
        key: str,
        value: Mapping[str, Any] | WireModel,
        *,
        ttl: float | None = None,
    ) -> KvEntry:
        return await self._exact.create(key, value, ttl=ttl)

    async def update(
        self,
        key: str,
        value: Mapping[str, Any] | WireModel,
        *,
        revision: int,
        ttl: float | None = None,
    ) -> KvEntry:
        return await self._exact.update(
            key,
            value,
            revision=revision,
            ttl=ttl,
        )

    async def delete(self, key: str, *, revision: int) -> int | None:
        return await self._exact.delete(key, revision=revision)


@dataclass(frozen=True, slots=True)
class ConcordBucketAdapters:
    exact: ExactConcordKvPort
    source: ConcordMaterializedSourcePort


def concord_bucket_adapters(
    bucket: KvMaterializedSource | Any,
) -> ConcordBucketAdapters:
    materialized = _normalize_materialized_bucket(bucket)
    return ConcordBucketAdapters(
        exact=_ExactKvAdapter(materialized.exact_bucket, source=materialized),
        source=_MaterializedSourceAdapter(materialized),
    )


def concord_maintenance_scan_store(
    bucket: ConcordRawMaintenanceStorePort | ConcordMaintenanceScanPort,
) -> ConcordMaintenanceScanPort:
    """Adapt one raw KV bucket to the exact maintenance scan boundary.

    Unlike :func:`concord_bucket_adapters`, this constructor never composes a
    materialized view. Prefix scans therefore use the raw store's temporary
    list consumer and maintenance construction starts no watch or background
    task.
    """

    required_methods = {
        "exact read": ("get_exact", "get"),
        "exact prefix scan": ("items_exact", "items"),
        "create": ("create",),
        "revision update": ("update",),
        "revision delete": ("delete",),
    }
    missing = [
        operation
        for operation, names in required_methods.items()
        if not any(callable(getattr(bucket, name, None)) for name in names)
    ]
    if missing or not hasattr(bucket, "bucket"):
        operations = ", ".join(missing or ["bucket identity"])
        raise TypeError(f"Concord maintenance store is missing {operations}")
    return _MaintenanceScanAdapter(bucket)
