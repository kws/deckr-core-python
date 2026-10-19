"""View access."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

from deckr.services.runtime import (
    ServiceViewReadContext,
    ServiceViewRef,
    ServiceViewWriteContext,
    UnsupportedServiceScope,
)
from deckr.services.view_records import (
    ServiceViewEntry,
    ServiceViewWatchChange,
    ServiceViewWatchSnapshot,
)
from deckr.services.view_store import ServiceViewStore


class ManagedServiceViewAccess:
    """Lease-bound retained-view access for one service-use contract."""

    def __init__(
        self,
        store: ServiceViewStore,
        *,
        read_context: ServiceViewReadContext | None = None,
        write_context: ServiceViewWriteContext | None = None,
    ) -> None:
        self._store = store
        self._read_context = read_context
        self._write_context = write_context

    async def read(self, view: ServiceViewRef) -> ServiceViewEntry | None:
        return await self._store.get(self._require_read_context(), view)

    @asynccontextmanager
    async def watch(
        self,
        view: ServiceViewRef,
    ) -> AsyncIterator[
        AsyncIterator[ServiceViewWatchSnapshot | ServiceViewWatchChange]
    ]:
        async with self._store.watch(self._require_read_context(), view) as changes:
            yield changes

    async def create(
        self,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        return await self._store.create(
            view=view,
            payload=payload,
            context=self._require_write_context(),
            ttl=ttl,
        )

    async def put(
        self,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        revision: int | None = None,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        return await self._store.put(
            view=view,
            payload=payload,
            context=self._require_write_context(),
            revision=revision,
            ttl=ttl,
        )

    async def update(
        self,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        revision: int,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        return await self._store.update(
            view=view,
            payload=payload,
            context=self._require_write_context(),
            revision=revision,
            ttl=ttl,
        )

    async def delete(
        self,
        view: ServiceViewRef,
        *,
        revision: int | None = None,
    ) -> None:
        await self._store.delete(
            view=view,
            context=self._require_write_context(),
            revision=revision,
        )

    def _require_read_context(self) -> ServiceViewReadContext:
        if self._read_context is None:
            raise UnsupportedServiceScope("managed service view access is write-only")
        return self._read_context

    def _require_write_context(self) -> ServiceViewWriteContext:
        if self._write_context is None:
            raise UnsupportedServiceScope("managed service view access is read-only")
        return self._write_context
