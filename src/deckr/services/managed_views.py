"""Consumer and provider view access with continuous authority checks."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from deckr.core.util.guarded_stream import guarded_changes
from deckr.services.runtime import (
    ServiceUseLease,
    ServiceViewReadContext,
    ServiceViewRef,
    ServiceViewWriteContext,
)
from deckr.services.view_store import AuthorityCheck
from deckr.services.views import (
    ManagedServiceViewAccess,
    ServiceViewEntry,
    ServiceViewStore,
    ServiceViewWatchChange,
    ServiceViewWatchSnapshot,
)

if TYPE_CHECKING:
    from deckr.services.provider_channel import ManagedServiceContract


class _ServiceManagedServiceViewAccess(ManagedServiceViewAccess):
    def __init__(
        self,
        store: ServiceViewStore,
        *,
        contract: ManagedServiceContract,
        require_current: AuthorityCheck | None = None,
    ) -> None:
        super().__init__(store)
        self._contract = contract
        self._require_current = require_current

    async def _context(self):
        context = await self._contract._context()
        if self._require_current is not None:
            await self._require_current()
        return context

    async def read(self, view: ServiceViewRef) -> ServiceViewEntry | None:
        context = await self._context()
        return await self._store.get(
            context.view_read_context(), view, require_authority=self._context
        )

    @asynccontextmanager
    async def watch(
        self,
        view: ServiceViewRef,
    ) -> AsyncIterator[
        AsyncIterator[ServiceViewWatchSnapshot | ServiceViewWatchChange]
    ]:
        context = await self._context()
        async with self._store.watch(
            context.view_read_context(), view, require_authority=self._context
        ) as changes:
            yield guarded_changes(
                changes,
                check=self._context,
                watch=self._contract._participant.watch_state,
            )

    async def create(
        self,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        context = await self._context()
        return await self._store.create(
            view=view,
            payload=payload,
            context=context.view_write_context(),
            ttl=ttl,
            require_authority=self._context,
        )

    async def put(
        self,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        revision: int | None = None,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        context = await self._context()
        return await self._store.put(
            view=view,
            payload=payload,
            context=context.view_write_context(),
            revision=revision,
            ttl=ttl,
            require_authority=self._context,
        )

    async def update(
        self,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        revision: int,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        context = await self._context()
        return await self._store.update(
            view=view,
            payload=payload,
            context=context.view_write_context(),
            revision=revision,
            ttl=ttl,
            require_authority=self._context,
        )

    async def delete(
        self,
        view: ServiceViewRef,
        *,
        revision: int | None = None,
    ) -> None:
        context = await self._context()
        await self._store.delete(
            view=view,
            context=context.view_write_context(),
            revision=revision,
            require_authority=self._context,
        )


class _LeaseManagedServiceViewAccess(ManagedServiceViewAccess):
    def __init__(
        self,
        store: ServiceViewStore,
        *,
        lease: ServiceUseLease,
        read_context: ServiceViewReadContext | None = None,
        write_context: ServiceViewWriteContext | None = None,
    ) -> None:
        super().__init__(
            store,
            read_context=read_context,
            write_context=write_context,
            require_authority=lease.require_authority,
        )
        self._lease = lease

    async def read(self, view: ServiceViewRef) -> ServiceViewEntry | None:
        await self._lease.require_authority()
        return await super().read(view)

    @asynccontextmanager
    async def watch(
        self,
        view: ServiceViewRef,
    ) -> AsyncIterator[
        AsyncIterator[ServiceViewWatchSnapshot | ServiceViewWatchChange]
    ]:
        await self._lease.require_authority()
        async with super().watch(view) as changes:
            yield guarded_changes(
                changes,
                check=self._lease.require_authority,
                watch=self._lease.agreement.watch_state,
            )

    async def create(
        self,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        await self._lease.require_authority()
        return await super().create(view, payload, ttl=ttl)

    async def put(
        self,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        revision: int | None = None,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        await self._lease.require_authority()
        return await super().put(view, payload, revision=revision, ttl=ttl)

    async def update(
        self,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        revision: int,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        await self._lease.require_authority()
        return await super().update(view, payload, revision=revision, ttl=ttl)

    async def delete(
        self,
        view: ServiceViewRef,
        *,
        revision: int | None = None,
    ) -> None:
        await self._lease.require_authority()
        await super().delete(view, revision=revision)
