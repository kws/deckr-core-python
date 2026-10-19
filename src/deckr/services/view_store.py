"""View store."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from typing import Any

import anyio

from deckr.core.kv import (
    JsonKvBucket,
    KvMaterializedChange,
    KvMaterializedSnapshot,
    KvUnavailable,
)
from deckr.core.kv_source import KvMaterializedSource
from deckr.core.util.anyio import CoalescedStateBroadcaster
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
    _entry_matches_fence,
    _entry_matches_read_context,
    _fenced_payload,
    _is_materialized_bucket,
    _parse_service_view_entry,
    _payload_hash,
    _read_fence,
    _service_view_entry_from_kv,
    _ServiceViewLeaseFence,
    _storage_key,
    _storage_key_for_context,
    logger,
)

AuthorityCheck = Callable[[], Awaitable[object]]


class ServiceViewStore:
    """Exact fenced service-view store with a coalesced materialized read model.

    Managed callers supply a local ``require_authority`` check. It runs after
    readiness waits, immediately before cached access or an exact mutation;
    immutable fence fields alone do not establish live agreement authority.
    """

    def __init__(
        self,
        *,
        bucket: JsonKvBucket | KvMaterializedSource,
    ) -> None:
        self._bucket = (
            bucket
            if _is_materialized_bucket(bucket)
            else KvMaterializedSource(bucket=bucket)
        )
        self._exact = self._bucket.exact_bucket
        self._entries: dict[str, ServiceViewEntry] = {}
        self._observed_revision: dict[str, int] = {}
        self._revision_condition = anyio.Condition()
        self._ready = anyio.Event()
        self._started = False
        self._state = CoalescedStateBroadcaster[str](current=False)
        self._closed = False

    @property
    def bucket(self) -> str:
        return self._bucket.bucket

    def start(self, task_group: anyio.abc.TaskGroup) -> None:
        self._bucket.start(task_group)
        if self._started:
            return
        self._started = True
        task_group.start_soon(self._event_loop)

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._state.aclose()
        await self._bucket.aclose()
        await self._notify_revisions()

    async def wait_ready(self) -> None:
        await self._ready.wait()

    def is_current(self) -> bool:
        return (
            self._bucket.is_current() and self._ready.is_set() and self._state.current
        )

    async def wait_current(self) -> None:
        while not self.is_current():
            await self._bucket.wait_current()
            if self.is_current():
                return
            await anyio.sleep(0)

    async def get(
        self,
        context: ServiceViewReadContext,
        view: ServiceViewRef,
        *,
        require_authority: AuthorityCheck | None = None,
    ) -> ServiceViewEntry | None:
        self._assert_read_authorized(context, view)
        await self.wait_current()
        if require_authority is not None:
            await require_authority()
        entry = self._entries.get(_storage_key_for_context(view, context))
        if entry is None or not _entry_matches_read_context(entry, context):
            return None
        return entry

    async def put(
        self,
        *,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        context: ServiceViewWriteContext,
        revision: int | None = None,
        ttl: float | None = None,
        require_authority: AuthorityCheck | None = None,
    ) -> ServiceViewEntry:
        self._assert_write_authorized(context, view)
        if self._started:
            await self.wait_current()
        if require_authority is not None:
            await require_authority()
        storage_key = _storage_key(view.key, context.contract)
        value = _fenced_payload(payload, view_key=view.key, context=context)
        entry = (
            await self._exact.put(storage_key, value, ttl=ttl)
            if revision is None
            else await self._exact.update(
                storage_key,
                value,
                revision=revision,
                ttl=ttl,
            )
        )
        await self._observe_write(storage_key, entry.revision)
        service_entry = _service_view_entry_from_kv(entry)
        logger.debug(
            "Service view write bucket=%s key=%s revision=%s service=%s "
            "namespace=%s session=%s operation=%s payload_hash=%s",
            self.bucket,
            storage_key,
            entry.revision,
            context.service_id,
            context.service_namespace,
            context.service_session_id,
            "put" if revision is None else "update",
            _payload_hash(payload),
        )
        return service_entry

    async def create(
        self,
        *,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        context: ServiceViewWriteContext,
        ttl: float | None = None,
        require_authority: AuthorityCheck | None = None,
    ) -> ServiceViewEntry:
        self._assert_write_authorized(context, view)
        if self._started:
            await self.wait_current()
        if require_authority is not None:
            await require_authority()
        storage_key = _storage_key(view.key, context.contract)
        value = _fenced_payload(payload, view_key=view.key, context=context)
        entry = await self._exact.create(storage_key, value, ttl=ttl)
        await self._observe_write(storage_key, entry.revision)
        service_entry = _service_view_entry_from_kv(entry)
        logger.debug(
            "Service view write bucket=%s key=%s revision=%s service=%s "
            "namespace=%s session=%s operation=create payload_hash=%s",
            self.bucket,
            storage_key,
            entry.revision,
            context.service_id,
            context.service_namespace,
            context.service_session_id,
            _payload_hash(payload),
        )
        return service_entry

    async def update(
        self,
        *,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        context: ServiceViewWriteContext,
        revision: int,
        ttl: float | None = None,
        require_authority: AuthorityCheck | None = None,
    ) -> ServiceViewEntry:
        return await self.put(
            view=view,
            payload=payload,
            context=context,
            revision=revision,
            ttl=ttl,
            require_authority=require_authority,
        )

    async def delete(
        self,
        *,
        view: ServiceViewRef,
        context: ServiceViewWriteContext,
        revision: int | None = None,
        require_authority: AuthorityCheck | None = None,
    ) -> None:
        self._assert_write_authorized(context, view)
        if self._started:
            await self.wait_current()
        if require_authority is not None:
            await require_authority()
        storage_key = _storage_key(view.key, context.contract)
        marker_revision = await self._exact.delete(storage_key, revision=revision)
        if marker_revision is None:
            return
        await self._observe_write(storage_key, marker_revision)
        logger.debug(
            "Service view write bucket=%s key=%s revision=%s operation=delete",
            self.bucket,
            storage_key,
            marker_revision,
        )

    async def _observe_write(self, key: str, revision: int) -> None:
        if not self._started:
            return
        await self._bucket.wait_for_revision(key, revision)
        while self._observed_revision.get(key, 0) < revision:
            if self._closed:
                raise KvUnavailable(
                    "Service view closed before observing "
                    f"revision {revision} for {key!r}"
                )
            async with self._revision_condition:
                if self._observed_revision.get(key, 0) >= revision:
                    return
                await self._revision_condition.wait()

    @asynccontextmanager
    async def watch(
        self,
        context: ServiceViewReadContext,
        view: ServiceViewRef,
        *,
        require_authority: AuthorityCheck | None = None,
    ) -> AsyncIterator[
        AsyncIterator[ServiceViewWatchSnapshot | ServiceViewWatchChange]
    ]:
        self._assert_read_authorized(context, view)
        await self.wait_current()
        if require_authority is not None:
            await require_authority()
        fence = _read_fence(context)
        storage_key = _storage_key_for_context(view, context)
        async with self._state.subscribe(
            lambda version, current: self._watch_snapshot_locked(
                storage_key,
                fence,
                version,
                current,
            )
        ) as subscription:

            async def stream() -> AsyncIterator[
                ServiceViewWatchSnapshot | ServiceViewWatchChange
            ]:
                initial = subscription.initial
                last_entry = initial.entry
                last_current = initial.current
                yield initial
                async for wakeup in subscription:
                    if (
                        not wakeup.resnapshot_required
                        and storage_key not in wakeup.changed
                        and wakeup.current == last_current
                    ):
                        continue
                    try:
                        snapshot = await self._state.capture(
                            lambda version, current: self._watch_snapshot_locked(
                                storage_key,
                                fence,
                                version,
                                current,
                            )
                        )
                    except anyio.ClosedResourceError:
                        return
                    if (
                        not wakeup.resnapshot_required
                        and snapshot.entry == last_entry
                        and snapshot.current == last_current
                    ):
                        continue
                    last_entry = snapshot.entry
                    last_current = snapshot.current
                    yield ServiceViewWatchChange(
                        version=snapshot.version,
                        current=snapshot.current,
                        entry=snapshot.entry,
                        resnapshot_required=wakeup.resnapshot_required,
                    )

            yield stream()

    def _watch_snapshot_locked(
        self,
        storage_key: str,
        fence: _ServiceViewLeaseFence,
        version: int,
        current: bool,
    ) -> ServiceViewWatchSnapshot:
        entry = self._entries.get(storage_key)
        if entry is not None and not _entry_matches_fence(entry, fence):
            entry = None
        return ServiceViewWatchSnapshot(
            version=version,
            current=current,
            entry=entry,
        )

    async def _event_loop(self) -> None:
        try:
            async with self._bucket.subscribe() as changes:
                async for item in changes:
                    if isinstance(item, KvMaterializedSnapshot):
                        await self._install_snapshot(item)
                    else:
                        await self._consume_change(item)
        except anyio.ClosedResourceError:
            return

    async def _consume_change(self, change: KvMaterializedChange) -> None:
        if change.resnapshot_required:
            await self._install_snapshot(await self._bucket.snapshot())
            return
        parsed = {
            key: _parse_service_view_entry(self._bucket.get_cached(key))
            for key in change.changed_keys
        }
        async with self._state.lock:
            for key, entry in parsed.items():
                if entry is None:
                    self._entries.pop(key, None)
                else:
                    self._entries[key] = entry
                revision = self._bucket.revision_cached(key)
                if revision is not None:
                    self._observed_revision[key] = revision
            current_changed = self._state.current != change.current
            if change.changed_keys or current_changed:
                self._state.publish_locked(
                    change.changed_keys,
                    current=change.current,
                )
            if change.current:
                self._ready.set()
        await self._notify_revisions()

    async def _install_snapshot(self, snapshot: KvMaterializedSnapshot) -> None:
        entries = {
            entry.storage_key: entry
            for raw in snapshot.entries
            if (entry := _parse_service_view_entry(raw)) is not None
        }
        async with self._state.lock:
            previous_keys = set(self._entries)
            changed = frozenset(
                key
                for key in previous_keys | entries.keys()
                if self._entries.get(key) != entries.get(key)
            )
            self._entries = entries
            self._observed_revision = {
                raw.key: raw.revision for raw in snapshot.entries
            }
            for key in previous_keys - self._observed_revision.keys():
                revision = self._bucket.revision_cached(key)
                if revision is not None:
                    self._observed_revision[key] = revision
            current_changed = self._state.current != snapshot.current
            if changed or current_changed or not self._ready.is_set():
                self._state.publish_locked(
                    changed,
                    current=snapshot.current,
                    resnapshot_required=len(changed) > 256,
                )
            if snapshot.current:
                self._ready.set()
        await self._notify_revisions()

    async def _notify_revisions(self) -> None:
        async with self._revision_condition:
            self._revision_condition.notify_all()

    def _assert_read_authorized(
        self,
        context: ServiceViewReadContext,
        view: ServiceViewRef,
    ) -> None:
        if view.store_name != self.bucket:
            raise UnsupportedServiceScope(
                f"Service view {view.key!r} is not in bucket {self.bucket!r}"
            )
        for view_family in context.views.values():
            if view.store_name != view_family.store_name:
                continue
            if view.key.startswith(view_family.key_prefix):
                if view_family.writer == context.reader:
                    raise UnsupportedServiceScope(
                        f"Service view {view.key!r} is written by "
                        f"{context.reader.value}"
                    )
                return
        raise UnsupportedServiceScope(
            f"Service read context does not authorize view {view.key!r}"
        )

    def _assert_write_authorized(
        self,
        context: ServiceViewWriteContext,
        view: ServiceViewRef,
    ) -> None:
        if view.store_name != self.bucket:
            raise UnsupportedServiceScope(
                f"Service view {view.key!r} is not in bucket {self.bucket!r}"
            )
        for view_family in context.views.values():
            if view.store_name != view_family.store_name:
                continue
            if not view.key.startswith(view_family.key_prefix):
                continue
            if view_family.writer != context.writer:
                raise UnsupportedServiceScope(
                    f"Service view {view.key!r} writer is "
                    f"{view_family.writer.value}, not {context.writer.value}"
                )
            return
        raise UnsupportedServiceScope(
            f"Service write context does not authorize view {view.key!r}"
        )
