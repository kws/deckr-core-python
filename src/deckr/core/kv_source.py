"""Current-state materialization from an ordered JSON KV watch."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import anyio

from deckr.core.kv import (
    JsonKvBucket,
    KvChange,
    KvEntry,
    KvMaterializedChange,
    KvMaterializedSnapshot,
    KvUnavailable,
    KvViewStatus,
    KvWatchBarrier,
)
from deckr.core.util.anyio import CoalescedStateBroadcaster, CoalescedStateSubscription

logger = logging.getLogger(__name__)

MATERIALIZED_TOMBSTONE_LIMIT = 2_000


class _KvMaterializedWatch:
    def __init__(
        self,
        subscription: CoalescedStateSubscription[
            str,
            KvMaterializedSnapshot,
        ],
    ) -> None:
        self._subscription = subscription
        self._initial: KvMaterializedSnapshot | None = subscription.initial

    def __aiter__(self) -> _KvMaterializedWatch:
        return self

    async def __anext__(self) -> KvMaterializedSnapshot | KvMaterializedChange:
        try:
            return await self.receive()
        except (anyio.ClosedResourceError, anyio.EndOfStream):
            raise StopAsyncIteration from None

    async def receive(self) -> KvMaterializedSnapshot | KvMaterializedChange:
        if self._initial is not None:
            initial = self._initial
            self._initial = None
            return initial
        change = await self._subscription.receive()
        return KvMaterializedChange(
            version=change.version,
            current=change.current,
            changed_keys=change.changed,
            resnapshot_required=change.resnapshot_required,
        )


class KvMaterializedSource:
    """Read-only, current-state materialization of one JSON KV bucket.

    The raw bucket remains the sole exact/CAS writer. This source installs each
    broker recovery snapshot atomically at its typed high-water barrier and
    emits only bounded coalesced wakeups afterward.
    """

    def __init__(
        self,
        *,
        bucket: JsonKvBucket,
        key_prefix: str = "",
    ) -> None:
        self._bucket = bucket
        self.key_prefix = key_prefix
        self._ready = anyio.Event()
        self._current_event = anyio.Event()
        self._run_started = anyio.Event()
        self._closed_event = anyio.Event()
        self._revision_condition = anyio.Condition()
        self._status = KvViewStatus.STARTING
        self._generation = 0
        self._started = False
        self._closed = False
        self._cancel_scope: anyio.CancelScope | None = None
        self._entries: dict[str, KvEntry] = {}
        self._tombstone_revision_by_key: dict[str, int] = {}
        self._high_water_revision = 0
        self._broadcaster = CoalescedStateBroadcaster[str](current=False)

    @property
    def bucket(self) -> str:
        return str(self._bucket.bucket)

    @property
    def exact_bucket(self) -> Any:
        """The separate exact/CAS store used by an owning runtime facade."""

        return self._bucket

    @property
    def status(self) -> KvViewStatus:
        return self._status

    @property
    def generation(self) -> int:
        """Monotonic continuity epoch, retained even when wakeups coalesce."""
        return self._generation

    @property
    def version(self) -> int:
        return self._broadcaster.version

    @property
    def high_water_revision(self) -> int:
        return self._high_water_revision

    @property
    def tombstone_count(self) -> int:
        return len(self._tombstone_revision_by_key)

    async def ttl_seconds(self) -> float | None:
        ttl_seconds = getattr(self._bucket, "ttl_seconds", None)
        if ttl_seconds is None:
            return None
        value = ttl_seconds()
        if hasattr(value, "__await__"):
            value = await value
        return value

    def start(self, task_group: anyio.abc.TaskGroup) -> None:
        if self._started:
            return
        if self._closed:
            raise RuntimeError("materialized KV source is closed")
        self._started = True
        task_group.start_soon(self._run)

    async def _run(self) -> None:
        try:
            with anyio.CancelScope() as cancel_scope:
                self._cancel_scope = cancel_scope
                self._run_started.set()
                await self._watch_loop()
        finally:
            self._cancel_scope = None
            await self._set_status(KvViewStatus.CLOSED)
            await self._broadcaster.aclose()
            self._closed_event.set()

    def is_ready(self) -> bool:
        return self._ready.is_set()

    def is_current(self) -> bool:
        return self._status == KvViewStatus.READY

    async def wait_ready(self) -> None:
        await self._ready.wait()

    async def wait_current(self) -> None:
        while self._status != KvViewStatus.READY:
            if self._status == KvViewStatus.CLOSED:
                raise KvUnavailable(
                    f"NATS KV materialized view is closed bucket={self.bucket!r}"
                )
            event = self._current_event
            await event.wait()

    async def wait_for_revision(self, key: str, revision: int) -> None:
        """Wait until the broker watch has observed a committed exact revision."""

        if not self._started:
            return
        while not self._revision_was_observed(key, revision):
            if self._status == KvViewStatus.CLOSED:
                raise KvUnavailable(
                    f"NATS KV materialized view closed before observing {key!r} "
                    f"revision {revision}"
                )
            async with self._revision_condition:
                if self._revision_was_observed(key, revision):
                    return
                await self._revision_condition.wait()

    def _revision_was_observed(self, key: str, revision: int) -> bool:
        cached = self.revision_cached(key)
        return (cached is not None and cached >= revision) or (
            self._high_water_revision >= revision
        )

    def get_cached(self, key: str) -> KvEntry | None:
        return self._entries.get(key)

    def items_cached(self, prefix: str = "") -> tuple[KvEntry, ...]:
        return tuple(
            entry
            for key, entry in sorted(self._entries.items())
            if key.startswith(prefix)
        )

    def revision_cached(self, key: str) -> int | None:
        entry = self._entries.get(key)
        if entry is not None:
            return entry.revision
        return self._tombstone_revision_by_key.get(key)

    async def aclose(self) -> None:
        if self._closed:
            if self._started:
                await self._closed_event.wait()
            return
        self._closed = True
        if not self._started:
            await self._set_status(KvViewStatus.CLOSED)
            await self._broadcaster.aclose()
            self._closed_event.set()
            return
        await self._run_started.wait()
        cancel_scope = self._cancel_scope
        if cancel_scope is not None:
            cancel_scope.cancel()
        await self._closed_event.wait()

    @asynccontextmanager
    async def subscribe(self) -> AsyncIterator[_KvMaterializedWatch]:
        async with self._broadcaster.subscribe(self._snapshot_locked) as subscription:
            yield _KvMaterializedWatch(subscription)

    def _snapshot_locked(self, version: int, current: bool) -> KvMaterializedSnapshot:
        return KvMaterializedSnapshot(
            version=version,
            current=current,
            entries=tuple(entry for _, entry in sorted(self._entries.items())),
        )

    async def snapshot(self) -> KvMaterializedSnapshot:
        return await self._broadcaster.capture(self._snapshot_locked)

    async def _watch_loop(self) -> None:
        retry_seconds = 1.0
        while True:
            if self._ready.is_set():
                await self._set_status(KvViewStatus.STALE)
            try:
                await self._consume_one_watch()
            except anyio.get_cancelled_exc_class():
                raise
            except Exception:
                logger.warning(
                    "KV materialized watch failed bucket=%s prefix=%s",
                    self.bucket,
                    self.key_prefix,
                    exc_info=True,
                )
                await anyio.sleep(retry_seconds)

    async def _consume_one_watch(self) -> bool:
        recovered: dict[str, KvChange] = {}
        post_barrier: dict[str, KvChange] = {}
        barrier_seen = False
        async with self._bucket.watch(self.key_prefix) as changes:
            try:
                async for item in changes:
                    if isinstance(item, KvWatchBarrier):
                        if barrier_seen:
                            continue
                        barrier_seen = True
                        compact = await self._install_recovered_snapshot(
                            recovered,
                            post_barrier,
                            barrier=item,
                        )
                        if compact:
                            return True
                        continue
                    change = item
                    if self.key_prefix and not change.key.startswith(self.key_prefix):
                        continue
                    if not barrier_seen:
                        # The raw watch's typed high-water determines which changes
                        # form the recovered snapshot. A change newer than that
                        # barrier is classified when the barrier item arrives.
                        recovered[change.key] = _newer_change(
                            recovered.get(change.key), change
                        )
                        continue
                    if await self._apply_change(change):
                        return True
            finally:
                with anyio.CancelScope(shield=True):
                    await self._set_status(
                        KvViewStatus.CLOSED if self._closed else KvViewStatus.STALE
                    )
        return False

    async def _install_recovered_snapshot(
        self,
        observed: Mapping[str, KvChange],
        post_barrier: Mapping[str, KvChange],
        *,
        barrier: KvWatchBarrier,
    ) -> bool:
        # Raw NATS watches can include a change newer than the captured stream
        # high-water in their initial replay. Split those changes before the
        # atomic install, then apply them in stream-revision order.
        snapshot_changes = {
            key: change
            for key, change in observed.items()
            if change.revision <= barrier.revision
        }
        newer = {
            key: change
            for key, change in observed.items()
            if change.revision > barrier.revision
        }
        for key, change in post_barrier.items():
            newer[key] = _newer_change(newer.get(key), change)

        entries: dict[str, KvEntry] = {}
        for change in snapshot_changes.values():
            if change.operation == "put" and change.entry is not None:
                entries[change.key] = change.entry

        tombstones: dict[str, int] = {}
        for change in sorted(newer.values(), key=lambda item: item.revision):
            current_entry = entries.get(change.key)
            current_revision = (
                current_entry.revision if current_entry is not None else 0
            )
            current_revision = max(current_revision, tombstones.get(change.key, 0))
            if change.revision <= current_revision:
                continue
            if change.operation == "put" and change.entry is not None:
                entries[change.key] = change.entry
                tombstones.pop(change.key, None)
            else:
                entries.pop(change.key, None)
                tombstones[change.key] = change.revision

        for key in self._entries.keys() - entries.keys():
            tombstones.setdefault(key, barrier.revision)

        compact = len(tombstones) >= MATERIALIZED_TOMBSTONE_LIMIT
        if len(tombstones) > MATERIALIZED_TOMBSTONE_LIMIT:
            newest_tombstones = sorted(
                tombstones.items(),
                key=lambda item: item[1],
                reverse=True,
            )[:MATERIALIZED_TOMBSTONE_LIMIT]
            tombstones = dict(newest_tombstones)

        installed_high_water = max(
            (barrier.revision, *(change.revision for change in newer.values()))
        )

        async with self._broadcaster.lock:
            changed = _changed_entry_keys(self._entries, entries)
            self._entries = entries
            self._tombstone_revision_by_key = tombstones
            self._high_water_revision = installed_high_water
            self._status = KvViewStatus.READY
            self._current_event.set()
            self._ready.set()
            self._broadcaster.publish_locked(
                changed,
                current=True,
                resnapshot_required=len(changed) > 256,
            )
        await self._notify_revision_waiters()
        return compact

    async def _apply_change(self, change: KvChange) -> bool:
        """Apply one ordered live broker observation.

        Returns true when the bounded tombstone metadata reached its compaction
        threshold and the watch should be reopened immediately.
        """

        if self.key_prefix and not change.key.startswith(self.key_prefix):
            return False
        async with self._broadcaster.lock:
            current_revision = self.revision_cached(change.key) or 0
            if change.revision <= current_revision:
                return False
            self._high_water_revision = max(self._high_water_revision, change.revision)
            if change.operation == "put" and change.entry is not None:
                self._entries[change.key] = change.entry
                self._tombstone_revision_by_key.pop(change.key, None)
            else:
                self._entries.pop(change.key, None)
                self._tombstone_revision_by_key[change.key] = change.revision
            self._broadcaster.publish_locked((change.key,), current=self.is_current())
            compact = (
                len(self._tombstone_revision_by_key) >= MATERIALIZED_TOMBSTONE_LIMIT
            )
        await self._notify_revision_waiters()
        return compact

    async def _set_status(self, status: KvViewStatus) -> None:
        async with self._broadcaster.lock:
            if self._status == KvViewStatus.CLOSED and status != KvViewStatus.CLOSED:
                return
            if self._status == status:
                return
            if self._status == KvViewStatus.READY and status != KvViewStatus.READY:
                self._generation += 1
            self._status = status
            current = status == KvViewStatus.READY
            if current:
                self._current_event.set()
            else:
                self._current_event = anyio.Event()
            if status == KvViewStatus.READY:
                self._ready.set()
            if status != KvViewStatus.CLOSED:
                self._broadcaster.publish_locked((), current=current)
        await self._notify_revision_waiters()

    async def _notify_revision_waiters(self) -> None:
        async with self._revision_condition:
            self._revision_condition.notify_all()


def _newer_change(previous: KvChange | None, change: KvChange) -> KvChange:
    if previous is None or change.revision > previous.revision:
        return change
    return previous


def _changed_entry_keys(
    previous: Mapping[str, KvEntry],
    current: Mapping[str, KvEntry],
) -> frozenset[str]:
    return frozenset(
        key
        for key in previous.keys() | current.keys()
        if previous.get(key) != current.get(key)
    )
