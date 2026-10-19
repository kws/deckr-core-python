"""Current Beacon candidates filtered into a caller-defined typed directory."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from threading import RLock
from typing import TYPE_CHECKING, Generic

import anyio

from deckr._beacon._models import (
    BeaconDirectoryParser,
    BeaconDirectoryPredicate,
    BeaconDirectorySelector,
    Candidate,
    T,
    _require_text,
)
from deckr._beacon._support import logger
from deckr.core.kv import KvUnavailable

if TYPE_CHECKING:
    from deckr._beacon._api import Beacon


class BeaconDirectory(Generic[T]):
    """Generic local parsed-record view over one Beacon feature."""

    def __init__(
        self,
        beacon: Beacon,
        feature_id: str,
        parser: BeaconDirectoryParser[T],
        *,
        log_label: str = "BeaconDirectory",
    ) -> None:
        self._beacon = beacon
        self.feature_id = _require_text(feature_id, field_name="Beacon feature id")
        self._parser = parser
        self._log_label = _require_text(
            log_label, field_name="Beacon directory log label"
        )
        self._ready = anyio.Event()
        self._changed = anyio.Event()
        self._closed = False
        self._started = False
        self._current = False
        self._cancel_scope: anyio.CancelScope | None = None
        self._records_by_key: dict[str, tuple[T, ...]] = {}
        self._version = 0
        self._lock = RLock()

    def start(self, task_group: anyio.abc.TaskGroup) -> None:
        self.start_soon(task_group.start_soon)

    def start_soon(self, start_soon: Callable[..., object] | None) -> None:
        if start_soon is None or self._closed or self._started:
            return
        self._started = True
        start_soon(self._event_loop)

    async def wait_ready(self) -> None:
        if self._closed:
            raise anyio.ClosedResourceError
        await self._ready.wait()
        if self._closed:
            raise anyio.ClosedResourceError

    async def watch_records(self) -> AsyncIterator[tuple[T, ...]]:
        """Yield current parsed records and subsequent current snapshots."""

        last_version = -1
        while True:
            with self._lock:
                if self._closed:
                    return
                current = (
                    self._ready.is_set() and self._current and self._beacon.is_current()
                )
                version = self._version
                if current and version != last_version:
                    records = self._records_locked()
                    last_version = version
                    changed: anyio.Event | None = None
                else:
                    records = None
                    changed = self._changed

            if records is not None:
                yield records
                continue

            assert changed is not None
            await changed.wait()

    def is_current(self) -> bool:
        with self._lock:
            return self._ready.is_set() and self._current and self._beacon.is_current()

    async def wait_current(self, *, timeout: float | None = None) -> None:
        async def wait_loop() -> None:
            await self.wait_ready()
            while True:
                with self._lock:
                    if self._closed:
                        raise KvUnavailable(
                            f"Beacon directory for feature {self.feature_id!r} is closed"
                        )
                    current = (
                        self._ready.is_set()
                        and self._current
                        and self._beacon.is_current()
                    )
                    changed = self._changed
                if current:
                    return
                if self._beacon.is_current():
                    await changed.wait()
                else:
                    await self._beacon.wait_current()

        if timeout is None:
            await wait_loop()
            return
        with anyio.fail_after(timeout):
            await wait_loop()

    def records(self) -> tuple[T, ...]:
        with self._lock:
            self._raise_if_stale_locked()
            return self._records_locked()

    def resolve(
        self,
        predicate: BeaconDirectoryPredicate[T] | None = None,
        *,
        select: BeaconDirectorySelector[T] | None = None,
    ) -> T | None:
        with self._lock:
            self._raise_if_stale_locked()
            return self._resolve_locked(predicate, select=select)

    async def wait_for(
        self,
        predicate: BeaconDirectoryPredicate[T] | None = None,
        *,
        select: BeaconDirectorySelector[T] | None = None,
        timeout: float | None = None,
    ) -> T:
        async def wait_loop() -> T:
            await self.wait_ready()
            while True:
                with self._lock:
                    if self._closed:
                        raise anyio.ClosedResourceError
                    current = (
                        self._ready.is_set()
                        and self._current
                        and self._beacon.is_current()
                    )
                    selected = (
                        self._resolve_locked(predicate, select=select)
                        if current
                        else None
                    )
                    changed = self._changed
                if selected is not None:
                    return selected
                if self._beacon.is_current():
                    await changed.wait()
                else:
                    await self._beacon.wait_current()

        if timeout is None:
            return await wait_loop()
        with anyio.fail_after(timeout):
            return await wait_loop()

    async def aclose(self) -> None:
        self._closed = True
        if self._cancel_scope is not None:
            self._cancel_scope.cancel()
        self._ready.set()
        self._notify_changed()

    async def _event_loop(self) -> None:
        with anyio.CancelScope() as cancel_scope:
            self._cancel_scope = cancel_scope
            try:
                async with self._beacon.watch(self.feature_id) as snapshots:
                    async for snapshot in snapshots:
                        self._apply_snapshot(
                            snapshot.candidates,
                            current=snapshot.current,
                        )
            except anyio.get_cancelled_exc_class():
                raise
            except KvUnavailable:
                pass
            except Exception:
                logger.warning(
                    "%s Beacon directory watch failed feature=%s",
                    self._log_label,
                    self.feature_id,
                    exc_info=True,
                )
            finally:
                self._mark_stale()

    def _apply_snapshot(
        self,
        candidates: tuple[Candidate, ...],
        *,
        current: bool,
    ) -> None:
        # Parser code is intentionally outside the directory state lock so one
        # slow or faulty feature parser cannot block readers or view ingestion.
        records_by_key = {
            candidate.key: records
            for candidate in candidates
            if (records := self._parse_candidate(candidate))
        }
        with self._lock:
            self._records_by_key = records_by_key
            self._current = current
            if current:
                self._ready.set()
            self._notify_changed_locked()

    def _parse_candidate(self, candidate: Candidate | None) -> tuple[T, ...]:
        if candidate is None:
            return ()
        try:
            parsed = self._parser(candidate)
        except Exception:
            logger.warning(
                "%s Beacon directory parser rejected feature=%s key=%s",
                self._log_label,
                self.feature_id,
                candidate.key,
                exc_info=True,
            )
            return ()
        if parsed is None:
            return ()
        if isinstance(parsed, list | tuple):
            return tuple(parsed)
        return (parsed,)

    def _mark_stale(self) -> None:
        with self._lock:
            self._current = False
            self._notify_changed_locked()

    def _raise_if_stale_locked(self) -> None:
        if not (self._ready.is_set() and self._current and self._beacon.is_current()):
            raise KvUnavailable(
                f"Beacon directory for feature {self.feature_id!r} is not current"
            )

    def _records_locked(self) -> tuple[T, ...]:
        return tuple(
            record for records in self._records_by_key.values() for record in records
        )

    def _resolve_locked(
        self,
        predicate: BeaconDirectoryPredicate[T] | None,
        *,
        select: BeaconDirectorySelector[T] | None,
    ) -> T | None:
        records = self._records_locked()
        if predicate is not None:
            records = tuple(record for record in records if predicate(record))
        if select is not None:
            return select(records)
        return records[0] if records else None

    def _notify_changed(self) -> None:
        with self._lock:
            self._notify_changed_locked()

    def _notify_changed_locked(self) -> None:
        self._version += 1
        changed = self._changed
        self._changed = anyio.Event()
        changed.set()
