"""Owned Beacon advertisement renewal and cancellation-safe withdrawal."""

from __future__ import annotations

import random
import uuid
from collections.abc import Mapping, Sequence
from time import monotonic
from typing import TYPE_CHECKING, Any

import anyio

from deckr._beacon._models import (
    BeaconAdvertisementSpec,
    BeaconEndpointSession,
    _AdvertisementHandle,
    _BeaconAdvertisementMissing,
    _require_text,
)
from deckr._beacon._support import (
    _UNSET,
    _beacon_lifecycle_log_level,
    _beacon_refresh_interval,
    logger,
)
from deckr._beacon._tasks import _BeaconTaskGroup
from deckr.core.identity import EndpointAddress, parse_endpoint_address
from deckr.core.kv import KvConflict, KvUnavailable

if TYPE_CHECKING:
    from deckr._beacon._api import Beacon


class BeaconAdvertisementLease:
    """Core-owned Beacon advertisement lease with heartbeat refreshes."""

    def __init__(
        self,
        beacon: Beacon,
        endpoint: BeaconEndpointSession,
        spec: BeaconAdvertisementSpec,
    ) -> None:
        self._beacon = beacon
        self.spec = spec
        self.feature_id = spec.feature_id
        self.endpoint = parse_endpoint_address(endpoint.address)
        self.session_id = _require_text(
            endpoint.session_id,
            field_name="Beacon endpoint session id",
        )
        self._advertisement_id = str(uuid.uuid4())
        self._protocol = spec.protocol
        self._operations = tuple(spec.operations)
        self._labels = dict(spec.labels or {})
        self._hints = dict(spec.hints or {})
        self._payload = dict(spec.payload) if spec.payload is not None else None
        self._refresh_interval: float | None = None
        self._handle: _AdvertisementHandle | None = None
        self._last_refresh_at: float | None = None
        self._lock = anyio.Lock()
        self._tasks: _BeaconTaskGroup | None = None
        self._task_group: anyio.abc.TaskGroup | None = None
        self._entered = False
        self._closed = False
        self._close_started = False
        self._close_lock = anyio.Lock()
        self._close_done = anyio.Event()
        self._enter_cancel_scope: anyio.CancelScope | None = None
        self._enter_task_id: int | None = None
        self._enter_done = anyio.Event()

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def address(self) -> EndpointAddress:
        return self.endpoint

    async def __aenter__(self) -> BeaconAdvertisementLease:
        if self._entered:
            raise RuntimeError("Beacon advertisement contexts cannot be re-entered")
        self._entered = True
        with anyio.CancelScope() as scope:
            self._enter_cancel_scope = scope
            self._enter_task_id = anyio.get_current_task().id
            try:
                return await self._enter()
            except BaseException:
                if not self._close_started:
                    await self.aclose()
                raise
            finally:
                self._enter_cancel_scope = None
                self._enter_done.set()
        raise anyio.ClosedResourceError

    async def _enter(self) -> BeaconAdvertisementLease:
        if self._beacon._started:  # noqa: SLF001
            await self._beacon.wait_current()
        if self._closed:
            raise anyio.ClosedResourceError
        await self._beacon.remove_stale_advertisements(self, self.spec)
        if self._closed:
            raise anyio.ClosedResourceError
        handle = await self._publish_initial()
        if self._closed:
            raise anyio.ClosedResourceError
        async with self._beacon._lock:  # noqa: SLF001
            if self._beacon._closed:  # noqa: SLF001
                raise KvUnavailable("Beacon is closed")
            self._beacon._leases.add(self)  # noqa: SLF001
        self._tasks = _BeaconTaskGroup(self._beacon._task_group)  # noqa: SLF001
        self._task_group = await self._tasks.start()
        self._task_group.start_soon(self._heartbeat_loop)
        logger.log(
            _beacon_lifecycle_log_level(handle.feature_id),
            "Beacon advertisement announced feature=%s endpoint=%s "
            "session=%s advertisement=%s refresh=%s revision=%s",
            handle.feature_id,
            handle.endpoint,
            handle.session_id,
            handle.advertisement_id,
            handle.refresh_seq,
            handle.revision,
        )
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def update(
        self,
        *,
        payload: Mapping[str, Any] | None | object = _UNSET,
        labels: Mapping[str, str] | None = None,
        hints: Mapping[str, Any] | None = None,
        operations: Sequence[str] | None = None,
    ) -> None:
        async with self._lock:
            if payload is not _UNSET:
                if payload is not None and not isinstance(payload, Mapping):
                    raise TypeError("Beacon payload must be a mapping or None")
                self._payload = dict(payload) if payload is not None else None
            if labels is not None:
                self._labels = dict(labels)
            if hints is not None:
                self._hints = dict(hints)
            if operations is not None:
                self._operations = tuple(
                    _require_text(item, field_name="Beacon operation")
                    for item in operations
                )
            await self._publish_locked(force_refresh=False)

    async def aclose(self) -> None:
        with anyio.CancelScope(shield=True):
            async with self._close_lock:
                if self._close_started:
                    first = False
                else:
                    first = True
                    self._close_started = True
                    self._closed = True
                    enter_scope = self._enter_cancel_scope
                    if enter_scope is not None:
                        enter_scope.cancel()
                    wait_for_enter = (
                        enter_scope is not None
                        and self._enter_task_id != anyio.get_current_task().id
                    )
            if not first:
                await self._close_done.wait()
                return
            try:
                with anyio.move_on_after(5):
                    if wait_for_enter:
                        await self._enter_done.wait()
                    task_group = self._task_group
                    tasks = self._tasks
                    self._task_group = None
                    self._tasks = None
                    if task_group is not None:
                        task_group.cancel_scope.cancel()
                    if tasks is not None:
                        await tasks.aclose()
                    async with self._lock:
                        handle = self._handle
                        self._handle = None
                    try:
                        if handle is not None:
                            await self._beacon._withdraw_advertisement(handle)
                        else:
                            await self._beacon._withdraw_advertisement_identity(
                                feature_id=self.feature_id,
                                advertisement_id=self._advertisement_id,
                                endpoint=self.endpoint,
                                session_id=self.session_id,
                            )
                    except (KvConflict, KvUnavailable):
                        logger.debug(
                            "Could not withdraw Beacon advertisement feature=%s "
                            "endpoint=%s session=%s advertisement=%s",
                            self.feature_id,
                            self.endpoint,
                            self.session_id,
                            self._advertisement_id,
                            exc_info=True,
                        )
            finally:
                try:
                    await self._beacon._forget_lease(self)
                finally:
                    self._close_done.set()

    async def _publish_initial(self) -> _AdvertisementHandle:
        async with self._lock:
            return await self._publish_locked(force_refresh=True)

    async def _heartbeat_loop(self) -> None:
        retry_delay: float | None = None
        while not self._closed:
            delay = (
                await self._next_heartbeat_delay()
                if retry_delay is None
                else random.uniform(retry_delay * 0.5, retry_delay)
            )
            await anyio.sleep(delay)
            if self._closed:
                return
            try:
                async with self._lock:
                    await self._publish_locked(force_refresh=True)
            except (KvConflict, KvUnavailable):
                retry_delay = 0.1 if retry_delay is None else min(retry_delay * 2, 5.0)
                logger.warning(
                    "Beacon advertisement heartbeat failed feature=%s "
                    "endpoint=%s session=%s advertisement=%s",
                    self.feature_id,
                    self.endpoint,
                    self.session_id,
                    self._advertisement_id,
                    exc_info=True,
                )
            else:
                retry_delay = None

    async def _publish_locked(self, *, force_refresh: bool) -> _AdvertisementHandle:
        if self._closed:
            raise KvConflict("Beacon advertisement is closed")
        if self._handle is None:
            self._handle = await self._create_advertisement_from_current_state()
            self._last_refresh_at = monotonic()
            self._refresh_interval = await self._next_refresh_interval()
            return self._handle
        if force_refresh and not self._refresh_due():
            return self._handle
        old_revision = self._handle.revision
        old_handle = self._handle
        try:
            refreshed = await self._beacon._refresh_advertisement(
                old_handle,
                protocol=self._protocol,
                operations=self._operations,
                labels=self._labels,
                hints=self._hints,
                payload=self._payload,
                force_refresh=force_refresh,
            )
        except _BeaconAdvertisementMissing:
            logger.warning(
                "Beacon advertisement missing; recreating feature=%s "
                "endpoint=%s session=%s advertisement=%s key=%s",
                self.feature_id,
                self.endpoint,
                self.session_id,
                self._advertisement_id,
                old_handle.key,
                exc_info=True,
            )
            self._handle = await self._create_advertisement_from_current_state()
            self._last_refresh_at = monotonic()
            self._refresh_interval = await self._next_refresh_interval()
            return self._handle
        if refreshed.revision != old_revision:
            logger.debug(
                "Beacon advertisement heartbeat feature=%s endpoint=%s "
                "session=%s advertisement=%s refresh=%s revision=%s",
                refreshed.feature_id,
                refreshed.endpoint,
                refreshed.session_id,
                refreshed.advertisement_id,
                refreshed.refresh_seq,
                refreshed.revision,
            )
        self._handle = refreshed
        if refreshed.revision != old_revision:
            self._last_refresh_at = monotonic()
            self._refresh_interval = await self._next_refresh_interval()
        return refreshed

    def _refresh_due(self) -> bool:
        return (
            self._last_refresh_at is None
            or self._refresh_interval is None
            or monotonic() - self._last_refresh_at >= self._refresh_interval
        )

    async def _next_heartbeat_delay(self) -> float:
        if self._refresh_interval is None:
            self._refresh_interval = await self._next_refresh_interval()
        if self._last_refresh_at is None:
            return self._refresh_interval
        elapsed = monotonic() - self._last_refresh_at
        return max(0.0, self._refresh_interval - elapsed)

    async def _create_advertisement_from_current_state(self) -> _AdvertisementHandle:
        return await self._beacon._create_advertisement(
            self,
            self.spec,
            advertisement_id=self._advertisement_id,
            protocol=self._protocol,
            operations=self._operations,
            labels=self._labels,
            hints=self._hints,
            payload=self._payload,
        )

    async def _next_refresh_interval(self) -> float:
        return _beacon_refresh_interval(
            ttl_seconds=await self._beacon._ttl_seconds(),  # noqa: SLF001
        )
