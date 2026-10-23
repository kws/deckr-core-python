"""Beacon advertisement discovery and managed advertisement factories."""

from __future__ import annotations

import random
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any

import anyio

from deckr._beacon._lease import BeaconAdvertisementLease
from deckr._beacon._models import (
    AdvertisementFilter,
    AdvertisementRecord,
    BeaconAdvertisementSpec,
    BeaconEndpointSession,
    BeaconProtocol,
    BeaconWatchChange,
    BeaconWatchSnapshot,
    Candidate,
    CandidateStatus,
    _AdvertisementHandle,
    _BeaconAdvertisementMissing,
    _BeaconAdvertisementOwnerChanged,
    _require_text,
    beacon_advertisement_key,
)
from deckr._beacon._records import (
    _advertisement_handle,
    _advertisement_matches_handle,
    _candidate_from_entry,
    _candidate_newest_sort_key,
    _exact_get,
    _feature_endpoint_key,
    _filter_candidates,
    _is_materialized_bucket,
    _record_from_spec,
    _selector_accepts,
    _updated_record,
)
from deckr._beacon._support import _beacon_ttl_seconds
from deckr._beacon._view import BeaconView
from deckr.core.identity import EndpointAddress
from deckr.core.kv import KvConflict, KvEntry, KvUnavailable
from deckr.core.kv_source import KvMaterializedSource
from deckr.core.restart import VerifiedRestart


class Beacon:
    """Direct KV-backed Beacon runtime with a materialized advertisement view."""

    def __init__(
        self,
        bucket: KvMaterializedSource | Any,
    ) -> None:
        self._bucket = (
            bucket
            if _is_materialized_bucket(bucket)
            else KvMaterializedSource(bucket=bucket)
        )
        self._exact = self._bucket.exact_bucket
        self._view = BeaconView[Candidate](
            self._bucket,
            parse_entry=_candidate_from_entry,
            candidate_key=lambda candidate: candidate.key,
            candidate_feature=lambda candidate: candidate.advertisement.feature_id,
            candidate_endpoint_key=lambda candidate: _feature_endpoint_key(
                candidate.advertisement
            ),
            sort_key=_candidate_newest_sort_key,
        )
        self._started = False
        self._closed = False
        self._task_group: anyio.abc.TaskGroup | None = None
        self._leases: set[BeaconAdvertisementLease] = set()
        self._lock = anyio.Lock()

    @property
    def bucket(self) -> str:
        return self._bucket.bucket

    def start(self, task_group: anyio.abc.TaskGroup) -> None:
        self._task_group = task_group
        self._bucket.start(task_group)
        if not self._started:
            self._started = True
            task_group.start_soon(self._view.run)

    async def wait_ready(self) -> None:
        await self._view.wait_ready()

    def is_current(self) -> bool:
        return self._bucket.is_current() and self._view.is_current()

    async def wait_current(self) -> None:
        await self._view.wait_current()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        async with self._lock:
            leases = tuple(self._leases)
            self._leases.clear()
        with anyio.move_on_after(5, shield=True):
            async with anyio.create_task_group() as cleanup_group:
                for lease in leases:
                    cleanup_group.start_soon(lease.aclose)
        await self._view.aclose()

    def advertise(
        self,
        endpoint: BeaconEndpointSession,
        spec: BeaconAdvertisementSpec,
    ) -> BeaconAdvertisementLease:
        if self._closed:
            raise KvUnavailable("Beacon is closed")
        return BeaconAdvertisementLease(self, endpoint, spec)

    def candidates(
        self,
        feature_id: str,
        *,
        selector: AdvertisementFilter | None = None,
    ) -> tuple[Candidate, ...]:
        self._raise_if_cache_unavailable()
        feature_id = _require_text(feature_id, field_name="Beacon feature id")
        candidates = self._view.candidates(feature_id)
        if selector is None:
            return candidates
        return tuple(
            candidate
            for candidate in candidates
            if _selector_accepts(selector, candidate.advertisement)
        )

    def get(
        self,
        *,
        feature_id: str,
        advertisement_id: str,
    ) -> Candidate | None:
        self._raise_if_cache_unavailable()
        key = beacon_advertisement_key(
            feature_id=_require_text(feature_id, field_name="Beacon feature id"),
            advertisement_id=_require_text(
                advertisement_id,
                field_name="Beacon advertisement id",
            ),
        )
        return self._view.get(key)

    async def validate(
        self,
        candidate: Candidate,
        *,
        current_sessions: Mapping[str, str] | None = None,
    ) -> CandidateStatus:
        if self._started:
            if not self.is_current():
                return CandidateStatus.UNAVAILABLE
        current = self._view.get(candidate.key)
        if current is None:
            if self._view.is_invalid(candidate.key):
                return CandidateStatus.SCHEMA_INVALID
            return CandidateStatus.MISSING
        advertisement = current.advertisement
        if advertisement.feature_id != candidate.advertisement.feature_id:
            return CandidateStatus.FEATURE_MISMATCH
        if current.revision < candidate.revision:
            return CandidateStatus.MISSING
        if current_sessions is not None:
            current_session = current_sessions.get(str(advertisement.advertiser))
            if (
                current_session is not None
                and advertisement.session_id != current_session
            ):
                return CandidateStatus.SESSION_MISMATCH
        return CandidateStatus.CANDIDATE

    @asynccontextmanager
    async def watch(
        self,
        feature_id: str | None = None,
        *,
        selector: AdvertisementFilter | None = None,
    ) -> AsyncIterator[AsyncIterator[BeaconWatchSnapshot | BeaconWatchChange]]:
        if feature_id is not None:
            feature_id = _require_text(feature_id, field_name="Beacon feature id")
        if self._started:
            await self.wait_current()
        async with self._view.subscribe(feature_id) as subscription:

            async def stream() -> AsyncIterator[
                BeaconWatchSnapshot | BeaconWatchChange
            ]:
                initial_candidates = _filter_candidates(
                    subscription.initial.candidates,
                    selector,
                )
                known_keys = {candidate.key for candidate in initial_candidates}
                last_current = subscription.initial.current
                yield BeaconWatchSnapshot(
                    version=subscription.initial.version,
                    current=last_current,
                    candidates=initial_candidates,
                )
                async for wakeup in subscription:
                    try:
                        snapshot = await self._view.snapshot(feature_id)
                    except anyio.ClosedResourceError:
                        return
                    candidates = _filter_candidates(snapshot.candidates, selector)
                    current_keys = {candidate.key for candidate in candidates}
                    relevant = wakeup.changed & (known_keys | current_keys)
                    if (
                        not wakeup.resnapshot_required
                        and not relevant
                        and snapshot.current == last_current
                    ):
                        known_keys = current_keys
                        continue
                    known_keys = current_keys
                    last_current = snapshot.current
                    yield BeaconWatchChange(
                        version=snapshot.version,
                        current=snapshot.current,
                        candidates=candidates,
                        changed_keys=frozenset(relevant),
                        resnapshot_required=wakeup.resnapshot_required,
                    )

            yield stream()

    async def remove_stale_advertisements(
        self,
        endpoint: BeaconEndpointSession,
        spec: BeaconAdvertisementSpec,
    ) -> int:
        if self._started:
            await self.wait_current()
        async with self._lock:
            local_stale = tuple(
                lease
                for lease in self._leases
                if lease.feature_id == spec.feature_id
                and lease.endpoint == endpoint.address
                and lease.session_id == endpoint.session_id
            )
        for lease in local_stale:
            await lease.aclose()
        removed = 0
        for candidate in self._view.candidates_for_endpoint(
            (spec.feature_id, str(endpoint.address), str(endpoint.address))
        ):
            # Only this exact session can retire its own advertisements.
            # Different sessions may be live competing instances.
            if candidate.advertisement.session_id == endpoint.session_id:
                await self._delete_candidate(candidate)
                removed += 1
        return removed

    async def cleanup_restart(
        self, restart: VerifiedRestart, endpoint: EndpointAddress
    ) -> None:
        """Remove only exact discovery identities from a verified exited process."""
        await self.wait_current()
        for candidate in self._view.candidates():
            record = candidate.advertisement
            if (
                record.endpoint == endpoint
                and record.advertiser == endpoint
                and restart.contains(endpoint, record.session_id)
            ):
                await self._withdraw_advertisement_identity(
                    feature_id=record.feature_id,
                    advertisement_id=record.advertisement_id,
                    endpoint=endpoint,
                    session_id=record.session_id,
                )

    async def _event_loop(self) -> None:
        await self._view.run()

    def _raise_if_cache_unavailable(self) -> None:
        if self._started and not self.is_current():
            raise KvUnavailable("Beacon materialized view is not current")

    async def _create_advertisement(
        self,
        endpoint: BeaconEndpointSession,
        spec: BeaconAdvertisementSpec,
        *,
        advertisement_id: str,
        protocol: Mapping[str, str] | BeaconProtocol | None,
        operations: Sequence[str],
        labels: Mapping[str, str],
        hints: Mapping[str, Any],
        payload: Mapping[str, Any] | None,
    ) -> _AdvertisementHandle:
        ttl_seconds = await self._ttl_seconds()
        record = _record_from_spec(
            spec,
            endpoint=endpoint,
            advertisement_id=advertisement_id,
            ttl_seconds=ttl_seconds,
            protocol=protocol,
            operations=operations,
            labels=labels,
            hints=hints,
            payload=payload,
        )
        key = beacon_advertisement_key(
            feature_id=record.feature_id,
            advertisement_id=record.advertisement_id,
        )
        try:
            entry = await self._exact.create(key, record, ttl=ttl_seconds)
            await self._observe_write(key, entry.revision)
        except KvUnavailable:
            entry = await self._recover_advertisement_write(key, record)
        return _advertisement_handle(key, record, entry.revision)

    async def _refresh_advertisement(
        self,
        handle: _AdvertisementHandle,
        *,
        protocol: Mapping[str, str] | BeaconProtocol | None,
        operations: Sequence[str],
        labels: Mapping[str, str],
        hints: Mapping[str, Any],
        payload: Mapping[str, Any] | None,
        force_refresh: bool,
    ) -> _AdvertisementHandle:
        ttl_seconds = await self._ttl_seconds()
        current = self._view.get(handle.key)
        if current is None:
            exact = await _exact_get(self._exact, handle.key)
            if exact is None:
                raise _BeaconAdvertisementMissing(
                    f"Beacon advertisement {handle.key!r} is missing"
                )
            current, _reason = _candidate_from_entry(exact)
            if current is None:
                raise KvConflict(f"Beacon advertisement {handle.key!r} is invalid")
        record = current.advertisement
        if not _advertisement_matches_handle(record, handle):
            raise KvConflict(f"Beacon advertisement {handle.key!r} changed owner")
        refreshed = _updated_record(
            record,
            protocol=protocol,
            operations=operations,
            labels=labels,
            hints=hints,
            payload=payload,
            ttl_seconds=ttl_seconds,
            force_refresh=force_refresh,
        )
        if refreshed is record:
            return handle
        try:
            try:
                entry = await self._exact.update(
                    handle.key,
                    refreshed,
                    revision=current.revision,
                    ttl=ttl_seconds,
                )
                await self._observe_write(handle.key, entry.revision)
            except KvUnavailable:
                entry = await self._recover_advertisement_write(
                    handle.key,
                    refreshed,
                )
        except KvConflict as exc:
            exact = await _exact_get(self._exact, handle.key)
            if exact is None:
                raise _BeaconAdvertisementMissing(
                    f"Beacon advertisement {handle.key!r} is missing"
                ) from exc
            exact_candidate, _reason = _candidate_from_entry(exact)
            if exact_candidate is None:
                raise
            exact_record = exact_candidate.advertisement
            if not _advertisement_matches_handle(exact_record, handle):
                raise
            refreshed = _updated_record(
                exact_record,
                protocol=protocol,
                operations=operations,
                labels=labels,
                hints=hints,
                payload=payload,
                ttl_seconds=ttl_seconds,
                force_refresh=True,
            )
            try:
                entry = await self._exact.update(
                    handle.key,
                    refreshed,
                    revision=exact_candidate.revision,
                    ttl=ttl_seconds,
                )
                await self._observe_write(handle.key, entry.revision)
            except KvUnavailable:
                entry = await self._recover_advertisement_write(
                    handle.key,
                    refreshed,
                )
        return _advertisement_handle(handle.key, refreshed, entry.revision)

    async def _ttl_seconds(self) -> int:
        ttl_seconds = getattr(self._exact, "ttl_seconds", None)
        value = None
        if ttl_seconds is not None:
            value = ttl_seconds()
            if hasattr(value, "__await__"):
                value = await value
        return _beacon_ttl_seconds(value, bucket=self.bucket)

    async def _withdraw_advertisement(self, handle: _AdvertisementHandle) -> bool:
        current = await _exact_get(self._exact, handle.key)
        if current is None:
            return False
        candidate, _reason = _candidate_from_entry(current)
        if candidate is None:
            raise KvConflict(f"Beacon advertisement {handle.key!r} is invalid")
        if not _advertisement_matches_handle(candidate.advertisement, handle):
            raise KvConflict(f"Beacon advertisement {handle.key!r} changed owner")
        try:
            marker_revision = await self._exact.delete(
                handle.key,
                revision=current.revision,
            )
            if marker_revision is not None:
                await self._observe_write(handle.key, marker_revision)
        except KvUnavailable:
            recovered = await _exact_get(self._exact, handle.key)
            if recovered is not None:
                raise
        return True

    async def _withdraw_advertisement_identity(
        self,
        *,
        feature_id: str,
        advertisement_id: str,
        endpoint: EndpointAddress,
        session_id: str,
    ) -> bool:
        key = beacon_advertisement_key(
            feature_id=feature_id,
            advertisement_id=advertisement_id,
        )
        entry = await _exact_get(self._exact, key)
        if entry is None:
            return False
        candidate, _reason = _candidate_from_entry(entry)
        if candidate is None:
            raise KvConflict(f"Beacon advertisement {key!r} is invalid")
        record = candidate.advertisement
        if (
            record.feature_id != feature_id
            or record.advertisement_id != advertisement_id
            or record.advertiser != endpoint
            or record.endpoint != endpoint
            or record.session_id != session_id
        ):
            raise KvConflict(f"Beacon advertisement {key!r} changed owner")
        try:
            marker_revision = await self._exact.delete(
                key,
                revision=entry.revision,
            )
            if marker_revision is not None:
                await self._observe_write(key, marker_revision)
        except KvUnavailable:
            recovered = await _exact_get(self._exact, key)
            if recovered is not None:
                raise
        return True

    async def _recover_advertisement_write(
        self,
        key: str,
        expected: AdvertisementRecord,
    ) -> KvEntry:
        entry = await _exact_get(self._exact, key)
        if entry is None:
            raise KvUnavailable(
                f"Beacon advertisement write outcome for {key!r} is unresolved"
            )
        candidate, _reason = _candidate_from_entry(entry)
        if candidate is None or candidate.advertisement != expected:
            raise KvConflict(
                f"Beacon advertisement write outcome for {key!r} conflicts"
            )
        await self._observe_write(key, entry.revision)
        return entry

    async def _delete_candidate(self, candidate: Candidate) -> None:
        delay = 0.1
        last_revision: int | None = None
        while True:
            try:
                exact = await _exact_get(self._exact, candidate.key)
                if exact is None:
                    return
                current, _reason = _candidate_from_entry(exact)
                if current is None or not _advertisement_matches_handle(
                    current.advertisement,
                    _advertisement_handle(
                        candidate.key,
                        candidate.advertisement,
                        candidate.revision,
                    ),
                ):
                    raise _BeaconAdvertisementOwnerChanged(
                        f"Beacon advertisement {candidate.key!r} changed owner"
                    )
                if last_revision is not None and exact.revision != last_revision:
                    delay = 0.1
                last_revision = exact.revision
                marker_revision = await self._exact.delete(
                    candidate.key,
                    revision=exact.revision,
                )
                if marker_revision is not None:
                    await self._observe_write(candidate.key, marker_revision)
                return
            except KvUnavailable:
                recovered = await _exact_get(self._exact, candidate.key)
                if recovered is None:
                    return
            except _BeaconAdvertisementOwnerChanged:
                raise
            except KvConflict:
                pass
            await anyio.sleep(random.uniform(delay * 0.5, delay))
            delay = min(delay * 2, 5.0)

    async def _observe_write(self, key: str, revision: int) -> None:
        await self._bucket.wait_for_revision(key, revision)
        if self._started:
            await self._view.wait_for_revision(revision)

    async def _forget_lease(self, lease: BeaconAdvertisementLease) -> None:
        async with self._lock:
            self._leases.discard(lease)
