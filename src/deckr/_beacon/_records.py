"""Records."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC
from typing import Any

from deckr._beacon._models import (
    AdvertisementFilter,
    AdvertisementRecord,
    BeaconAdvertisementSpec,
    BeaconEndpointSession,
    BeaconProtocol,
    Candidate,
    _AdvertisementHandle,
    parse_beacon_advertisement_key,
)
from deckr._beacon._support import _now_utc
from deckr.core.kv import KvEntry
from deckr.core.wire import freeze_json


def _is_materialized_bucket(value: Any) -> bool:
    return all(
        hasattr(value, name)
        for name in (
            "start",
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


async def _exact_get(bucket: Any, key: str) -> KvEntry | None:
    get_exact = getattr(bucket, "get_exact", None)
    return await (get_exact if get_exact is not None else bucket.get)(key)


def _filter_candidates(
    candidates: tuple[Candidate, ...],
    selector: AdvertisementFilter | None,
) -> tuple[Candidate, ...]:
    if selector is None:
        return candidates
    return tuple(
        candidate
        for candidate in candidates
        if _selector_accepts(selector, candidate.advertisement)
    )


def _record_from_spec(
    spec: BeaconAdvertisementSpec,
    *,
    endpoint: BeaconEndpointSession,
    advertisement_id: str,
    ttl_seconds: int,
    protocol: Mapping[str, str] | BeaconProtocol | None,
    operations: Sequence[str],
    labels: Mapping[str, str],
    hints: Mapping[str, Any],
    payload: Mapping[str, Any] | None,
) -> AdvertisementRecord:
    now = _now_utc()
    return AdvertisementRecord(
        advertisementId=advertisement_id,
        featureId=spec.feature_id,
        advertiser=endpoint.address,
        endpoint=endpoint.address,
        sessionId=endpoint.session_id,
        refreshSeq=1,
        ttlSeconds=ttl_seconds,
        protocol=protocol,
        operations=tuple(operations),
        labels=labels,
        hints=hints,
        payload=payload,
        createdAt=now,
        updatedAt=now,
    )


def _updated_record(
    record: AdvertisementRecord,
    *,
    protocol: Mapping[str, str] | BeaconProtocol | None,
    operations: Sequence[str],
    labels: Mapping[str, str],
    hints: Mapping[str, Any],
    payload: Mapping[str, Any] | None,
    ttl_seconds: int,
    force_refresh: bool,
) -> AdvertisementRecord:
    update = {
        "protocol": protocol,
        "operations": tuple(operations),
        "labels": freeze_json(labels),
        "hints": freeze_json(hints),
        "payload": freeze_json(payload) if payload is not None else None,
        "ttl_seconds": ttl_seconds,
    }
    changed = (
        record.protocol != update["protocol"]
        or record.operations != update["operations"]
        or record.labels != update["labels"]
        or record.hints != update["hints"]
        or record.payload != update["payload"]
        or record.ttl_seconds != ttl_seconds
    )
    if not changed and not force_refresh:
        return record
    return record.model_copy(
        update={
            **update,
            "refresh_seq": record.refresh_seq + 1,
            "updated_at": _now_utc(),
        }
    )


def _advertisement_handle(
    key: str,
    record: AdvertisementRecord,
    revision: int,
) -> _AdvertisementHandle:
    return _AdvertisementHandle(
        key=key,
        advertisement_id=record.advertisement_id,
        feature_id=record.feature_id,
        advertiser=record.advertiser,
        endpoint=record.endpoint,
        session_id=record.session_id,
        revision=revision,
        refresh_seq=record.refresh_seq,
    )


def _advertisement_matches_handle(
    record: AdvertisementRecord,
    handle: _AdvertisementHandle,
) -> bool:
    return (
        record.advertisement_id == handle.advertisement_id
        and record.feature_id == handle.feature_id
        and record.advertiser == handle.advertiser
        and record.endpoint == handle.endpoint
        and record.session_id == handle.session_id
    )


def _candidate_from_entry(entry: KvEntry) -> tuple[Candidate | None, str]:
    try:
        advertisement = AdvertisementRecord.from_wire(entry.value)
    except ValueError:
        return None, "invalid_schema"
    parsed = parse_beacon_advertisement_key(entry.key)
    if parsed is None:
        return None, "invalid_key"
    feature_id, advertisement_id = parsed
    if (
        feature_id != advertisement.feature_id
        or advertisement_id != advertisement.advertisement_id
    ):
        return None, "identity_mismatch"
    return (
        Candidate(
            key=entry.key,
            advertisement=advertisement,
            revision=entry.revision,
            observed_at=_now_utc(),
        ),
        "",
    )


def _candidate_newest_sort_key(candidate: Candidate) -> tuple[int, float, int, str]:
    updated_at = candidate.advertisement.updated_at
    updated_timestamp = (
        updated_at.astimezone(UTC).timestamp()
        if updated_at is not None
        else float("-inf")
    )
    return (
        -candidate.revision,
        -updated_timestamp,
        -candidate.advertisement.refresh_seq,
        candidate.advertisement.advertisement_id,
    )


def _selector_accepts(
    selector: AdvertisementFilter,
    advertisement: AdvertisementRecord,
) -> bool:
    accepts = getattr(selector, "accepts", None)
    if accepts is not None:
        return bool(accepts(advertisement))
    return bool(selector(advertisement))


def _feature_endpoint_key(record: AdvertisementRecord) -> tuple[str, str, str]:
    return (record.feature_id, str(record.advertiser), str(record.endpoint))
