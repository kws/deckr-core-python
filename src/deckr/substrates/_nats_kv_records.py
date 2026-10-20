"""Nats kv records."""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Mapping

import anyio

from deckr.core.kv import (
    KvChange,
    KvEntry,
    KvUnavailable,
    kv_value,
)

logger = logging.getLogger(__name__)


KV_OPERATION_HEADER = "KV-Operation"


KV_DELETE_OPERATION = "DEL"


KV_PURGE_OPERATION = "PURGE"


NATS_MARKER_REASON_HEADER = "Nats-Marker-Reason"


NATS_MARKER_MAX_AGE = "MaxAge"


NATS_NANOSECONDS_PER_SECOND = 1_000_000_000


NATS_SUBJECT_DELETE_MARKER_TTL_FIELD = "subject_delete_marker_ttl"


NATS_KV_POLICY_VECTOR_FIELDS = (
    "max_age",
    "max_msgs_per_subject",
    "allow_msg_ttl",
    NATS_SUBJECT_DELETE_MARKER_TTL_FIELD,
)


NATS_KV_NON_AUTHORITATIVE_CONFIG_FIELDS = frozenset(
    {"allow_direct", "metadata", "storage"}
)


NATS_WATCH_CLEANUP_TIMEOUT_SECONDS = 0.5


NATS_WATCH_HEALTH_INTERVAL_SECONDS = 0.1


NATS_WATCH_INACTIVE_THRESHOLD_SECONDS = 1.0


def kv_entry_from_raw(bucket: str, entry) -> KvEntry:
    return kv_entry_from_bytes(bucket, str(entry.key), int(entry.revision), entry.value)


def kv_entry_from_bytes(bucket: str, key: str, revision: int, raw: bytes) -> KvEntry:
    """Bad record bytes are an invalid observation, never a continuity failure."""

    def object_pairs(pairs):
        result = {}
        for name, value in pairs:
            if name in result:
                raise ValueError("Duplicate JSON object key")
            result[name] = value
        return result

    def scalars(value):
        if isinstance(value, str):
            value.encode("utf-8")
        elif isinstance(value, float) and not math.isfinite(value):
            raise ValueError("Non-finite JSON number")
        elif isinstance(value, list):
            for item in value:
                scalars(item)
        elif isinstance(value, dict):
            for name, item in value.items():
                scalars(name)
                scalars(item)

    try:
        value = json.loads(raw, object_pairs_hook=object_pairs)
        scalars(value)
        if not isinstance(value, dict):
            raise ValueError("KV value must be a JSON object")
        return KvEntry(bucket, key, kv_value(value), revision)
    except (ValueError, TypeError, UnicodeError) as exc:
        return KvEntry(bucket, key, kv_value({}), revision, str(exc), raw)


def kv_change_from_raw(bucket: str, entry) -> KvChange | None:
    key = str(entry.key)
    revision = int(getattr(entry, "revision", 0))
    operation = str(getattr(entry, "operation", "") or "").upper()
    marker_reason = _raw_header(entry, NATS_MARKER_REASON_HEADER)
    if operation in {KV_DELETE_OPERATION, KV_PURGE_OPERATION}:
        return KvChange(
            bucket=bucket,
            key=key,
            revision=revision,
            operation=("expire" if marker_reason == NATS_MARKER_MAX_AGE else "delete"),
            marker_reason=marker_reason or operation,
        )
    if kv_entry_is_absent_marker(entry):
        return KvChange(
            bucket=bucket,
            key=key,
            revision=revision,
            operation=("expire" if marker_reason == NATS_MARKER_MAX_AGE else "delete"),
            marker_reason=marker_reason or operation or "absent",
        )
    return KvChange(
        bucket=bucket,
        key=key,
        revision=revision,
        operation="put",
        entry=kv_entry_from_raw(bucket, entry),
    )


def kv_entry_is_absent_marker(entry) -> bool:
    if getattr(entry, "operation", None) in {
        KV_DELETE_OPERATION,
        KV_PURGE_OPERATION,
    }:
        return True
    return getattr(entry, "value", None) in {None, b""}


async def kv_absent_marker_revision(kv, key: str) -> int | None:
    get_raw = getattr(kv, "_get", None)
    if get_raw is None:
        get_raw = kv.get
    try:
        entry = await get_raw(key)
    except Exception as exc:
        entry = getattr(exc, "entry", None)
        if entry is not None and kv_entry_is_absent_marker(entry):
            return int(entry.revision)
        raise
    if kv_entry_is_absent_marker(entry):
        return int(entry.revision)
    return None


def kv_watch_pattern(prefix: str) -> str:
    if not prefix:
        return ">"
    if prefix.endswith("."):
        return f"{prefix}>"
    return prefix


async def kv_keys(kv, prefix: str) -> tuple[str, ...]:
    try:
        return await kv_keys_by_watch(kv, prefix)
    except Exception as exc:
        if is_key_missing(exc):
            return ()
        raise


async def kv_keys_by_watch(kv, prefix: str) -> tuple[str, ...]:
    watcher = await kv.watch(
        kv_watch_pattern(prefix),
        ignore_deletes=True,
        meta_only=True,
        inactive_threshold=NATS_WATCH_INACTIVE_THRESHOLD_SECONDS,
    )
    keys: list[str] = []
    try:
        async for entry in watcher:
            if entry is None:
                break
            key = str(entry.key)
            if key.startswith(prefix):
                keys.append(key)
    finally:
        await _close_snapshot_watcher(watcher, reason=f"KV keys prefix {prefix!r}")
    return tuple(keys)


async def kv_keys_unfiltered(kv, prefix: str) -> tuple[str, ...]:
    return await kv_keys_by_watch(kv, prefix)


async def kv_snapshot_entries(kv, bucket: str, prefix: str) -> tuple[KvEntry, ...]:
    watcher = await kv.watch(
        kv_watch_pattern(prefix),
        ignore_deletes=True,
        inactive_threshold=NATS_WATCH_INACTIVE_THRESHOLD_SECONDS,
    )
    entries = {}
    try:
        async for raw in watcher:
            if raw is None:
                break
            if str(raw.key).startswith(prefix) and not kv_entry_is_absent_marker(raw):
                entry = kv_entry_from_raw(bucket, raw)
                entries[entry.key] = entry
    finally:
        await _close_snapshot_watcher(watcher, reason="KV snapshot")
    return tuple(entries[key] for key in sorted(entries))


async def _close_snapshot_watcher(watcher, *, reason: str) -> None:
    # Cancellation of the list must not cancel its owned consumer cleanup.
    # Give unsubscribe and deletion independent bounded attempts.
    try:
        with anyio.move_on_after(NATS_WATCH_CLEANUP_TIMEOUT_SECONDS, shield=True):
            await watcher.stop()
    finally:
        with anyio.fail_after(NATS_WATCH_CLEANUP_TIMEOUT_SECONDS, shield=True):
            deleted = await delete_ephemeral_consumer(
                getattr(watcher, "_sub", None), reason=reason
            )
            if not deleted:
                raise KvUnavailable("Owned NATS snapshot consumer cleanup is pending")


def _raw_header(entry, key: str) -> str | None:
    headers = getattr(entry, "headers", None)
    if headers is None:
        return None
    try:
        value = headers.get(key)
    except AttributeError:
        value = None
    if value is None and isinstance(headers, Mapping):
        value = headers.get(key)
    if isinstance(value, list):
        value = value[0] if value else None
    return str(value) if value is not None else None


async def delete_ephemeral_consumer(subscription, *, reason: str) -> bool:
    stream = getattr(subscription, "_stream", None)
    consumer = getattr(subscription, "_consumer", None)
    js = getattr(subscription, "_js", None)
    jsm = getattr(js, "_jsm", None)
    if not stream or not consumer or jsm is None:
        return False
    try:
        await jsm.delete_consumer(stream, consumer)
        return True
    except Exception as exc:
        if is_key_missing(exc):
            return True
        logger.debug(
            "Could not delete NATS ephemeral consumer stream=%s consumer=%s after %s",
            stream,
            consumer,
            reason,
            exc_info=True,
        )
        return False


def exception_names(exc: BaseException) -> set[str]:
    names: set[str] = set()
    current: BaseException | None = exc
    while current is not None:
        names.add(type(current).__name__)
        current = current.__cause__
    return names


def is_key_missing(exc: BaseException) -> bool:
    names = exception_names(exc)
    if names & {"KeyNotFoundError", "KeyDeletedError", "NoKeysError", "NotFoundError"}:
        return True
    message = str(exc).lower()
    return message in {"missing", "not found", "key not found"}


def is_revision_conflict(exc: BaseException) -> bool:
    names = exception_names(exc)
    if "KeyWrongLastSequenceError" in names:
        return True
    if getattr(exc, "err_code", None) == 10071:
        return True
    message = str(exc).lower()
    return (
        message in {"exists", "revision changed"}
        or "wrong last" in message
        or "wrong sequence" in message
        or "revision changed" in message
    )
