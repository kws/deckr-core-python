"""Nats kv records."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping

from deckr.core.kv import (
    KvChange,
    KvEntry,
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
    value = json.loads(entry.value.decode("utf-8")) if entry.value else {}
    return KvEntry(
        bucket=bucket,
        key=str(entry.key),
        value=kv_value(value),
        revision=int(entry.revision),
    )


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
    if not prefix:
        return await kv_keys_unfiltered(kv, prefix)

    try:
        return await kv_keys_by_watch(kv, prefix)
    except (AttributeError, TypeError):
        return await kv_keys_unfiltered(kv, prefix)
    except Exception as exc:
        if is_key_missing(exc):
            return ()
        raise


async def kv_keys_by_watch(kv, prefix: str) -> tuple[str, ...]:
    watcher = await kv.watch(
        kv_watch_pattern(prefix),
        ignore_deletes=True,
        meta_only=True,
        inactive_threshold=5 * 60,
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
        try:
            await watcher.stop()
        finally:
            await delete_ephemeral_consumer(
                getattr(watcher, "_sub", None),
                reason=f"KV keys prefix {prefix!r}",
            )
    return tuple(keys)


async def kv_keys_unfiltered(kv, prefix: str) -> tuple[str, ...]:
    try:
        keys = await kv.keys()
    except Exception as exc:
        if is_key_missing(exc):
            return ()
        raise
    if keys is None:
        return ()
    return tuple(str(key) for key in keys if str(key).startswith(prefix))


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


async def delete_ephemeral_consumer(subscription, *, reason: str) -> None:
    stream = getattr(subscription, "_stream", None)
    consumer = getattr(subscription, "_consumer", None)
    js = getattr(subscription, "_js", None)
    jsm = getattr(js, "_jsm", None)
    if not stream or not consumer or jsm is None:
        return
    try:
        await jsm.delete_consumer(stream, consumer)
    except Exception:
        logger.debug(
            "Could not delete NATS ephemeral consumer stream=%s consumer=%s after %s",
            stream,
            consumer,
            reason,
            exc_info=True,
        )


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
