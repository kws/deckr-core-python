"""Nats kv bucket."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import anyio

from deckr.core.kv import (
    KvBucketPolicy,
    KvChange,
    KvConflict,
    KvEntry,
    KvUnavailable,
    KvWatchBarrier,
    kv_payload,
    kv_value,
)
from deckr.core.wire import WireModel
from deckr.substrates._nats_kv_policy import (
    _duration_nanoseconds,
    _duration_seconds_equal,
    _nats_stream_raw_config,
    _raw_duration_nanoseconds,
    _resolved_stream_ttl_seconds,
    _update_nats_stream_raw_config,
)
from deckr.substrates._nats_kv_records import (
    NATS_SUBJECT_DELETE_MARKER_TTL_FIELD,
    is_key_missing,
    is_revision_conflict,
    kv_absent_marker_revision,
    kv_entry_from_raw,
    kv_entry_is_absent_marker,
    kv_keys,
    logger,
)


class NatsJsonKvBucket:
    """Thin JSON adapter for one NATS KV bucket."""

    def __init__(
        self,
        *,
        js: Any,
        policy: KvBucketPolicy,
        buffer_size: int = 100,
    ) -> None:
        self._js = js
        self.policy = policy
        self._buffer_size = buffer_size
        self._kv = None
        self._ensure_lock = anyio.Lock()
        self._resolved_ttl_seconds: float | None = None

    @property
    def bucket(self) -> str:
        return self.policy.bucket

    async def ttl_seconds(self) -> float | None:
        await self._available_kv()
        return self._resolved_ttl_seconds

    async def get(self, key: str) -> KvEntry | None:
        kv = await self._available_kv()
        try:
            entry = await kv.get(key)
        except Exception as exc:
            if is_key_missing(exc):
                return None
            raise KvUnavailable(f"Could not get KV key {key!r}") from exc
        if kv_entry_is_absent_marker(entry):
            return None
        return kv_entry_from_raw(self.bucket, entry)

    async def items(self, prefix: str = "") -> tuple[KvEntry, ...]:
        kv = await self._available_kv()
        try:
            keys = await kv_keys(kv, prefix)
        except Exception as exc:
            if is_key_missing(exc):
                return ()
            raise KvUnavailable(f"Could not list KV prefix {prefix!r}") from exc
        entries: list[KvEntry] = []
        for key in sorted(str(item) for item in keys if str(item).startswith(prefix)):
            entry = await self.get(key)
            if entry is not None:
                entries.append(entry)
        return tuple(entries)

    async def put(
        self,
        key: str,
        value: Mapping[str, Any] | WireModel,
        *,
        ttl: float | None = None,
    ) -> KvEntry:
        self._validate_ttl(ttl)
        kv = await self._available_kv()
        normalized = kv_value(value)
        try:
            revision = await kv.put(key, kv_payload(normalized))
        except Exception as exc:
            raise KvUnavailable(f"Could not put KV key {key!r}") from exc
        return KvEntry(self.bucket, key, normalized, int(revision))

    async def create(
        self,
        key: str,
        value: Mapping[str, Any] | WireModel,
        *,
        ttl: float | None = None,
    ) -> KvEntry:
        self._validate_ttl(ttl)
        kv = await self._available_kv()
        normalized = kv_value(value)
        payload = kv_payload(normalized)
        try:
            revision = await kv.create(key, payload)
        except Exception as exc:
            if is_revision_conflict(exc):
                revision = await self._create_over_absent_marker(
                    kv,
                    key,
                    payload,
                    conflict=exc,
                )
            else:
                raise KvUnavailable(f"Could not create KV key {key!r}") from exc
        return KvEntry(self.bucket, key, normalized, int(revision))

    async def _create_over_absent_marker(
        self,
        kv,
        key: str,
        payload: bytes,
        *,
        conflict: BaseException,
    ) -> int:
        try:
            marker_revision = await kv_absent_marker_revision(kv, key)
        except Exception as exc:
            if is_key_missing(exc):
                raise KvConflict(f"KV key {key!r} already exists") from conflict
            raise KvUnavailable(f"Could not create KV key {key!r}") from exc
        if marker_revision is None:
            raise KvConflict(f"KV key {key!r} already exists") from conflict
        try:
            return int(await kv.update(key, payload, last=marker_revision))
        except Exception as exc:
            if is_revision_conflict(exc) or is_key_missing(exc):
                raise KvConflict(f"KV key {key!r} already exists") from exc
            raise KvUnavailable(f"Could not create KV key {key!r}") from exc

    async def update(
        self,
        key: str,
        value: Mapping[str, Any] | WireModel,
        *,
        revision: int,
        ttl: float | None = None,
    ) -> KvEntry:
        self._validate_ttl(ttl)
        kv = await self._available_kv()
        normalized = kv_value(value)
        try:
            new_revision = await kv.update(
                key,
                kv_payload(normalized),
                last=revision,
            )
        except Exception as exc:
            if is_revision_conflict(exc) or is_key_missing(exc):
                raise KvConflict(f"KV key {key!r} revision changed") from exc
            raise KvUnavailable(f"Could not update KV key {key!r}") from exc
        return KvEntry(self.bucket, key, normalized, int(new_revision))

    async def delete(self, key: str, *, revision: int | None = None) -> int | None:
        kv = await self._available_kv()
        try:
            if revision is None:
                await kv.delete(key)
            else:
                await kv.delete(key, last=revision)
        except Exception as exc:
            if is_key_missing(exc):
                return None
            if is_revision_conflict(exc):
                raise KvConflict(f"KV key {key!r} revision changed") from exc
            raise KvUnavailable(f"Could not delete KV key {key!r}") from exc
        try:
            return await kv_absent_marker_revision(kv, key)
        except Exception as exc:
            if is_key_missing(exc):
                return None
            logger.debug(
                "Could not confirm KV delete marker bucket=%s key=%s",
                self.bucket,
                key,
                exc_info=True,
            )
            return None

    @asynccontextmanager
    async def watch(
        self,
        prefix: str = "",
    ) -> AsyncIterator[anyio.abc.ObjectReceiveStream[KvChange | KvWatchBarrier]]:
        kv = await self._available_kv()
        try:
            high_water = await self._stream_high_water_revision(kv)
            from deckr.substrates._nats_watch import NatsKvWatch

            stream = NatsKvWatch(
                js=self._js,
                bucket=self.bucket,
                prefix=prefix,
                revision=high_water,
                buffer_size=self._buffer_size,
            )
            await stream.start()
        except Exception as exc:
            raise KvUnavailable(f"Could not watch KV prefix {prefix!r}") from exc
        try:
            yield stream
        finally:
            await stream.aclose()

    async def _stream_high_water_revision(self, kv: Any) -> int:
        stream_name = getattr(kv, "_stream", f"KV_{self.bucket}")
        info = await self._js.stream_info(stream_name)
        state = getattr(info, "state", None)
        value = getattr(state, "last_seq", None)
        if value is None:
            # Deterministic test stores expose their revision directly. Real
            # JetStream stream info always supplies ``state.last_seq``.
            value = getattr(kv, "_revision", 0)
        return int(value)

    async def _ensure_kv(self):
        if self._kv is not None:
            return self._kv
        async with self._ensure_lock:
            if self._kv is not None:
                return self._kv
            return await self._ensure_kv_locked()

    async def _ensure_kv_locked(self):
        newly_created = False
        try:
            kv = await self._js.key_value(self.bucket)
        except Exception:
            try:
                kv = await self._create_kv()
                newly_created = True
            except TypeError:
                try:
                    kv = await self._create_kv_with_params()
                    newly_created = True
                except Exception:
                    # Another process may have created the bucket between our
                    # lookup and create attempt. The winner owns its policy;
                    # treat the bucket as existing and validate it read-only.
                    kv = await self._js.key_value(self.bucket)
            except Exception:
                # Another process may have created the bucket between our
                # lookup and create attempt. The winner owns its policy;
                # treat the bucket as existing and validate it read-only.
                kv = await self._js.key_value(self.bucket)
        await self._ensure_kv_stream_config(kv, newly_created=newly_created)
        # Do not cache a handle until its policy has been validated. Otherwise
        # a failed first call would bypass validation on the next operation.
        self._kv = kv
        return kv

    async def _available_kv(self):
        try:
            return await self._ensure_kv()
        except KvUnavailable:
            raise
        except Exception as exc:
            raise KvUnavailable(
                f"NATS KV bucket {self.bucket!r} is unavailable"
            ) from exc

    async def _create_kv(self):
        from nats.js.api import DiscardPolicy, KeyValueConfig, StreamConfig

        if not self.policy.allow_write_ttl:
            # nats.py 2.14 creates every KeyValueConfig stream with
            # allow_msg_ttl=True. NATS does not allow that capability to be
            # disabled after creation, so JSON KV buckets without per-write TTL
            # must be created with their final policy atomically. This mirrors
            # nats.py's KV stream shape except for the deliberate TTL flag.
            await self._js.add_stream(
                StreamConfig(
                    name=f"KV_{self.bucket}",
                    subjects=[f"$KV.{self.bucket}.>"],
                    allow_direct=None,
                    allow_rollup_hdrs=True,
                    allow_msg_ttl=False,
                    deny_delete=True,
                    discard=DiscardPolicy.NEW,
                    duplicate_window=min(120.0, self.policy.ttl_seconds or 120.0),
                    max_age=self.policy.ttl_seconds,
                    max_bytes=None,
                    max_consumers=-1,
                    max_msg_size=None,
                    max_msgs=-1,
                    max_msgs_per_subject=1,
                    num_replicas=1,
                    storage=None,
                )
            )
            return await self._js.key_value(self.bucket)

        return await self._js.create_key_value(
            config=KeyValueConfig(
                bucket=self.bucket,
                history=1,
                ttl=self.policy.ttl_seconds,
            )
        )

    async def _create_kv_with_params(self):
        if not self.policy.allow_write_ttl:
            # The exact StreamConfig path above does not depend on the
            # KeyValueConfig call signature. Reaching this fallback means the
            # stream creation itself raised TypeError, which is not safe to
            # reinterpret as permission to create a non-canonical bucket.
            raise KvUnavailable(
                f"Could not create NATS KV bucket {self.bucket!r} "
                "with its required policy"
            )
        return await self._js.create_key_value(
            bucket=self.bucket,
            history=1,
            ttl=self.policy.ttl_seconds,
        )

    async def _ensure_kv_stream_config(self, kv, *, newly_created: bool) -> None:
        stream_name = getattr(kv, "_stream", f"KV_{self.bucket}")
        try:
            info = await self._js.stream_info(stream_name)
        except Exception as exc:
            raise RuntimeError(
                f"Could not inspect NATS KV bucket {self.bucket!r}; delete and "
                "recreate the development bucket if it was created by an older "
                "client build."
            ) from exc
        config = info.config
        expected_marker_ttl_ns = _duration_nanoseconds(self.policy.ttl_seconds)
        raw_config = None
        if expected_marker_ttl_ns is not None:
            try:
                raw_config = await _nats_stream_raw_config(self._js, stream_name)
            except Exception as exc:
                raise RuntimeError(
                    f"Could not inspect raw NATS KV stream config for {self.bucket!r}; "
                    "JSON KV requires subject delete markers on TTL-bound buckets."
                ) from exc

        max_age_matches = _duration_seconds_equal(
            getattr(config, "max_age", None),
            self.policy.ttl_seconds,
        )
        max_messages_matches = getattr(config, "max_msgs_per_subject", None) == 1
        observed_allow_write_ttl = (
            raw_config.get("allow_msg_ttl")
            if raw_config is not None
            else getattr(config, "allow_msg_ttl", None)
        )
        allow_write_ttl_matches = (
            observed_allow_write_ttl is True
        ) == self.policy.allow_write_ttl
        marker_ttl_matches = (
            expected_marker_ttl_ns is None
            or _raw_duration_nanoseconds(
                raw_config,
                NATS_SUBJECT_DELETE_MARKER_TTL_FIELD,
            )
            == expected_marker_ttl_ns
        )
        policy_matches = (
            max_age_matches
            and max_messages_matches
            and allow_write_ttl_matches
            and marker_ttl_matches
        )
        if policy_matches:
            self._resolved_ttl_seconds = _resolved_stream_ttl_seconds(
                config,
                raw_config=raw_config,
            )
            return

        if not newly_created:
            mismatches: list[str] = []
            if not max_age_matches:
                mismatches.append(
                    "max_age "
                    f"(expected {self.policy.ttl_seconds!r}s, "
                    f"found {getattr(config, 'max_age', None)!r})"
                )
            if not max_messages_matches:
                mismatches.append(
                    "max_msgs_per_subject "
                    f"(expected 1, "
                    f"found {getattr(config, 'max_msgs_per_subject', None)!r})"
                )
            if not allow_write_ttl_matches:
                mismatches.append(
                    "allow_msg_ttl "
                    f"(expected {self.policy.allow_write_ttl!r}, "
                    f"found {observed_allow_write_ttl!r})"
                )
            if not marker_ttl_matches:
                mismatches.append(
                    f"{NATS_SUBJECT_DELETE_MARKER_TTL_FIELD} "
                    f"(expected {expected_marker_ttl_ns!r}ns, "
                    "found "
                    f"{_raw_duration_nanoseconds(raw_config, NATS_SUBJECT_DELETE_MARKER_TTL_FIELD)!r})"
                )
            raise KvUnavailable(
                f"Existing NATS KV bucket {self.bucket!r} has an incompatible "
                f"configured KV policy: {', '.join(mismatches)}. The adapter will not "
                "rewrite shared bucket policy; delete and recreate the "
                f"development bucket/stream {stream_name} and restart."
            )

        # The KV creation API expresses max_age and history. If the broker did
        # not honor those fields, do not disguise the mismatch with a rewrite.
        # A proven-new bucket may only receive the fields that API cannot set.
        if not max_age_matches or not max_messages_matches:
            raise KvUnavailable(
                f"New NATS KV bucket {self.bucket!r} was created with an "
                "incompatible max_age or max_msgs_per_subject policy."
            )
        try:
            if raw_config is None:
                config.allow_msg_ttl = self.policy.allow_write_ttl
                await self._js.update_stream(config)
            else:
                updated_config = dict(raw_config)
                if expected_marker_ttl_ns is not None:
                    updated_config[NATS_SUBJECT_DELETE_MARKER_TTL_FIELD] = (
                        expected_marker_ttl_ns
                    )
                updated_config["allow_msg_ttl"] = self.policy.allow_write_ttl
                await _update_nats_stream_raw_config(
                    self._js,
                    stream_name,
                    updated_config,
                )
                raw_config = updated_config
        except Exception as exc:
            raise KvUnavailable(
                f"New NATS KV bucket {self.bucket!r} could not be configured for "
                "the configured KV policy. "
                f"Delete the development bucket/stream {stream_name} and restart."
            ) from exc
        self._resolved_ttl_seconds = _resolved_stream_ttl_seconds(
            config,
            raw_config=raw_config,
        )

    def _validate_ttl(self, ttl: float | None) -> None:
        if not self.policy.allow_write_ttl:
            if ttl is None:
                return
            raise ValueError(
                f"NATS {self.policy.description} does not use write TTL; "
                f"per-key TTL {ttl!r} is not supported."
            )
        if ttl is None:
            return
        bucket_ttl = self.policy.ttl_seconds
        if bucket_ttl is not None and abs(float(ttl) - bucket_ttl) <= 0.001:
            return
        raise ValueError(
            "NATS KV uses the broker-owned bucket TTL "
            f"({bucket_ttl:g}s); per-key TTL {ttl!r} is not supported."
        )
