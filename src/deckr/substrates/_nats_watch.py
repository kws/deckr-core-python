"""JetStream KV watches with explicit liveness and consumer continuity.

Use a plain NATS subscription so client-side ordered-consumer recovery cannot
silently restore authority. The materialized-source owner opens a new snapshot
only after observing this watch's failure.
"""

from __future__ import annotations

import uuid
from time import monotonic
from typing import Any

import anyio

from deckr.core.kv import KvChange, KvUnavailable, KvWatchBarrier
from deckr.substrates._nats_kv_records import kv_entry_from_bytes

HEARTBEAT_INTERVAL_SECONDS = 0.5
HEARTBEAT_DEADLINE_SECONDS = 1.5
HEALTH_POLL_SECONDS = 0.1
CLEANUP_TIMEOUT_SECONDS = 0.5


class NatsKvWatch(anyio.abc.ObjectReceiveStream[KvChange | KvWatchBarrier]):
    """One ephemeral consumer; failures are terminal for this watch."""

    def __init__(
        self, *, js: Any, bucket: str, prefix: str, revision: int, buffer_size: int
    ) -> None:
        self._js = js
        self._connection = js._nc
        self._bucket = bucket
        self._prefix = prefix
        self._stream = f"KV_{bucket}"
        self._consumer = f"kv_watch_{uuid.uuid4().hex}"
        self._subject_prefix = f"$KV.{bucket}."
        self._barrier = KvWatchBarrier(revision)
        self._send, self._receive = anyio.create_memory_object_stream[
            KvChange | KvWatchBarrier
        ](buffer_size)
        self._subscription: Any = None
        self._closed = False
        self._failure: KvUnavailable | None = None
        self._last_activity = monotonic()
        self._consumer_sequence = 0
        self._stream_sequence = 0
        self._initial_complete = False
        self._reconnects = self._connection.stats.get("reconnects", 0)

    async def start(self) -> None:
        from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

        inbox = self._connection.new_inbox()
        self._subscription = await self._connection.subscribe(
            inbox, cb=self._on_message, pending_msgs_limit=1024
        )
        pattern = self._prefix + ">" if self._prefix else ">"
        config = ConsumerConfig(
            name=self._consumer,
            deliver_subject=inbox,
            filter_subject=self._subject_prefix + pattern,
            deliver_policy=DeliverPolicy.LAST_PER_SUBJECT,
            ack_policy=AckPolicy.NONE,
            idle_heartbeat=HEARTBEAT_INTERVAL_SECONDS,
            flow_control=True,
            inactive_threshold=5.0,
        )
        try:
            info = await self._js.add_consumer(self._stream, config=config)
            self._last_activity = monotonic()
            if info.num_pending == 0 and info.delivered.consumer_seq == 0:
                self._finish_snapshot()
        except BaseException:
            await self.aclose()
            raise

    def _enqueue(self, value: KvChange | KvWatchBarrier) -> None:
        try:
            self._send.send_nowait(value)
        except anyio.WouldBlock:
            self._failure = KvUnavailable("NATS KV watch buffer overflow")
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            pass

    def _finish_snapshot(self) -> None:
        if not self._initial_complete:
            self._initial_complete = True
            self._enqueue(self._barrier)

    async def _on_message(self, message: Any) -> None:
        if self._closed or self._failure is not None:
            return
        try:
            self._check_health()
            headers = message.headers or {}
            status = headers.get("Status")
            if status is not None:
                if status != "100":
                    raise KvUnavailable(f"NATS KV watch status {status}")
                last_consumer = headers.get("Nats-Last-Consumer")
                if (
                    last_consumer is not None
                    and int(last_consumer) != self._consumer_sequence
                ):
                    raise KvUnavailable("NATS KV watch consumer sequence gap")
                self._last_activity = monotonic()
                if message.reply:
                    await self._connection.publish(message.reply)
                return
            metadata = message.metadata
            if metadata.consumer != self._consumer:
                raise KvUnavailable("NATS KV watch consumer changed")
            if metadata.sequence.consumer != self._consumer_sequence + 1:
                raise KvUnavailable("NATS KV watch consumer sequence gap")
            if metadata.sequence.stream <= self._stream_sequence:
                raise KvUnavailable("NATS KV watch stream sequence regressed")
            if not message.subject.startswith(self._subject_prefix):
                raise KvUnavailable("NATS KV watch subject mismatch")
            self._consumer_sequence = metadata.sequence.consumer
            self._stream_sequence = metadata.sequence.stream
            self._last_activity = monotonic()
            key = message.subject[len(self._subject_prefix) :]
            operation = headers.get("KV-Operation", "")
            marker = headers.get("Nats-Marker-Reason")
            if operation in {"DEL", "PURGE"} or not message.data:
                change = KvChange(
                    self._bucket,
                    key,
                    metadata.sequence.stream,
                    "expire" if marker == "MaxAge" else "delete",
                    marker_reason=marker or operation or "absent",
                )
            else:
                entry = kv_entry_from_bytes(
                    self._bucket,
                    key,
                    metadata.sequence.stream,
                    message.data,
                )
                change = KvChange(self._bucket, key, entry.revision, "put", entry)
            self._enqueue(change)
            if metadata.num_pending == 0:
                self._finish_snapshot()
        except Exception as exc:
            self._failure = KvUnavailable(f"NATS KV watch lost continuity: {exc}")

    def _check_health(self) -> None:
        if not self._connection.is_connected:
            raise KvUnavailable("NATS KV watch connection lost")
        if self._connection.stats.get("reconnects", 0) != self._reconnects:
            raise KvUnavailable("NATS KV watch connection recovered")
        if monotonic() - self._last_activity >= HEARTBEAT_DEADLINE_SECONDS:
            raise KvUnavailable("NATS KV watch heartbeat deadline missed")

    async def receive(self) -> KvChange | KvWatchBarrier:
        while True:
            if self._closed:
                raise anyio.ClosedResourceError
            if self._failure is not None:
                raise self._failure
            try:
                self._check_health()
            except KvUnavailable as exc:
                self._failure = exc
                raise
            with anyio.move_on_after(HEALTH_POLL_SECONDS) as scope:
                value = await self._receive.receive()
            # A deadline may fire after receive has already delivered a value.
            # Discard only a cancelled receive, never an observed broker event.
            if not scope.cancelled_caught:
                if self._failure is not None:
                    raise self._failure
                try:
                    self._check_health()
                except KvUnavailable as exc:
                    self._failure = exc
                    raise
                return value

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._send.aclose()
        await self._receive.aclose()
        with anyio.move_on_after(CLEANUP_TIMEOUT_SECONDS, shield=True):
            if self._subscription is not None:
                await self._subscription.unsubscribe()
        with anyio.move_on_after(CLEANUP_TIMEOUT_SECONDS, shield=True):
            try:
                await self._js.delete_consumer(self._stream, self._consumer)
            except Exception:
                # Consumer inactivity is the final cleanup bound after disconnect.
                pass
