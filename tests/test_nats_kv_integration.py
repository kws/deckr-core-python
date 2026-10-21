"""Optional real-broker checks; enabled by the supervised-nats extra."""

import os
from contextlib import asynccontextmanager
from pathlib import Path

import anyio
import nats
import pytest

from deckr.core.kv import KvBucketPolicy, KvUnavailable, KvWatchBarrier
from deckr.substrates.nats_kv import NatsJsonKvBucket
from deckr.substrates.nats_server import NatsServerSupervisor


def _server_binary():
    explicit = os.environ.get("NATS_SERVER_BINARY")
    if explicit:
        return Path(explicit)
    package = pytest.importorskip(
        "deckr_nats_server_bin",
        reason="install deckr[supervised-nats] for real-broker tests",
    )
    return package.nats_server_path()


@asynccontextmanager
async def _broker(tmp_path):
    supervisor = NatsServerSupervisor(
        server_path=_server_binary(), port=-1, runtime_dir=tmp_path
    )
    handle = await supervisor.start()
    connection = None
    try:
        connection = await nats.connect(handle.url, token=handle.auth_token)
        yield connection.jetstream()
    finally:
        if connection is not None:
            await connection.close()
        await supervisor.stop()


@pytest.mark.asyncio
async def test_real_kv_watch_replay_idle_heartbeats_and_consumer_loss(tmp_path):
    async with _broker(tmp_path) as js:
        bucket = NatsJsonKvBucket(
            js=js,
            policy=KvBucketPolicy(
                bucket="integration", ttl_seconds=60, allow_write_ttl=True
            ),
        )
        created = await bucket.create("items.one", {"value": 1})
        async with bucket.watch("items.") as changes:
            with anyio.fail_after(5):
                replay = await anext(changes)
                assert replay.entry == created
                assert isinstance(await anext(changes), KvWatchBarrier)
                # A live but idle consumer must outlast its heartbeat deadline.
                with anyio.move_on_after(2):
                    await anext(changes)
                updated = await bucket.update(
                    "items.one", {"value": 2}, revision=created.revision
                )
                change = await anext(changes)
                assert change.entry == updated
                await js.delete_consumer("KV_integration", changes._consumer)
                with pytest.raises(KvUnavailable, match="heartbeat"):
                    await anext(changes)
        # A replacement watch explicitly replays a new snapshot.
        async with bucket.watch("items.") as changes:
            with anyio.fail_after(3):
                assert (await anext(changes)).entry == updated
                assert isinstance(await anext(changes), KvWatchBarrier)


@pytest.mark.asyncio
async def test_lists_delete_consumers_and_bad_json_does_not_break_shared_watch(
    tmp_path,
):
    async with _broker(tmp_path) as js:
        bucket = NatsJsonKvBucket(
            js=js,
            policy=KvBucketPolicy(
                bucket="integration", ttl_seconds=60, allow_write_ttl=True
            ),
        )
        created = await bucket.create("items.one", {"value": 1})
        for prefix in ("", "items."):
            for _ in range(5):
                assert await bucket.items(prefix) == (created,)
                assert (
                    await js.stream_info("KV_integration")
                ).state.consumer_count == 0
        await js.publish("$KV.integration.items.invalid", b"{")
        invalid = await bucket.get("items.invalid")
        assert invalid is not None and invalid.parse_error
        assert invalid.raw == b"{"
        async with bucket.watch("items.") as changes:
            with anyio.fail_after(3):
                seen = {}
                async for change in changes:
                    if isinstance(change, KvWatchBarrier):
                        break
                    seen[change.key] = change.entry
                assert seen["items.invalid"] == invalid
                assert seen["items.one"] == created
                updated = await bucket.update(
                    "items.one", {"value": 2}, revision=created.revision
                )
                assert (await anext(changes)).entry == updated
        assert (await js.stream_info("KV_integration")).state.consumer_count == 0


@pytest.mark.asyncio
async def test_large_replay_and_slow_reader_use_bounded_broker_backpressure(tmp_path):
    async with _broker(tmp_path) as js:
        bucket = NatsJsonKvBucket(
            js=js,
            policy=KvBucketPolicy(bucket="large_replay", ttl_seconds=None),
            buffer_size=8,
        )
        entries = [
            await bucket.put(f"items.{index}", {"value": index})
            for index in range(2048)
        ]
        async with bucket.watch("items.") as changes:
            # Leave the reader idle beyond several heartbeats. Replay must stop
            # at the configured window, without losing data or continuity.
            await anyio.sleep(2)
            info = await js.consumer_info("KV_large_replay", changes._consumer)
            assert info.num_ack_pending == 8
            assert info.delivered.consumer_seq == 8
            with anyio.fail_after(10):
                observed = []
                async for change in changes:
                    if isinstance(change, KvWatchBarrier):
                        assert change.revision == entries[-1].revision
                        break
                    observed.append(change.entry)
                assert observed == entries
                latest = await bucket.put("items.live", {"value": "live"})
                assert (await anext(changes)).entry == latest
        assert (await js.stream_info("KV_large_replay")).state.consumer_count == 0


@pytest.mark.asyncio
async def test_broker_policy_adjustment_is_rejected_instead_of_silently_accepted(
    tmp_path,
):
    async with _broker(tmp_path) as js:
        # NATS 2.14 enables message TTL when subject delete markers are enabled.
        # A strict adapter must inspect the CREATE result, not assume an UPDATE
        # to the requested policy was honored.
        bucket = NatsJsonKvBucket(
            js=js, policy=KvBucketPolicy(bucket="incompatible", ttl_seconds=60)
        )
        with pytest.raises(KvUnavailable, match="allow_msg_ttl"):
            await bucket.items()
