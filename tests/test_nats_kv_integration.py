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
            js=js, policy=KvBucketPolicy(bucket="integration", ttl_seconds=60)
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
