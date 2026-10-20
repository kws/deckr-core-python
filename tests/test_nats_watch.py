"""Exercise the actual watch adapter, including control messages and idle loss."""

from types import SimpleNamespace

import anyio
import pytest

from deckr.core.kv import KvChange, KvUnavailable, KvWatchBarrier
from deckr.substrates._nats_watch import NatsKvWatch


class Connection:
    is_connected = True

    def __init__(self):
        self.stats = {"reconnects": 0}
        self.replies = []

    def new_inbox(self):
        return "_INBOX.test"

    async def subscribe(self, inbox, *, cb, **kwargs):
        self.callback = cb
        return SimpleNamespace(unsubscribe=self.unsubscribe)

    async def unsubscribe(self):
        pass

    async def publish(self, reply):
        self.replies.append(reply)


class JetStream:
    def __init__(self):
        self._nc = Connection()
        self.deleted = []

    async def add_consumer(self, stream, *, config):
        self.consumer = config.name
        return SimpleNamespace(num_pending=0, delivered=SimpleNamespace(consumer_seq=0))

    async def delete_consumer(self, stream, consumer):
        self.deleted.append((stream, consumer))


def heartbeat(sequence=0):
    return SimpleNamespace(
        headers={"Status": "100", "Nats-Last-Consumer": str(sequence)}, reply=""
    )


async def watch():
    js = JetStream()
    stream = NatsKvWatch(js=js, bucket="test", prefix="", revision=0, buffer_size=8)
    await stream.start()
    await stream.receive()  # initial empty snapshot barrier
    return js, stream


@pytest.mark.asyncio
async def test_idle_watch_heartbeats_keep_current_without_values(monkeypatch):
    js, stream = await watch()
    clock = [0.0]
    monkeypatch.setattr("deckr.substrates._nats_watch.monotonic", lambda: clock[0])
    stream._last_activity = 0
    try:
        for _ in range(5):
            clock[0] += 1
            await js._nc.callback(heartbeat())
            stream._check_health()
        assert stream._failure is None
    finally:
        await stream.aclose()


@pytest.mark.asyncio
async def test_missing_heartbeat_is_terminal_even_when_connection_stays_up(monkeypatch):
    js, stream = await watch()
    monkeypatch.setattr(
        "deckr.substrates._nats_watch.monotonic", lambda: stream._last_activity + 2
    )
    try:
        assert js._nc.is_connected
        with pytest.raises(KvUnavailable, match="heartbeat"):
            await stream.receive()
        await js._nc.callback(heartbeat())
        assert stream._failure is not None
    finally:
        await stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["gap", "reconnect", "disconnect", "consumer"])
async def test_watch_continuity_failure_requires_new_snapshot(failure):
    js, stream = await watch()
    try:
        if failure == "gap":
            await js._nc.callback(heartbeat(2))
        elif failure == "reconnect":
            js._nc.stats["reconnects"] += 1
        elif failure == "disconnect":
            js._nc.is_connected = False
        else:
            await js._nc.callback(
                SimpleNamespace(
                    headers={}, metadata=SimpleNamespace(consumer="replacement")
                )
            )
        with pytest.raises(KvUnavailable):
            await stream.receive()
    finally:
        await stream.aclose()
    assert len(js.deleted) == 1


@pytest.mark.asyncio
async def test_flow_control_is_acknowledged():
    js, stream = await watch()
    try:
        await js._nc.callback(SimpleNamespace(headers={"Status": "100"}, reply="flow"))
        assert js._nc.replies == ["flow"]
    finally:
        await stream.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "item", [KvWatchBarrier(1), KvChange("test", "key", 1, "delete")]
)
async def test_poll_deadline_does_not_discard_an_already_received_event(
    monkeypatch, item
):
    _js, stream = await watch()
    move_on_after = anyio.move_on_after
    receive = stream._receive.receive
    scopes = []
    received = False

    def poll_scope(seconds):
        scope = move_on_after(seconds)
        scopes.append(scope)
        return scope

    async def receive_at_deadline():
        nonlocal received
        assert not received, "An already consumed broker event was discarded"
        value = await receive()
        received = True
        # The polling deadline can fire after the receive completes, without
        # cancelling that receive. The delivered event must still be returned.
        scopes[-1].cancel()
        return value

    try:
        await stream._send.send(item)
        with monkeypatch.context() as patch:
            patch.setattr(anyio, "move_on_after", poll_scope)
            patch.setattr(stream._receive, "receive", receive_at_deadline)
            assert await stream.receive() == item
    finally:
        await stream.aclose()
