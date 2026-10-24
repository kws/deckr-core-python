import json
import socket
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace

import anyio
import pytest

from deckr.contracts.lanes import DEFAULT_MESSAGE_CONTRACT_REGISTRY
from deckr.runtime import Deckr
from deckr.supervision import SupervisorChannel
from deckr.testing.runtime import mock_message_bus


@pytest.mark.asyncio
async def test_endpoint_is_not_exposed_before_durable_session_acknowledgement():
    entered, release, usable = anyio.Event(), anyio.Event(), anyio.Event()
    recorded = []

    async def report(session):
        recorded.append((str(session.address), session.session_id))
        entered.set()
        await release.wait()

    async with Deckr(
        message_bus=mock_message_bus(DEFAULT_MESSAGE_CONTRACT_REGISTRY),
        session_reporter=report,
    ) as runtime:

        async def open_endpoint():
            async with runtime.endpoint("controller:supervised"):
                usable.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(open_endpoint)
            await entered.wait()
            assert not usable.is_set()
            assert recorded[0][0] == "controller:supervised"
            release.set()
            await usable.wait()
        assert runtime.endpoint_sessions == ()


@pytest.mark.asyncio
async def test_failed_session_registration_prevents_use_and_releases_context():
    async def report(session):
        raise OSError("supervisor storage unavailable")

    async with Deckr(
        message_bus=mock_message_bus(DEFAULT_MESSAGE_CONTRACT_REGISTRY),
        session_reporter=report,
    ) as runtime:
        with pytest.raises(OSError, match="storage unavailable"):
            async with runtime.endpoint("controller:supervised"):
                pytest.fail("unacknowledged session became usable")
        assert runtime.endpoint_sessions == ()


@pytest.mark.parametrize(
    "reply", [b'{"ok":true}\n', b'{"ok":false}\n', b"{}", b"x" * 4097]
)
@pytest.mark.asyncio
async def test_channel_requires_a_bounded_positive_acknowledgement(reply):
    with tempfile.TemporaryDirectory(dir="/tmp", prefix="deckr-sup-") as directory:
        path = str(Path(directory) / "socket")
        captured = []
        with socket.socket(socket.AF_UNIX) as server:
            server.bind(path)
            server.listen(1)

            def respond():
                connection, _ = server.accept()
                with connection, connection.makefile("rb") as stream:
                    captured.append(json.loads(stream.readline()))
                    connection.sendall(reply)

            worker = threading.Thread(target=respond)
            worker.start()
            channel = SupervisorChannel(path, "instance")
            call = channel.register(
                SimpleNamespace(address="controller:main", session_id="fresh")
            )
            if reply == b'{"ok":true}\n':
                await call
            else:
                with pytest.raises(RuntimeError):
                    await call
            worker.join(timeout=5)
            assert not worker.is_alive()
        assert captured == [
            dict(
                op="register",
                instanceId="instance",
                endpoint="controller:main",
                sessionId="fresh",
            )
        ]


def test_supervisor_configuration_fails_closed(monkeypatch):
    monkeypatch.delenv("DECKR_SUPERVISOR_SOCKET", raising=False)
    assert SupervisorChannel.from_env() is None
    monkeypatch.setenv("DECKR_SUPERVISOR_SOCKET", "/restart/sessions.sock")
    monkeypatch.delenv("DECKR_INSTANCE_ID", raising=False)
    with pytest.raises(ValueError):
        SupervisorChannel.from_env()
    monkeypatch.setenv("DECKR_INSTANCE_ID", "controller")
    monkeypatch.setenv("DECKR_RESTART_HANDOFF", "relative.json")
    with pytest.raises(ValueError):
        SupervisorChannel.from_env()
    monkeypatch.setenv("DECKR_RESTART_HANDOFF", "/restart/handoff.json")
    assert SupervisorChannel.from_env() == SupervisorChannel(
        "/restart/sessions.sock", "controller"
    )
