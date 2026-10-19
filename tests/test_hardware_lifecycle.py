from contextlib import asynccontextmanager

import anyio
import pytest
from hardware_client_helpers import eventually, hardware_setup

from deckr.hardware import HardwarePhase, HardwareUnavailable

pytestmark = pytest.mark.asyncio


async def test_endpoint_claim_limit_includes_other_clients_pending_negotiations(
    monkeypatch,
):
    monkeypatch.setattr("deckr.hardware.client_runtime.MAX_ENDPOINT_CLAIMS", 2)
    async with (
        hardware_setup() as h,
        h.client.claim(h.spec) as winner,
        h.runtime.hardware(h.endpoint) as other,
    ):
        pending = other.claim(h.spec)

        async def negotiate():
            with pytest.raises(anyio.ClosedResourceError):
                await pending.__aenter__()

        async with anyio.create_task_group() as group:
            group.start_soon(negotiate)
            await eventually(lambda: len(other._router._admitted) == 2)
            with pytest.raises(HardwareUnavailable) as failure:
                async with h.client.claim(h.spec):
                    pass
            assert failure.value.code == "claim_limit"
            await pending.aclose()
        assert winner.state.local_authority
        assert len(other._router._admitted) == 1


async def test_client_limit_releases_capacity_on_exit(monkeypatch):
    monkeypatch.setattr("deckr.hardware.client_runtime.MAX_HARDWARE_CLIENTS", 2)
    async with hardware_setup() as h, h.runtime.hardware(h.endpoint) as other:
        with pytest.raises(HardwareUnavailable) as failure:
            async with h.runtime.hardware(h.endpoint):
                pass
        assert failure.value.code == "client_limit"
        await other.aclose()
        async with h.runtime.hardware(h.endpoint) as replacement:
            assert replacement.directory.state.current


@pytest.mark.parametrize("close_runtime", [False, True])
async def test_closing_client_cancels_subscription_startup(monkeypatch, close_runtime):
    async with (
        hardware_setup() as h,
        h.runtime.endpoint("controller:other") as endpoint,
    ):
        started = anyio.Event()
        finished = anyio.Event()

        @asynccontextmanager
        async def blocked_subscription(*_args):
            started.set()
            await anyio.sleep_forever()
            yield

        monkeypatch.setattr(endpoint, "subscribe", blocked_subscription)
        client = h.runtime.hardware(endpoint)

        async def enter():
            with pytest.raises(anyio.ClosedResourceError):
                await client.__aenter__()
            finished.set()

        async with anyio.create_task_group() as group:
            group.start_soon(enter)
            await started.wait()
            if close_runtime:
                await h.client._resources.aclose()
            else:
                await client.aclose()
            with anyio.fail_after(2):
                await finished.wait()
        assert client.closed
        assert len(h.client._resources._routers) == (0 if close_runtime else 1)


async def test_subscription_failure_revokes_claim_without_stopping_host():
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:
        await h.runtime._message_bus.subscriptions[-1]._send.aclose()
        state = await claim.wait_for_state(
            lambda s: not s.local_authority, timeout_seconds=2
        )
        assert state.failure.code == "hardware_subscription_closed"
        assert h.runtime.is_running


async def test_manager_subscription_failure_closes_manager_and_claim():
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:
        await h.runtime._message_bus.subscriptions[0]._send.aclose()
        state = await h.manager.wait_for_state(
            lambda s: s.phase == HardwarePhase.CLOSED, timeout_seconds=2
        )
        assert state.failure.code == "hardware_subscription_closed"
        await claim.wait_for_state(lambda s: not s.local_authority, timeout_seconds=2)
        assert h.runtime.is_running


async def test_cancelled_caller_closes_claim_and_allows_next_claim():
    async with hardware_setup() as h:
        with anyio.CancelScope() as scope:
            async with h.client.claim(h.spec) as claim:
                scope.cancel()
                await anyio.sleep_forever()
        assert claim.state.phase == HardwarePhase.CLOSED
        assert not claim.state.local_authority
        async with h.client.claim(h.spec) as successor:
            assert successor.state.local_authority
            assert successor.state.contract != claim.state.contract


async def test_client_close_owns_active_recovery_and_claim_contexts():
    async with hardware_setup() as h:
        use = await h.client.use(h.spec).__aenter__()
        claim = use.claim
        await h.client.aclose()
        assert use.state.phase == HardwarePhase.CLOSED
        assert claim.state.phase == HardwarePhase.CLOSED
        await use.aclose()


async def test_duplicate_entry_and_wrong_endpoint_fail_explicitly():
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:
        with pytest.raises(ValueError, match="controller"):
            h.runtime.hardware(h.manager_endpoint)
        with pytest.raises(RuntimeError, match="re-entered"):
            await h.client.__aenter__()
        with pytest.raises(RuntimeError, match="re-entered"):
            await claim.__aenter__()
        with pytest.raises(RuntimeError, match="restarted"):
            await h.manager.start(h.runtime._task_group)


async def test_closing_recovery_use_during_initial_negotiation_joins_workers():
    async with hardware_setup() as h, h.client.claim(h.spec):
        use = h.client.use(h.spec)
        finished = anyio.Event()

        async def enter():
            with pytest.raises(anyio.ClosedResourceError):
                await use.__aenter__()
            finished.set()

        async with anyio.create_task_group() as group:
            group.start_soon(enter)
            await eventually(lambda: use in h.client._uses)
            await use.aclose()
            with anyio.fail_after(2):
                await finished.wait()
        assert use.state.phase == HardwarePhase.CLOSED
        assert use._tasks._group is None
        assert len(h.client._claims) == 1
