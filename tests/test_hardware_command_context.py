"""Queued and threaded hardware work retains the exact managed authority."""

from unittest.mock import AsyncMock

import anyio
import pytest
from hardware_client_helpers import eventually, hardware_setup

from deckr.hardware import HardwareUnavailable

pytestmark = pytest.mark.asyncio


async def capture(h, claim):
    handler = h.manager.command_handler = AsyncMock(return_value=True)
    await h.manager.wait_for_state(lambda s: bool(s.claims), timeout_seconds=2)
    await claim.command("device-0", "raster.bitmap", "clear", control_id="key.0.0")
    await eventually(lambda: handler.await_count == 1)
    context = handler.await_args.args[0]
    context.authorize()
    return context


async def test_cancelled_context_stays_revoked_after_successor_claim():
    async with hardware_setup() as h:
        async with h.client.claim(h.spec) as claim:
            context = await capture(h, claim)
        await h.manager.wait_for_state(lambda s: not s.claims, timeout_seconds=2)
        async with h.client.claim(h.spec) as successor:
            fresh = await capture(h, successor)
            assert fresh.message.contract != context.message.contract
            with pytest.raises(HardwareUnavailable):
                context.authorize()
            fresh.authorize()


@pytest.mark.parametrize("change", ["remove", "replace", "stop"])
async def test_device_and_manager_lifetimes_revoke_retained_work(change):
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:
        context = await capture(h, claim)
        original = h.devices["device-0"]
        if change == "remove":
            await h.manager.remove_device("device-0")
            await h.manager.set_device(original)
        elif change == "replace":
            await h.manager.set_device(
                original.model_copy(update={"fingerprint": "new"})
            )
            await h.manager.set_device(original)
        else:
            await h.manager.stop()
        with pytest.raises(HardwareUnavailable):
            context.authorize()


async def test_presentation_edits_preserve_context():
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:
        context = await capture(h, claim)
        await h.manager.set_device(
            h.devices["device-0"].model_copy(update={"display_name": "Renamed"})
        )
        context.authorize()


async def test_thread_capacity_wait_rechecks_authority_before_call(monkeypatch):
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:
        context = await capture(h, claim)
        waiting, release = anyio.Event(), anyio.Event()
        run_sync = anyio.to_thread.run_sync
        writes = []

        async def delayed(function, *args, **kwargs):
            waiting.set()
            await release.wait()
            return await run_sync(function, *args, **kwargs)

        async def write():
            with pytest.raises(HardwareUnavailable):
                await context.run_sync(writes.append, "stale")

        monkeypatch.setattr(anyio.to_thread, "run_sync", delayed)
        async with anyio.create_task_group() as tasks:
            tasks.start_soon(write)
            with anyio.fail_after(2):
                await waiting.wait()
            await h.manager.remove_device("device-0")
            release.set()
        assert writes == []
