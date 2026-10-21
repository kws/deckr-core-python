import anyio
import pytest

from deckr.core.util.singleflight import AsyncSingleFlight, SharedOperationCancelled

pytestmark = pytest.mark.asyncio


async def test_cancelled_owner_wakes_waiters_and_next_call_can_retry():
    flight = AsyncSingleFlight()
    entered, waiter_done = anyio.Event(), anyio.Event()
    scope = anyio.CancelScope()

    async def lookup():
        entered.set()
        await anyio.sleep_forever()

    async def owner():
        with scope:
            await flight.run("item", lookup)

    async def waiter():
        with pytest.raises(SharedOperationCancelled):
            await flight.run("item", lookup)
        waiter_done.set()

    async with anyio.create_task_group() as tasks:
        tasks.start_soon(owner)
        await entered.wait()
        tasks.start_soon(waiter)
        await anyio.sleep(0)
        await anyio.sleep(0)
        scope.cancel()
        with anyio.fail_after(1):
            await waiter_done.wait()

    async def recovered():
        return "current"

    assert await flight.run("item", recovered) == "current"
