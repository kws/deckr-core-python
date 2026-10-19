"""Hardware state subscriptions with atomic initial and final snapshots."""

from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from typing import Generic, TypeVar

import anyio

from deckr.core.util.state import CoalescedStateBroadcaster

T = TypeVar("T")


class HardwareStateChannel(Generic[T]):
    def __init__(self, read: Callable[[], T]) -> None:
        self._read = read
        self._bus = CoalescedStateBroadcaster[str]()
        self._closed = False

    @property
    def state(self) -> T:
        return replace(self._read(), version=self._bus.version)

    async def publish(self) -> None:
        if not self._closed:
            try:
                await self._bus.publish()
            except anyio.ClosedResourceError:
                if not self._closed:
                    raise

    @asynccontextmanager
    async def watch(self) -> AsyncIterator[AsyncIterator[T]]:
        async with AsyncExitStack() as stack:
            try:
                subscription = await stack.enter_async_context(
                    self._bus.subscribe(lambda _v, _c: self.state)
                )
            except anyio.ClosedResourceError:
                subscription = None

            async def stream():
                if subscription is not None:
                    yield subscription.initial
                    async for _change in subscription:
                        yield self.state
                if self._closed:
                    yield self.state

            yield stream()

    async def wait_for(
        self, predicate: Callable[[T], bool], *, timeout_seconds=None
    ) -> T:
        with anyio.fail_after(timeout_seconds):
            async with self.watch() as states:
                async for state in states:
                    if predicate(state):
                        return state
        raise anyio.ClosedResourceError

    async def aclose(self) -> None:
        if self._closed:
            return
        await self.publish()
        self._closed = True
        await self._bus.aclose()
