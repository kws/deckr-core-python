"""Scheduled queue."""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Generic

import anyio

from deckr.core.util.state import T


@dataclass(order=True)
class _Entry(Generic[T]):
    due: float
    seq: int
    item: T = field(compare=False)


class ScheduledQueue(Generic[T]):
    def __init__(self) -> None:
        self._cv = anyio.Condition()
        self._heap: list[_Entry[T]] = []
        self._seq: int = 0

    async def put_at(self, due: float, item: T) -> None:
        async with self._cv:
            self._seq += 1
            heapq.heappush(self._heap, _Entry(due=due, seq=self._seq, item=item))
            self._cv.notify()

    async def put_after(self, delay: float, item: T) -> None:
        await self.put_at(anyio.current_time() + delay, item)

    async def get(self) -> T:
        async with self._cv:
            while True:
                if not self._heap:
                    await self._cv.wait()
                    continue

                head = self._heap[0]
                now = anyio.current_time()

                if head.due <= now:
                    return heapq.heappop(self._heap).item

                timeout = head.due - now
                try:
                    with anyio.fail_after(timeout):
                        await self._cv.wait()
                except TimeoutError:
                    # due time reached; loop will pop next iteration
                    pass

    async def peek_due(self) -> float | None:
        async with self._cv:
            return self._heap[0].due if self._heap else None

    async def qsize(self) -> int:
        async with self._cv:
            return len(self._heap)
