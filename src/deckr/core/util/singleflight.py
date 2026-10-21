"""Share one asynchronous lookup per key without retaining abandoned work."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Hashable
from dataclasses import dataclass
from typing import Generic, TypeVar, cast

import anyio

K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


class SharedOperationCancelled(Exception):
    """The owner of a shared lookup was cancelled; a later call can retry."""


@dataclass
class _Flight(Generic[V]):
    done: anyio.Event
    value: V | None = None
    error: BaseException | None = None


class AsyncSingleFlight(Generic[K, V]):
    """Share results, not side effects or request authority. Results are not cached."""

    def __init__(self) -> None:
        self._lock = anyio.Lock()
        self._pending: dict[K, _Flight[V]] = {}

    async def run(self, key: K, factory: Callable[[], Awaitable[V]]) -> V:
        async with self._lock:
            flight = self._pending.get(key)
            owner = flight is None
            if flight is None:
                flight = _Flight(anyio.Event())
                self._pending[key] = flight
        if not owner:
            await flight.done.wait()
            if flight.error is not None:
                raise flight.error
            return cast(V, flight.value)
        try:
            flight.value = await factory()
            return flight.value
        except anyio.get_cancelled_exc_class():
            flight.error = SharedOperationCancelled("Shared lookup owner was cancelled")
            raise
        except BaseException as exc:
            flight.error = exc
            raise
        finally:
            with anyio.CancelScope(shield=True):
                async with self._lock:
                    if self._pending.get(key) is flight:
                        del self._pending[key]
                    flight.done.set()
