"""Pull streams that wake on capability revocation, including while idle."""

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Generic, TypeVar

import anyio

T = TypeVar("T")
AuthorityWatch = Callable[[], AbstractAsyncContextManager[AsyncIterator[object]]]
AuthorityCheck = Callable[[], Awaitable[object]]


@dataclass
class _Result(Generic[T]):
    value: T | None = None
    error: Exception | None = None


async def _next_authorized(
    changes: AsyncIterator[T], check: AuthorityCheck, watch: AuthorityWatch
) -> T:
    result: _Result[T] = _Result()
    done = anyio.Event()

    async def receive() -> None:
        try:
            result.value = await anext(changes)
        except Exception as exc:
            result.error = exc
        finally:
            done.set()

    async def monitor() -> None:
        try:
            async with watch() as states:
                async for _state in states:
                    await check()
            raise anyio.ClosedResourceError
        except Exception as exc:
            result.error = exc
            done.set()

    # Each pull owns and closes its task group before returning. No cancellation
    # scope crosses a generator yield into the caller's task or timeout scope.
    async with anyio.create_task_group() as group:
        group.start_soon(monitor)
        group.start_soon(receive)
        await done.wait()
        group.cancel_scope.cancel()
    if result.error is not None:
        raise result.error
    await check()
    return result.value  # type: ignore[return-value]


async def guarded_changes(
    changes: AsyncIterator[T], *, check: AuthorityCheck, watch: AuthorityWatch
) -> AsyncIterator[T]:
    """Check each delivery and terminate an idle receive on authority loss."""
    while True:
        try:
            value = await _next_authorized(changes, check, watch)
        except StopAsyncIteration:
            return
        yield value
