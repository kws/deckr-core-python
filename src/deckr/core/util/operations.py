"""Carry a local operation guard across asynchronous and thread-capacity waits."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TypeVar

import anyio

T = TypeVar("T")
_guard: ContextVar[Callable[[], None] | None] = ContextVar(
    "operation_guard", default=None
)


@contextmanager
def operation_guard(check: Callable[[], None] | None) -> Iterator[None]:
    token = _guard.set(check)
    try:
        yield
    finally:
        _guard.reset(token)


def check_operation() -> None:
    check = _guard.get()
    if check is not None:
        check()


async def guarded_call(call: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
    check_operation()
    return await call(*args, **kwargs)


async def guarded_run_sync(
    call: Callable[..., T],
    *args,
    limiter: anyio.CapacityLimiter | None = None,
    **kwargs,
) -> T:
    """Check in the worker, after capacity is available, before external I/O.

    Cancellation abandons the result, not an external operation already started.
    Callers must not replay that operation on a replacement agreement.
    """

    def invoke() -> T:
        check_operation()
        return call(*args, **kwargs)

    return await anyio.to_thread.run_sync(
        invoke, abandon_on_cancel=True, limiter=limiter
    )
