"""Trigger."""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import anyio


class CoalescedTrigger:
    """Coalesce bursty async notifications into one delayed handler call."""

    def __init__(self, *, batch_interval: float) -> None:
        if batch_interval <= 0:
            raise ValueError("batch_interval must be greater than zero")
        self._batch_interval = batch_interval
        self._condition = anyio.Condition()
        self._first_reason: str | None = None
        self._reason_count = 0
        self._closed = False

    async def request(self, reason: str) -> None:
        async with self._condition:
            if self._closed:
                return
            if self._reason_count == 0:
                self._first_reason = reason
            self._reason_count += 1
            self._condition.notify()

    async def aclose(self) -> None:
        async with self._condition:
            self._closed = True
            self._condition.notify_all()

    async def run(
        self,
        handler: Callable[[str], Awaitable[None]],
        *,
        reason_prefix: str = "coalesced notifications",
    ) -> None:
        next_run_at = 0.0
        while True:
            async with self._condition:
                while self._reason_count == 0 and not self._closed:
                    await self._condition.wait()
                if self._closed:
                    return

                now = anyio.current_time()
                delay = max(0.0, next_run_at - now)

            if delay:
                await anyio.sleep(delay)

            async with self._condition:
                if self._closed:
                    return
                first_reason = self._first_reason
                reason_count = self._reason_count
                self._first_reason = None
                self._reason_count = 0

            next_run_at = anyio.current_time() + self._batch_interval
            await handler(
                _coalesced_reason(
                    reason_prefix,
                    first_reason=first_reason,
                    reason_count=reason_count,
                )
            )


def _coalesced_reason(
    prefix: str,
    *,
    first_reason: str | None,
    reason_count: int,
) -> str:
    if first_reason is None or reason_count <= 0:
        return prefix
    if reason_count == 1:
        return f"{prefix}: {first_reason}"
    return f"{prefix}: {first_reason} (+{reason_count - 1} more)"
