"""Task startup coordination and process signal integration."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import anyio
from anyio.abc import TaskGroup

from deckr.core.util.owned_tasks import OwnedTaskGroup as OwnedTaskGroup

logger = logging.getLogger(__name__)


@dataclass
class EnsureStarted:
    _lock: anyio.Lock
    _tg: TaskGroup | None
    _started: bool
    _ready: anyio.Event

    def __init__(self) -> None:
        self._lock = anyio.Lock()
        self._tg = None
        self._started = False
        self._ready = anyio.Event()

    @property
    def started(self) -> bool:
        return self._started

    def attach_task_group(self, tg: TaskGroup) -> None:
        self._tg = tg

    async def ensure_started(self, runner: Callable[[], Awaitable[None]]) -> None:
        if self._started:
            await self._ready.wait()
            return

        async with self._lock:
            if not self._started:
                if self._tg is None:
                    raise RuntimeError("No TaskGroup attached")
                self._tg.start_soon(runner)
                self._started = True

        await self._ready.wait()

    def mark_ready(self) -> None:
        self._ready.set()


async def add_signal_handler(tg: anyio.abc.TaskGroup):
    import signal

    cancelled = anyio.get_cancelled_exc_class()

    async def signal_handler():
        try:
            with anyio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
                async for sig in signals:
                    logger.info(
                        f"Received {signal.Signals(sig).name}, shutting down gracefully..."
                    )
                    tg.cancel_scope.cancel()
                    return
        except cancelled:
            raise

    tg.start_soon(signal_handler)
