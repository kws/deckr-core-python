"""Manager context."""

from __future__ import annotations

from typing import TYPE_CHECKING

import anyio

from deckr.core.util.anyio import OwnedTaskGroup

if TYPE_CHECKING:
    from deckr.hardware.manager import HardwareManagerRuntime


class HardwareManagerContext:
    """Context-owned hardware manager lifecycle and worker group."""

    def __init__(
        self,
        runtime: HardwareManagerRuntime,
        *,
        task_group: anyio.abc.TaskGroup,
    ) -> None:
        self.runtime = runtime
        self._tasks = OwnedTaskGroup(
            task_group,
            not_started_message=(
                "Deckr must be started before entering hardware manager contexts"
            ),
        )
        self._entered = False
        self._close_started = False
        self._close_lock = anyio.Lock()
        self._close_done = anyio.Event()

    async def __aenter__(self) -> HardwareManagerRuntime:
        if self._entered:
            raise RuntimeError("hardware manager contexts cannot be re-entered")
        self._entered = True
        task_group = await self._tasks.start()
        try:
            await self.runtime.start(task_group)
        except BaseException:
            await self.aclose()
            raise
        return self.runtime

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        with anyio.CancelScope(shield=True):
            async with self._close_lock:
                if self._close_started:
                    first = False
                else:
                    first = True
                    self._close_started = True
        if not first:
            with anyio.CancelScope(shield=True):
                await self._close_done.wait()
            return
        try:
            with anyio.CancelScope(shield=True):
                await self.runtime._begin_close()
                self._tasks.cancel()
                await self._tasks.aclose()
                await self.runtime.stop()
        finally:
            with anyio.CancelScope(shield=True):
                self._close_done.set()
