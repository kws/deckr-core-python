"""Cancellation ownership for bounded runtime worker groups."""

from __future__ import annotations

import anyio


class OwnedTaskGroup:
    """A private worker group whose cancel scope is owned by a parent runner."""

    def __init__(
        self,
        parent: anyio.abc.TaskGroup | None,
        *,
        not_started_message: str,
        join_seconds: float = 5.0,
    ) -> None:
        self._parent = parent
        self._not_started_message = not_started_message
        self._join_seconds = join_seconds
        self._group: anyio.abc.TaskGroup | None = None
        self._done = anyio.Event()

    @property
    def group(self) -> anyio.abc.TaskGroup:
        if self._group is None:
            raise RuntimeError("Owned task group has not started")
        return self._group

    async def start(self) -> anyio.abc.TaskGroup:
        if self._group is not None:
            raise RuntimeError("Owned task group is already started")
        if self._parent is None:
            raise RuntimeError(self._not_started_message)
        self._group = await self._parent.start(self._run)
        return self._group

    def cancel(self) -> None:
        group = self._group
        if group is not None:
            group.cancel_scope.cancel()

    async def aclose(self) -> None:
        if self._group is None:
            return
        self.cancel()
        with anyio.move_on_after(self._join_seconds, shield=True):
            await self._done.wait()
        self._group = None

    async def _run(
        self,
        *,
        task_status: anyio.abc.TaskStatus[anyio.abc.TaskGroup] = (
            anyio.TASK_STATUS_IGNORED
        ),
    ) -> None:
        try:
            async with anyio.create_task_group() as group:
                task_status.started(group)
                await anyio.sleep_forever()
        finally:
            self._done.set()
