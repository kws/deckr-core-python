"""Private worker ownership for Beacon contexts."""

import anyio

from deckr.core.util.anyio import OwnedTaskGroup


class _BeaconTaskGroup(OwnedTaskGroup):
    def __init__(self, parent: anyio.abc.TaskGroup | None) -> None:
        super().__init__(
            parent,
            not_started_message="Beacon must be started before entering its contexts",
        )
