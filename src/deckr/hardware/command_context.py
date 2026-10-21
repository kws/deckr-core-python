"""Device work scoped to one validated hardware command and agreement."""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

import anyio

from deckr.contracts.messages import DeckrMessage
from deckr.hardware.descriptors import DeviceDescriptor

_Result = TypeVar("_Result")


@dataclass(frozen=True, slots=True)
class HardwareCommandContext:
    """Retain this context across queues; authorize immediately before each write.

    The manager owns the authority check. Device code must not reconstruct it
    from a contract pointer or from discovery. ``run_sync`` checks inside the
    worker thread, after any wait for thread capacity and before device I/O.
    """

    message: DeckrMessage
    device: DeviceDescriptor
    _authorize: Callable[[], None] = field(repr=False)

    def authorize(self) -> None:
        """Raise HardwareUnavailable when this operation has lost authority."""
        self._authorize()

    async def run_sync(self, function: Callable[..., _Result], *args: Any) -> _Result:
        """Run blocking device work only while this exact context is active."""
        self.authorize()

        def guarded() -> _Result:
            self.authorize()
            result = function(*args)
            self.authorize()
            return result

        return await anyio.to_thread.run_sync(guarded)
