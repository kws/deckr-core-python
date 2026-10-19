"""Subscription session."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic

import anyio

from deckr.core.util.anyio import CoalescedStateSubscription
from deckr.services.messages import ServiceMessageBody
from deckr.services.subscription_state import ResourceT, ServiceSubscriptionMessage

if TYPE_CHECKING:
    from deckr.services.subscription_manager import SharedResourceSubscriptionManager


@dataclass(slots=True)
class _LogicalSubscriber(Generic[ResourceT]):
    resources: set[ResourceT]


class _LogicalMessageStream(Generic[ResourceT]):
    def __init__(
        self,
        manager: SharedResourceSubscriptionManager[ResourceT],
        session_id: str,
        subscription: CoalescedStateSubscription[
            ResourceT,
            tuple[ResourceT, ...],
        ],
        subscription_context: AbstractAsyncContextManager[
            CoalescedStateSubscription[ResourceT, tuple[ResourceT, ...]]
        ],
    ) -> None:
        self._manager = manager
        self._session_id = session_id
        self._subscription = subscription
        self._subscription_context = subscription_context
        self._pending = list(subscription.initial)
        self._closed = False

    def __aiter__(self) -> _LogicalMessageStream[ResourceT]:
        return self

    async def __anext__(self) -> ServiceSubscriptionMessage[ResourceT]:
        try:
            return await self.receive()
        except anyio.EndOfStream:
            raise StopAsyncIteration from None

    async def receive(self) -> ServiceSubscriptionMessage[ResourceT]:
        while True:
            while self._pending:
                resource = self._pending.pop(0)
                message = await self._manager._current_session_message(  # noqa: SLF001
                    self._session_id,
                    resource,
                )
                if message is not None:
                    return message
            wakeup = await self._subscription.receive()
            self._pending = list(
                await self._manager._dirty_session_resources(  # noqa: SLF001
                    self._session_id,
                    None if wakeup.resnapshot_required else wakeup.changed,
                )
            )

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._manager.close_session(self._session_id)
        await self._subscription_context.__aexit__(None, None, None)


class ResourceSubscriptionSession(Generic[ResourceT]):
    """Logical subscription handle backed by a shared resource manager."""

    def __init__(
        self,
        manager: SharedResourceSubscriptionManager[ResourceT],
        session_id: str,
        messages: _LogicalMessageStream[ResourceT],
    ) -> None:
        self._manager = manager
        self._session_id = session_id
        self._messages = messages
        self._closed = False

    @property
    def messages(
        self,
    ) -> _LogicalMessageStream[ResourceT]:
        return self._messages

    async def ensure(self, resources: Collection[ResourceT]) -> None:
        await self._manager.ensure(self._session_id, resources)

    async def set(self, resources: Collection[ResourceT]) -> None:
        await self._manager.set(self._session_id, resources)

    async def drop(self, resources: Collection[ResourceT]) -> None:
        await self._manager.drop(self._session_id, resources)

    async def request(
        self,
        operation: str,
        params: Mapping[str, Any] | None = None,
        *,
        required_resource: ResourceT | None = None,
        timeout_seconds: float | None = None,
    ) -> ServiceMessageBody:
        return await self._manager.request(
            operation,
            params,
            required_resource=required_resource,
            timeout_seconds=timeout_seconds,
        )

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._messages.aclose()
