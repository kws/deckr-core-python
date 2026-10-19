"""State."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import TracebackType
from typing import Generic, TypeVar

import anyio

logger = logging.getLogger(__name__)


K = TypeVar("K")


V = TypeVar("V")


S = TypeVar("S")


MAX_COALESCED_STATE_IDENTITIES = 256


MAX_COALESCED_STATE_SUBSCRIPTIONS = 256


class StateSubscriptionLimitExceeded(RuntimeError):
    """Raised when a state broadcaster cannot admit another subscriber."""


class _CancellationSafeMutex:
    """A short critical-section mutex whose owner cannot be cancelled mid-handoff."""

    def __init__(self) -> None:
        self._semaphore = anyio.Semaphore(1)
        self._scopes: dict[int, anyio.CancelScope] = {}

    async def __aenter__(self) -> _CancellationSafeMutex:
        task_id = anyio.get_current_task().id
        scope = anyio.CancelScope(shield=True)
        scope.__enter__()
        try:
            await self._semaphore.acquire()
        except BaseException as exc:
            scope.__exit__(type(exc), exc, exc.__traceback__)
            raise
        self._scopes[task_id] = scope
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        task_id = anyio.get_current_task().id
        scope = self._scopes.pop(task_id)
        self._semaphore.release()
        return scope.__exit__(exc_type, exc, traceback)

    async def acquire(self) -> None:
        await self._semaphore.acquire()

    def release(self) -> None:
        self._semaphore.release()

    def statistics(self) -> anyio.SemaphoreStatistics:
        return self._semaphore.statistics()


@dataclass(frozen=True, slots=True)
class CoalescedStateChange(Generic[K]):
    """Immutable current-state wakeup produced by a state broadcaster."""

    version: int
    current: bool
    changed: frozenset[K]
    resnapshot_required: bool


@dataclass(slots=True, eq=False)
class _StateRegistration(Generic[K]):
    send: anyio.abc.ObjectSendStream[None]
    receive: anyio.abc.ObjectReceiveStream[None]
    version: int
    current: bool
    changed: set[K] = field(default_factory=set)
    resnapshot_required: bool = False
    notified: bool = False
    closed: bool = False


class CoalescedStateSubscription(Generic[K, S]):
    """One admitted broadcaster registration and its atomic initial snapshot."""

    def __init__(
        self,
        broadcaster: CoalescedStateBroadcaster[K],
        registration: _StateRegistration[K],
        initial: S,
    ) -> None:
        self._broadcaster = broadcaster
        self._registration = registration
        self.initial = initial

    def __aiter__(self) -> CoalescedStateSubscription[K, S]:
        return self

    async def __anext__(self) -> CoalescedStateChange[K]:
        try:
            return await self.receive()
        except (anyio.ClosedResourceError, anyio.EndOfStream):
            raise StopAsyncIteration from None

    async def receive(self) -> CoalescedStateChange[K]:
        registration = self._registration
        while True:
            await registration.receive.receive()
            async with self._broadcaster.lock:
                if registration.closed:
                    raise anyio.ClosedResourceError
                # ``notified`` stays true until the pending state is consumed.
                # A publisher racing between the stream receive and this lock
                # therefore coalesces into this same immutable wakeup instead of
                # leaving an empty token behind or losing the wakeup entirely.
                registration.notified = False
                change = CoalescedStateChange(
                    version=registration.version,
                    current=registration.current,
                    changed=frozenset(registration.changed),
                    resnapshot_required=registration.resnapshot_required,
                )
                registration.changed.clear()
                registration.resnapshot_required = False
                return change


class CoalescedStateBroadcaster(Generic[K]):
    """Bounded, non-blocking fanout for versioned current-state wakeups.

    Owners use :attr:`lock` for both their in-memory state commit and
    :meth:`publish_locked`. Subscriber registration and initial snapshot capture
    use that same lock, making the first snapshot atomic with respect to state
    changes. Later wakeups coalesce by identity and never await a reader.
    """

    def __init__(
        self,
        *,
        current: bool = False,
        max_changed: int = MAX_COALESCED_STATE_IDENTITIES,
        max_subscriptions: int = MAX_COALESCED_STATE_SUBSCRIPTIONS,
    ) -> None:
        if max_changed <= 0:
            raise ValueError("max_changed must be greater than zero")
        if max_subscriptions <= 0:
            raise ValueError("max_subscriptions must be greater than zero")
        self.lock = _CancellationSafeMutex()
        self._version = 0
        self._current = current
        self._max_changed = max_changed
        self._max_subscriptions = max_subscriptions
        self._registrations: set[_StateRegistration[K]] = set()
        self._closed = False

    @property
    def version(self) -> int:
        return self._version

    @property
    def current(self) -> bool:
        return self._current

    @property
    def subscription_count(self) -> int:
        return len(self._registrations)

    async def publish(
        self,
        changed: Iterable[K] = (),
        *,
        current: bool | None = None,
        resnapshot_required: bool = False,
    ) -> CoalescedStateChange[K]:
        identities = frozenset(changed)
        async with self.lock:
            return self.publish_locked(
                identities,
                current=current,
                resnapshot_required=resnapshot_required,
            )

    def publish_locked(
        self,
        changed: Iterable[K] = (),
        *,
        current: bool | None = None,
        resnapshot_required: bool = False,
    ) -> CoalescedStateChange[K]:
        """Commit one view version while the broadcaster lock is held."""

        if self._closed:
            raise anyio.ClosedResourceError
        identities = frozenset(changed)
        if len(identities) > self._max_changed:
            identities = frozenset()
            resnapshot_required = True
        if current is not None:
            self._current = current
        self._version += 1
        change = CoalescedStateChange(
            version=self._version,
            current=self._current,
            changed=identities,
            resnapshot_required=resnapshot_required,
        )
        for registration in tuple(self._registrations):
            if registration.closed:
                self._registrations.discard(registration)
                continue
            registration.version = change.version
            registration.current = change.current
            if change.resnapshot_required:
                registration.changed.clear()
                registration.resnapshot_required = True
            elif not registration.resnapshot_required:
                registration.changed.update(change.changed)
                if len(registration.changed) > self._max_changed:
                    registration.changed.clear()
                    registration.resnapshot_required = True
            if registration.notified:
                continue
            try:
                registration.send.send_nowait(None)
                registration.notified = True
            except (anyio.BrokenResourceError, anyio.ClosedResourceError):
                registration.closed = True
                self._registrations.discard(registration)
        return change

    async def capture(self, snapshot: Callable[[int, bool], S]) -> S:
        """Capture owner state and broadcaster metadata under the state lock."""

        async with self.lock:
            if self._closed:
                raise anyio.ClosedResourceError
            return snapshot(self._version, self._current)

    @asynccontextmanager
    async def subscribe(
        self,
        snapshot: Callable[[int, bool], S],
    ) -> AsyncIterator[CoalescedStateSubscription[K, S]]:
        send, receive = anyio.create_memory_object_stream[None](max_buffer_size=1)
        async with self.lock:
            if self._closed:
                await send.aclose()
                await receive.aclose()
                raise anyio.ClosedResourceError
            if len(self._registrations) >= self._max_subscriptions:
                await send.aclose()
                await receive.aclose()
                raise StateSubscriptionLimitExceeded(
                    "state broadcaster supports at most "
                    f"{self._max_subscriptions} simultaneous subscriptions"
                )
            registration = _StateRegistration(
                send=send,
                receive=receive,
                version=self._version,
                current=self._current,
            )
            self._registrations.add(registration)
            initial = snapshot(self._version, self._current)
        subscription = CoalescedStateSubscription(self, registration, initial)
        try:
            async with send, receive:
                yield subscription
        finally:
            async with self.lock:
                registration.closed = True
                self._registrations.discard(registration)

    async def aclose(self) -> None:
        async with self.lock:
            if self._closed:
                return
            self._closed = True
            registrations = tuple(self._registrations)
            self._registrations.clear()
            for registration in registrations:
                registration.closed = True
        for registration in registrations:
            await registration.send.aclose()


T = TypeVar("T")
