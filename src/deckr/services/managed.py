"""Consumer service capabilities governed by a Concord agreement."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import replace
from typing import TYPE_CHECKING, Any

import anyio

from deckr.contracts.authority import ContractPointer
from deckr.contracts.messages import DeckrMessage
from deckr.core.util.anyio import CoalescedStateBroadcaster
from deckr.services._client_helpers import (
    _deadline,
    _lease_pointer,
    _managed_failure,
    _remaining_timeout,
)
from deckr.services.messages import ServiceMessageBody
from deckr.services.runtime import (
    ServiceDescriptor,
    ServiceUnavailable,
    ServiceUseLease,
    ServiceViewRef,
)
from deckr.services.state import (
    ManagedServiceFailure,
    ManagedServicePhase,
    ManagedServiceState,
    ManagedServiceStatePredicate,
)
from deckr.services.views import ManagedServiceViewAccess, ServiceViewEntry

if TYPE_CHECKING:
    from deckr.services._client import DeckrServices


class ManagedService:
    """Opaque capability whose state follows current Concord authority."""

    def __init__(
        self,
        client: DeckrServices,
        lease: ServiceUseLease | None = None,
        *,
        reconnect: bool = False,
    ) -> None:
        self._client = client
        self._lease = lease
        self._reconnect = reconnect
        self._closed = False
        self._close_callback: Callable[[], Awaitable[None]] | None = None
        self._close_lock = anyio.Lock()
        self._close_done = anyio.Event()
        self._close_started = False
        self._shutdown_failure: ServiceUnavailable | None = None
        self._state_bus = CoalescedStateBroadcaster[str](current=lease is not None)
        self._state = ManagedServiceState(
            version=0,
            phase=(
                ManagedServicePhase.READY
                if lease is not None
                else ManagedServicePhase.CONNECTING
            ),
            descriptor=lease.descriptor if lease is not None else None,
            local_authority=lease is not None,
            contract=_lease_pointer(lease) if lease is not None else None,
        )

    @property
    def state(self) -> ManagedServiceState:
        return self._state

    @property
    def descriptor(self) -> ServiceDescriptor:
        descriptor = self._state.descriptor
        if descriptor is None:
            raise ServiceUnavailable(
                "service_connecting",
                "Managed service has not selected a descriptor",
            )
        return descriptor

    @asynccontextmanager
    async def watch_state(
        self,
    ) -> AsyncIterator[AsyncIterator[ManagedServiceState]]:
        async with self._state_bus.subscribe(
            lambda _version, _current: self._state
        ) as subscription:

            async def stream() -> AsyncIterator[ManagedServiceState]:
                yield subscription.initial
                async for _change in subscription:
                    try:
                        yield await self._state_bus.capture(
                            lambda _version, _current: self._state
                        )
                    except anyio.ClosedResourceError:
                        return

            yield stream()

    async def wait_for_state(
        self,
        predicate: ManagedServiceStatePredicate,
        *,
        timeout_seconds: float | None = None,
    ) -> ManagedServiceState:
        async def wait() -> ManagedServiceState:
            async with self._state_bus.subscribe(
                lambda _version, _current: self._state
            ) as subscription:
                state = subscription.initial
                if predicate(state):
                    return state
                async for _change in subscription:
                    state = await self._state_bus.capture(
                        lambda _version, _current: self._state
                    )
                    if predicate(state):
                        return state
                    if state.phase == ManagedServicePhase.CLOSED:
                        raise anyio.ClosedResourceError
            raise anyio.ClosedResourceError

        if timeout_seconds is None:
            return await wait()
        with anyio.fail_after(timeout_seconds):
            return await wait()

    async def aclose(self) -> None:
        """Close this managed capability and its current authority exactly once."""

        with anyio.CancelScope(shield=True):
            async with self._close_lock:
                if self._close_started:
                    first = False
                else:
                    first = True
                    self._close_started = True
            if not first:
                await self._close_done.wait()
                if self._shutdown_failure is not None:
                    raise self._shutdown_failure
                return
            try:
                callback = self._close_callback
                if callback is None:
                    await self._mark_closed()
                else:
                    await callback()
                if self._shutdown_failure is not None:
                    raise self._shutdown_failure
            finally:
                self._close_done.set()

    async def _record_cleanup_failure(self, error: Exception) -> None:
        failure = ServiceUnavailable("service_cleanup_failed", str(error))
        async with self._state_bus.lock:
            self._state = replace(
                self._state,
                cleanup_failures=self._state.cleanup_failures + 1,
                last_cleanup_failure=_managed_failure(failure),
            )
            if self._closed:
                self._state = replace(self._state, version=self._state.version + 1)
            else:
                self._publish_locked(
                    self._state.phase,
                    descriptor=self._state.descriptor,
                    local_authority=self._state.local_authority,
                    failure=self._state.failure,
                )
            if self._close_started:
                self._shutdown_failure = failure

    async def send(
        self,
        name: str,
        params: Mapping[str, Any] | None = None,
        *,
        event: Mapping[str, Any] | None = None,
    ) -> Any:
        lease = self._require_ready()
        return await self._client._send(lease, name, params, event=event)

    async def request(
        self,
        name: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> ServiceMessageBody:
        deadline: float | None = None
        lease = self._lease
        while True:
            if lease is None or self._closed:
                if deadline is None:
                    deadline = _deadline(timeout_seconds)
                lease = await self._wait_for_ready_lease(_remaining_timeout(deadline))
            try:
                # This is a local state check and contains no transport await.
                # A failure therefore proves the request has not been
                # published and is the one case that may wait for a successor.
                await lease.require_authority()
            except ServiceUnavailable:
                if not self._reconnect:
                    raise
                if deadline is None:
                    deadline = _deadline(timeout_seconds)
                lease = await self._wait_for_replacement_lease(
                    lease,
                    _remaining_timeout(deadline),
                )
                continue
            request_timeout = (
                timeout_seconds if deadline is None else _remaining_timeout(deadline)
            )
            break
        return await self._client._request(
            lease,
            name,
            params,
            timeout_seconds=request_timeout,
        )

    async def create_view(
        self,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        lease = self._require_ready()
        return await self._client._create_view(lease, view, payload, ttl=ttl)

    async def put_view(
        self,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        revision: int | None = None,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        lease = self._require_ready()
        return await self._client._put_view(
            lease,
            view,
            payload,
            revision=revision,
            ttl=ttl,
        )

    async def update_view(
        self,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        revision: int,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        lease = self._require_ready()
        return await self._client._update_view(
            lease,
            view,
            payload,
            revision=revision,
            ttl=ttl,
        )

    async def delete_view(
        self,
        view: ServiceViewRef,
        *,
        revision: int | None = None,
    ) -> None:
        lease = self._require_ready()
        await self._client._delete_view(lease, view, revision=revision)

    async def read_view(self, view: ServiceViewRef) -> Mapping[str, Any] | None:
        lease = await self._wait_for_ready_lease(None)
        return await self._client._read_view(lease, view)

    async def watch_view(
        self,
        view: ServiceViewRef,
    ) -> AsyncIterator[Mapping[str, Any] | None]:
        lease = await self._wait_for_ready_lease(None)
        async for payload in self._client._watch_view(lease, view):
            if self._lease is not lease:
                raise ServiceUnavailable(
                    "contract_local_authority_lost",
                    "Service view watch lost its original Concord authority",
                )
            yield payload

    async def authorize_inbound_message(
        self,
        message: DeckrMessage,
        *,
        name: str | None = None,
    ) -> ServiceMessageBody:
        lease = self._require_ready()
        return await self._client._authorize_inbound_message(
            lease,
            message,
            name=name,
        )

    def view_access(self, store_name: str) -> ManagedServiceViewAccess:
        lease = self._require_ready()
        return self._client._view_access(lease, store_name)

    async def _refresh(self) -> None:
        await self._require_ready().refresh()

    async def _wait_for_authority_loss(self) -> ServiceUnavailable:
        lease = self._require_ready()
        try:
            await lease.agreement.wait_for_state(lambda state: not state.valid)
        except anyio.ClosedResourceError:
            return ServiceUnavailable(
                "service_use_closed",
                "Managed service capability closed while waiting for authority",
                {"serviceId": self.descriptor.service_id},
            )
        try:
            await lease.require_authority()
        except ServiceUnavailable as exc:
            return exc
        return ServiceUnavailable(
            "contract_local_authority_lost",
            "Managed service capability lost local authority",
            {"serviceId": self.descriptor.service_id},
        )

    def _authority_pointer(self) -> ContractPointer:
        return _lease_pointer(self._require_ready())

    async def _set_lease(
        self,
        lease: ServiceUseLease,
    ) -> None:
        async with self._state_bus.lock:
            if self._closed:
                raise anyio.ClosedResourceError
            self._lease = lease
            self._publish_locked(
                ManagedServicePhase.READY,
                descriptor=lease.descriptor,
                local_authority=True,
            )

    async def _set_disconnected(
        self,
        lease: ServiceUseLease,
        failure: ServiceUnavailable,
    ) -> bool:
        async with self._state_bus.lock:
            if self._lease is not lease:
                return False
            self._lease = None
            self._publish_locked(
                ManagedServicePhase.RECONNECTING,
                descriptor=lease.descriptor,
                local_authority=False,
                failure=_managed_failure(failure),
            )
            return True

    async def _set_unavailable(self, failure: ServiceUnavailable) -> None:
        async with self._state_bus.lock:
            self._lease = None
            self._publish_locked(
                ManagedServicePhase.UNAVAILABLE,
                descriptor=self._state.descriptor,
                local_authority=False,
                failure=_managed_failure(failure),
            )

    async def _mark_closed(self) -> None:
        async with self._state_bus.lock:
            if self._state.phase == ManagedServicePhase.CLOSED:
                return
            self._closed = True
            self._lease = None
            self._publish_locked(
                ManagedServicePhase.CLOSING,
                descriptor=self._state.descriptor,
                local_authority=False,
            )
            self._publish_locked(
                ManagedServicePhase.CLOSED,
                descriptor=self._state.descriptor,
                local_authority=False,
            )
        await self._state_bus.aclose()

    def _require_ready(self) -> ServiceUseLease:
        lease = self._lease
        if self._closed or lease is None:
            raise ServiceUnavailable(
                ("service_use_closed" if self._closed else "service_reconnecting"),
                (
                    "Managed service capability is closed"
                    if self._closed
                    else "Managed service has no current Concord authority"
                ),
                (
                    {"serviceId": self._state.descriptor.service_id}
                    if self._state.descriptor is not None
                    else None
                ),
            )
        return lease

    async def _wait_for_ready_lease(
        self,
        timeout_seconds: float | None,
    ) -> ServiceUseLease:
        if self._closed:
            raise ServiceUnavailable(
                "service_use_closed",
                "Managed service capability is closed",
            )
        lease = self._lease
        if lease is not None and not self._closed:
            return lease
        try:
            state = await self.wait_for_state(
                lambda current: current.phase
                in {
                    ManagedServicePhase.READY,
                    ManagedServicePhase.UNAVAILABLE,
                    ManagedServicePhase.CLOSING,
                    ManagedServicePhase.CLOSED,
                },
                timeout_seconds=timeout_seconds,
            )
        except TimeoutError as exc:
            raise ServiceUnavailable(
                "service_reconnect_timeout",
                "Timed out waiting for replacement Concord authority",
            ) from exc
        if state.phase != ManagedServicePhase.READY or self._lease is None:
            failure = state.failure
            raise ServiceUnavailable(
                failure.code if failure is not None else "service_use_closed",
                (
                    failure.message
                    if failure is not None
                    else "Managed service capability is closed"
                ),
                failure.diagnostics if failure is not None else None,
            )
        return self._lease

    async def _wait_for_replacement_lease(
        self,
        predecessor: ServiceUseLease,
        timeout_seconds: float | None,
    ) -> ServiceUseLease:
        """Wait only when a request failed before it was published."""

        try:
            state = await self.wait_for_state(
                lambda current: (
                    self._lease is not predecessor
                    and current.phase
                    in {
                        ManagedServicePhase.READY,
                        ManagedServicePhase.UNAVAILABLE,
                        ManagedServicePhase.CLOSING,
                        ManagedServicePhase.CLOSED,
                    }
                ),
                timeout_seconds=timeout_seconds,
            )
        except TimeoutError as exc:
            raise ServiceUnavailable(
                "service_reconnect_timeout",
                "Timed out waiting for replacement Concord authority",
            ) from exc
        if state.phase != ManagedServicePhase.READY or self._lease is None:
            failure = state.failure
            raise ServiceUnavailable(
                failure.code if failure is not None else "service_use_closed",
                (
                    failure.message
                    if failure is not None
                    else "Managed service capability is closed"
                ),
                failure.diagnostics if failure is not None else None,
            )
        return self._lease

    def _publish_locked(
        self,
        phase: ManagedServicePhase,
        *,
        descriptor: ServiceDescriptor | None,
        local_authority: bool,
        failure: ManagedServiceFailure | None = None,
    ) -> None:
        self._state = ManagedServiceState(
            version=self._state_bus.version + 1,
            phase=phase,
            descriptor=descriptor,
            local_authority=local_authority,
            failure=failure,
            cleanup_failures=self._state.cleanup_failures,
            last_cleanup_failure=self._state.last_cleanup_failure,
            contract=_lease_pointer(self._lease) if self._lease is not None else None,
        )
        self._state_bus.publish_locked(("service",), current=local_authority)
