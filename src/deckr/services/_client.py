"""Deckr service discovery, candidate racing, and capability factories."""

from __future__ import annotations

import random
from collections.abc import AsyncIterator, Callable, Collection, Hashable, Mapping
from contextlib import asynccontextmanager
from typing import Any

import anyio

from deckr.beacon import Beacon, BeaconDirectory
from deckr.concord import Concord, ConcordConflict, ConcordUnavailable
from deckr.contracts.messages import DeckrMessage
from deckr.core.kv import JsonKvBucket, KvBucketPolicy
from deckr.lanes import EndpointSession
from deckr.services._client_helpers import (
    _MAX_MATCHING_SERVICE_CANDIDATES,
    _DirectoryKey,
    _retryable_service_negotiation_failure,
    _wait_for_service_authority_loss,
)
from deckr.services.consumer_messaging import ServiceConsumerMessaging
from deckr.services.consumer_views import ServiceConsumerViews
from deckr.services.directories import ServiceDirectoryRegistry
from deckr.services.managed import ManagedService
from deckr.services.messages import ServiceMessageBody
from deckr.services.negotiation import ServiceUseNegotiator
from deckr.services.runtime import (
    ServiceDescriptor,
    ServiceProtocol,
    ServiceUnavailable,
    ServiceUseLease,
    ServiceViewReadContext,
    ServiceViewRef,
    ServiceViewWriteContext,
)
from deckr.services.state import ManagedServicePhase
from deckr.services.views import (
    ManagedServiceViewAccess,
    ServiceViewEntry,
    ServiceViewStore,
)


class DeckrServices:
    """Managed surface for service discovery, authority, requests, and views."""

    def __init__(
        self,
        *,
        endpoint: EndpointSession,
        beacon: Beacon,
        concord: Concord,
        task_group: anyio.abc.TaskGroup,
        kv_bucket_for: Callable[[KvBucketPolicy], JsonKvBucket],
    ) -> None:
        self._endpoint = endpoint
        self._beacon = beacon
        self._concord = concord
        self._task_group = task_group
        self._kv_bucket_for = kv_bucket_for
        self._directories: dict[_DirectoryKey, BeaconDirectory[ServiceDescriptor]] = {}
        self._view_stores: dict[str, ServiceViewStore] = {}
        self._active_service_use_leases: dict[int, ServiceUseLease] = {}
        self._shared_managers: dict[Hashable, Any] = {}

        self._messaging = ServiceConsumerMessaging(endpoint)

        self._views = ServiceConsumerViews(
            endpoint, task_group, kv_bucket_for, self._view_stores
        )

        self._negotiator = ServiceUseNegotiator(endpoint, concord)

        self._discovery = ServiceDirectoryRegistry(
            beacon, self._directories, task_group
        )

    def directory(
        self, protocol: ServiceProtocol
    ) -> BeaconDirectory[ServiceDescriptor]:
        return self._discovery.directory(protocol)

    def resolve_descriptor(
        self,
        protocol: ServiceProtocol,
        predicate: Callable[[ServiceDescriptor], bool] | None = None,
        *,
        select: Callable[[Collection[ServiceDescriptor]], ServiceDescriptor | None]
        | None = None,
        unavailable_code: str = "service_discovery_pending",
        unavailable_message: str = "Service discovery is not current yet",
        diagnostics: Mapping[str, Any] | None = None,
    ) -> ServiceDescriptor | None:
        return self._discovery.resolve_descriptor(
            protocol,
            predicate,
            select=select,
            unavailable_code=unavailable_code,
            unavailable_message=unavailable_message,
            diagnostics=diagnostics,
        )

    async def wait_for_descriptor(
        self,
        protocol: ServiceProtocol,
        predicate: Callable[[ServiceDescriptor], bool] | None = None,
        *,
        select: Callable[[Collection[ServiceDescriptor]], ServiceDescriptor | None]
        | None = None,
        timeout_seconds: float | None = None,
        unavailable_code: str = "beacon_unavailable",
        unavailable_message: str = "Service discovery is unavailable",
        diagnostics: Mapping[str, Any] | None = None,
    ) -> ServiceDescriptor:
        return await self._discovery.wait_for_descriptor(
            protocol,
            predicate,
            select=select,
            timeout_seconds=timeout_seconds,
            unavailable_code=unavailable_code,
            unavailable_message=unavailable_message,
            diagnostics=diagnostics,
        )

    @asynccontextmanager
    async def use_matching(
        self,
        protocol: ServiceProtocol,
        *,
        predicate: Callable[[ServiceDescriptor], bool] | None = None,
        timeout_seconds: float | None = None,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[ManagedService]:
        """Race matching endpoint sessions and reconnect after authority loss."""

        service = ManagedService(self, reconnect=True)
        started = anyio.Event()
        done = anyio.Event()
        runner_scope: anyio.CancelScope | None = None

        async def run() -> None:
            nonlocal runner_scope
            try:
                with anyio.CancelScope() as scope:
                    runner_scope = scope
                    started.set()
                    await self._run_matching_service(
                        service,
                        protocol,
                        predicate,
                        diagnostics,
                    )
            finally:
                done.set()

        async def close() -> None:
            await service._mark_closed()
            if runner_scope is not None:
                runner_scope.cancel()
            with anyio.move_on_after(5, shield=True) as cleanup:
                await done.wait()
            if cleanup.cancelled_caught:
                await service._record_cleanup_failure(
                    TimeoutError("Service cleanup timed out")
                )

        service._close_callback = close  # noqa: SLF001
        self._task_group.start_soon(run)
        await started.wait()
        try:
            try:
                state = await service.wait_for_state(
                    lambda current: current.phase
                    in {
                        ManagedServicePhase.READY,
                        ManagedServicePhase.UNAVAILABLE,
                        ManagedServicePhase.CLOSED,
                    },
                    timeout_seconds=timeout_seconds,
                )
            except TimeoutError as exc:
                raise ServiceUnavailable(
                    "contract_timeout",
                    "Timed out waiting for a matching service contract",
                    diagnostics,
                ) from exc
            if state.phase != ManagedServicePhase.READY:
                failure = state.failure
                raise ServiceUnavailable(
                    failure.code if failure is not None else "service_use_closed",
                    (
                        failure.message
                        if failure is not None
                        else "Managed service closed during setup"
                    ),
                    failure.diagnostics if failure is not None else diagnostics,
                )
            yield service
        finally:
            await service.aclose()

    @asynccontextmanager
    async def use(
        self,
        descriptor: ServiceDescriptor,
        *,
        timeout_seconds: float | None = None,
    ) -> AsyncIterator[ManagedService]:
        """Open a scoped service-use contract and close it on context exit."""

        lease = await self._propose_service_use(
            descriptor,
            timeout_seconds=timeout_seconds,
        )
        self._active_service_use_leases[id(lease)] = lease
        service = ManagedService(self, lease)

        async def close() -> None:
            await service._mark_closed()
            await self._close_active_service_use_lease(
                lease,
                reason="service_use_closed",
                service=service,
            )

        service._close_callback = close  # noqa: SLF001
        try:
            yield service
        finally:
            await service.aclose()

    async def _run_matching_service(
        self,
        service: ManagedService,
        protocol: ServiceProtocol,
        predicate: Callable[[ServiceDescriptor], bool] | None,
        diagnostics: Mapping[str, Any] | None,
    ) -> None:
        winner: ServiceUseLease | None = None
        try:
            while not service._closed:  # noqa: SLF001
                try:
                    winner = await self._race_service_candidates(
                        protocol,
                        predicate=predicate,
                        diagnostics=diagnostics,
                    )
                except ServiceUnavailable as exc:
                    await service._set_unavailable(exc)
                    return
                if service._closed:  # noqa: SLF001
                    await self._close_active_service_use_lease(
                        winner,
                        reason="service_use_closed",
                        service=service,
                    )
                    winner = None
                    return
                await service._set_lease(winner)
                failure = await _wait_for_service_authority_loss(winner)
                if not await service._set_disconnected(winner, failure):
                    return
                await self._close_active_service_use_lease(
                    winner,
                    reason="service_use_reconnecting",
                    service=service,
                )
                winner = None
        finally:
            if winner is not None:
                with anyio.CancelScope(shield=True):
                    await self._close_active_service_use_lease(
                        winner,
                        reason="service_use_closed",
                        service=service,
                    )

    async def _race_service_candidates(
        self,
        protocol: ServiceProtocol,
        *,
        predicate: Callable[[ServiceDescriptor], bool] | None,
        diagnostics: Mapping[str, Any] | None,
    ) -> ServiceUseLease:
        directory = self.directory(protocol)
        send, receive = anyio.create_memory_object_stream[
            tuple[
                str,
                ServiceDescriptor | None,
                ServiceUseLease | None,
                BaseException | None,
            ]
        ](_MAX_MATCHING_SERVICE_CANDIDATES + 1)
        attempted: set[tuple[str, str]] = set()
        successful: dict[tuple[str, str], ServiceUseLease] = {}
        retry_wakeups: dict[tuple[str, str], anyio.Event] = {}
        winner_identity: tuple[str, str] | None = None

        async def attempt(
            identity: tuple[str, str],
            descriptor: ServiceDescriptor,
        ) -> None:
            delay = 0.1
            while True:
                try:
                    lease = await self._propose_service_use(
                        descriptor,
                        timeout_seconds=None,
                    )
                except ServiceUnavailable as exc:
                    if not _retryable_service_negotiation_failure(exc):
                        await send.send(("failed", descriptor, None, exc))
                        return
                    wakeup = anyio.Event()
                    retry_wakeups[identity] = wakeup
                    with anyio.move_on_after(random.uniform(delay * 0.5, delay)):
                        await wakeup.wait()
                    delay = 0.1 if wakeup.is_set() else min(delay * 2, 5.0)
                    continue
                successful[identity] = lease
                await send.send(("ready", descriptor, lease, None))
                return

        async def discover(task_group: anyio.abc.TaskGroup) -> None:
            try:
                async for descriptors in directory.watch_records():
                    for descriptor in descriptors:
                        if predicate is not None and not predicate(descriptor):
                            continue
                        identity = (str(descriptor.endpoint), descriptor.session_id)
                        if identity in attempted:
                            wakeup = retry_wakeups.get(identity)
                            if wakeup is not None:
                                wakeup.set()
                            continue
                        if len(attempted) >= _MAX_MATCHING_SERVICE_CANDIDATES:
                            await send.send(
                                (
                                    "fatal",
                                    None,
                                    None,
                                    ServiceUnavailable(
                                        "service_candidate_limit_exceeded",
                                        "More than 64 matching service candidates were admitted",
                                        diagnostics,
                                    ),
                                )
                            )
                            return
                        attempted.add(identity)
                        task_group.start_soon(attempt, identity, descriptor)
            except BaseException as exc:
                if isinstance(exc, anyio.get_cancelled_exc_class()):
                    raise
                await send.send(("fatal", None, None, exc))
            else:
                await send.send(
                    (
                        "fatal",
                        None,
                        None,
                        ServiceUnavailable(
                            "beacon_unavailable",
                            "Service discovery closed while negotiating",
                            diagnostics,
                        ),
                    )
                )

        failure: BaseException | None = None
        async with anyio.create_task_group() as attempts:
            attempts.start_soon(discover, attempts)
            async with receive:
                async for status, descriptor, lease, error in receive:
                    if status == "ready":
                        assert descriptor is not None and lease is not None
                        winner_identity = (
                            str(descriptor.endpoint),
                            descriptor.session_id,
                        )
                        attempts.cancel_scope.cancel()
                        break
                    if status == "fatal":
                        failure = error
                        attempts.cancel_scope.cancel()
                        break

        winner = (
            successful.get(winner_identity) if winner_identity is not None else None
        )
        for identity, lease in successful.items():
            self._active_service_use_leases[id(lease)] = lease
            if identity != winner_identity:
                await self._close_active_service_use_lease(
                    lease,
                    reason="service_use_race_lost",
                )
        if winner is not None:
            return winner
        if isinstance(failure, ServiceUnavailable):
            raise failure
        raise ServiceUnavailable(
            "beacon_unavailable",
            "Service discovery failed while negotiating",
            {
                **dict(diagnostics or {}),
                "reason": str(failure) if failure is not None else None,
            },
        )

    async def _send(
        self,
        lease: ServiceUseLease,
        name: str,
        params: Mapping[str, Any] | None = None,
        *,
        event: Mapping[str, Any] | None = None,
        causation_id: str | None = None,
    ) -> Any:
        return await self._messaging._send(
            lease, name, params, event=event, causation_id=causation_id
        )

    async def _request(
        self,
        lease: ServiceUseLease,
        name: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> ServiceMessageBody:
        return await self._messaging._request(
            lease, name, params, timeout_seconds=timeout_seconds
        )

    async def _create_view(
        self,
        lease: ServiceUseLease,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        return await self._views._create_view(lease, view, payload, ttl=ttl)

    async def _put_view(
        self,
        lease: ServiceUseLease,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        revision: int | None = None,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        return await self._views._put_view(
            lease, view, payload, revision=revision, ttl=ttl
        )

    async def _update_view(
        self,
        lease: ServiceUseLease,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        revision: int,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        return await self._views._update_view(
            lease, view, payload, revision=revision, ttl=ttl
        )

    async def _delete_view(
        self,
        lease: ServiceUseLease,
        view: ServiceViewRef,
        *,
        revision: int | None = None,
    ) -> None:
        return await self._views._delete_view(lease, view, revision=revision)

    async def _read_view(
        self, lease: ServiceUseLease, view: ServiceViewRef
    ) -> Mapping[str, Any] | None:
        return await self._views._read_view(lease, view)

    async def _watch_view(
        self, lease: ServiceUseLease, view: ServiceViewRef
    ) -> AsyncIterator[Mapping[str, Any] | None]:
        async for item in self._views._watch_view(lease, view):
            yield item

    async def _authorize_inbound_message(
        self, lease: ServiceUseLease, message: DeckrMessage, *, name: str | None = None
    ) -> ServiceMessageBody:
        return await self._messaging._authorize_inbound_message(
            lease, message, name=name
        )

    def _view_access(
        self, lease: ServiceUseLease, store_name: str
    ) -> ManagedServiceViewAccess:
        return self._views._view_access(lease, store_name)

    def get_shared_manager(self, key: Hashable, factory: Callable[[], Any]) -> Any:
        """Return a runtime-scoped shared service helper."""

        manager = self._shared_managers.get(key)
        if manager is None:
            manager = factory()
            self._shared_managers[key] = manager
        return manager

    async def aclose(self) -> None:
        with anyio.CancelScope(shield=True):
            failures: list[ServiceUnavailable] = []
            try:
                for manager in tuple(self._shared_managers.values()):
                    aclose = getattr(manager, "aclose", None)
                    if callable(aclose):
                        await aclose()
                self._shared_managers.clear()
                for lease in tuple(self._active_service_use_leases.values()):
                    try:
                        await self._close_active_service_use_lease(
                            lease,
                            reason="service_use_closed",
                        )
                    except ServiceUnavailable as exc:
                        failures.append(exc)
            finally:
                for directory in tuple(self._directories.values()):
                    await directory.aclose()
                self._directories.clear()
                for store in tuple(self._view_stores.values()):
                    await store.aclose()
                self._view_stores.clear()
            if failures:
                raise failures[0]

    async def _close_active_service_use_lease(
        self,
        lease: ServiceUseLease,
        *,
        reason: str,
        service: ManagedService | None = None,
    ) -> None:
        if self._active_service_use_leases.pop(id(lease), None) is None:
            return
        try:
            await lease.agreement.cancel(reason)
        except (ConcordConflict, ConcordUnavailable):
            pass
        finally:
            with anyio.CancelScope(shield=True):
                await lease.agreement.aclose()
                if lease.agreement.cleanup_failure is not None:
                    if service is not None:
                        await service._record_cleanup_failure(
                            lease.agreement.cleanup_failure
                        )
                    else:
                        raise ServiceUnavailable(
                            "service_cleanup_failed",
                            str(lease.agreement.cleanup_failure),
                        )

    async def _propose_service_use(
        self, descriptor: ServiceDescriptor, *, timeout_seconds: float | None
    ) -> ServiceUseLease:
        return await self._negotiator._propose_service_use(
            descriptor, timeout_seconds=timeout_seconds
        )

    def _view_store(self, store_name: str) -> ServiceViewStore:
        return self._views._view_store(store_name)

    def _consumer_view_write_context(
        self, lease: ServiceUseLease
    ) -> ServiceViewWriteContext:
        return self._views._consumer_view_write_context(lease)

    def _consumer_view_read_context(
        self, lease: ServiceUseLease
    ) -> ServiceViewReadContext:
        return self._views._consumer_view_read_context(lease)
