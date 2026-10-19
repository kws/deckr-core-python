"""ServiceConsumerViews."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from typing import Any

import anyio

from deckr.contracts.authority import ContractPointer
from deckr.core.kv import KvBucketPolicy, KvUnavailable
from deckr.core.util.guarded_stream import guarded_changes
from deckr.lanes import EndpointSession
from deckr.services._client_helpers import _service_view_unavailable
from deckr.services.managed_views import _LeaseManagedServiceViewAccess
from deckr.services.runtime import (
    ServiceUseLease,
    ServiceViewReadContext,
    ServiceViewRef,
    ServiceViewWriteContext,
    ServiceViewWriter,
)
from deckr.services.views import (
    ManagedServiceViewAccess,
    ServiceViewEntry,
    ServiceViewStore,
)


class ServiceConsumerViews:
    def __init__(
        self,
        endpoint: EndpointSession,
        task_group: anyio.abc.TaskGroup,
        kv_bucket_for: Callable[[KvBucketPolicy], Any],
        stores: dict[str, ServiceViewStore],
    ) -> None:
        self._endpoint = endpoint
        self._task_group = task_group
        self._kv_bucket_for = kv_bucket_for
        self._view_stores = stores

    async def _create_view(
        self,
        lease: ServiceUseLease,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        """Create a consumer-written retained service view under the lease fence."""

        await lease.require_authority()
        return await self._view_store(view.store_name).create(
            view=view,
            payload=payload,
            context=self._consumer_view_write_context(lease),
            ttl=ttl,
        )

    async def _put_view(
        self,
        lease: ServiceUseLease,
        view: ServiceViewRef,
        payload: Mapping[str, Any],
        *,
        revision: int | None = None,
        ttl: float | None = None,
    ) -> ServiceViewEntry:
        """Put a consumer-written retained service view under the lease fence."""

        await lease.require_authority()
        return await self._view_store(view.store_name).put(
            view=view,
            payload=payload,
            context=self._consumer_view_write_context(lease),
            revision=revision,
            ttl=ttl,
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
        """Update a consumer-written retained service view under the lease fence."""

        await lease.require_authority()
        return await self._view_store(view.store_name).update(
            view=view,
            payload=payload,
            context=self._consumer_view_write_context(lease),
            revision=revision,
            ttl=ttl,
        )

    async def _delete_view(
        self,
        lease: ServiceUseLease,
        view: ServiceViewRef,
        *,
        revision: int | None = None,
    ) -> None:
        """Delete a consumer-written retained service view under the lease fence."""

        await lease.require_authority()
        await self._view_store(view.store_name).delete(
            view=view,
            context=self._consumer_view_write_context(lease),
            revision=revision,
        )

    async def _read_view(
        self,
        lease: ServiceUseLease,
        view: ServiceViewRef,
    ) -> Mapping[str, Any] | None:
        """Read a fenced service view entry authorized by the supplied lease.

        ``None`` is an ordinary absent-view value under the active lease; it
        does not imply service-use loss.
        """

        try:
            await lease.require_authority()
            entry = await self._view_store(view.store_name).get(
                self._consumer_view_read_context(lease),
                view,
            )
        except KvUnavailable as exc:
            raise _service_view_unavailable(view) from exc
        return dict(entry.value) if entry is not None else None

    async def _watch_view(
        self,
        lease: ServiceUseLease,
        view: ServiceViewRef,
    ) -> AsyncIterator[Mapping[str, Any] | None]:
        """Yield the current fenced service view payload and subsequent changes.

        A yielded ``None`` reports absent view state under the same service-use
        lease. Consumers should treat service-use lifecycle changes as
        ``ServiceUnavailable`` failures from the lease, command, or view
        infrastructure instead.
        """

        try:
            store = self._view_store(view.store_name)
            await lease.require_authority()
            async with store.watch(
                self._consumer_view_read_context(lease),
                view,
            ) as changes:
                async for snapshot in guarded_changes(
                    changes,
                    check=lease.require_authority,
                    watch=lease.agreement.watch_state,
                ):
                    yield (
                        dict(snapshot.entry.value)
                        if snapshot.entry is not None
                        else None
                    )
        except KvUnavailable as exc:
            raise _service_view_unavailable(view) from exc

    def _view_access(
        self,
        lease: ServiceUseLease,
        store_name: str,
    ) -> ManagedServiceViewAccess:
        """Return consumer-side managed view access for one store under a lease."""

        return _LeaseManagedServiceViewAccess(
            self._view_store(store_name),
            lease=lease,
            read_context=self._consumer_view_read_context(lease),
            write_context=self._consumer_view_write_context(lease),
        )

    def _view_store(self, store_name: str) -> ServiceViewStore:
        store = self._view_stores.get(store_name)
        if store is None:
            policy = KvBucketPolicy(
                bucket=store_name,
                ttl_seconds=None,
                description=f"Deckr service view {store_name}",
            )
            store = ServiceViewStore(bucket=self._kv_bucket_for(policy))
            store.start(self._task_group)
            self._view_stores[store_name] = store
        return store

    def _consumer_view_write_context(
        self,
        lease: ServiceUseLease,
    ) -> ServiceViewWriteContext:
        descriptor = lease.descriptor
        return ServiceViewWriteContext(
            writer=ServiceViewWriter.CONSUMER,
            service_id=descriptor.service_id,
            service_namespace=descriptor.namespace,
            service_endpoint=descriptor.endpoint,
            service_session_id=descriptor.session_id,
            consumer_endpoint=self._endpoint.address,
            consumer_session_id=self._endpoint.session_id,
            contract=ContractPointer(
                contractId=lease.contract.contract_id,
                generation=lease.contract.generation,
            ),
            views=descriptor.views,
        )

    def _consumer_view_read_context(
        self,
        lease: ServiceUseLease,
    ) -> ServiceViewReadContext:
        descriptor = lease.descriptor
        return ServiceViewReadContext(
            reader=ServiceViewWriter.CONSUMER,
            service_id=descriptor.service_id,
            service_namespace=descriptor.namespace,
            service_endpoint=descriptor.endpoint,
            service_session_id=descriptor.session_id,
            consumer_endpoint=self._endpoint.address,
            consumer_session_id=self._endpoint.session_id,
            contract=ContractPointer(
                contractId=lease.contract.contract_id,
                generation=lease.contract.generation,
            ),
            views=descriptor.views,
        )
