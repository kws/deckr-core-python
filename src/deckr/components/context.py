"""Context."""

from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Protocol,
)

if TYPE_CHECKING:
    from deckr.hardware import DeviceDescriptor, HardwareManagerContext
    from deckr.services import ServiceBackendStatus, ServiceProtocol, ServiceProvider


from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from pathlib import Path

from deckr.beacon import Beacon
from deckr.bindings.authority_stores import (
    RESERVED_AUTHORITY_BUCKET_POLICIES,
    ConcordMaintenanceStores,
)
from deckr.components.manifest import ComponentManifest
from deckr.concord import Concord
from deckr.contracts.messages import EndpointAddress, endpoint_address
from deckr.core.kv import KvBucketPolicy
from deckr.lanes import EndpointSession, Lane, LaneRegistry


class ComponentEndpointOpener(Protocol):
    def __call__(
        self,
        slot: str,
        *,
        session_id: str | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> AbstractAsyncContextManager[EndpointSession]: ...


@dataclass(frozen=True, slots=True)
class ComponentContext:
    component_id: str
    instance_id: str
    runtime_name: str
    manifest: ComponentManifest
    config: Mapping[str, Any]
    endpoints: Mapping[str, str]
    base_dir: Path
    lanes: LaneRegistry
    beacon: Beacon | None = None
    concord: Concord | None = None
    kv_bucket_for: Callable[[KvBucketPolicy], Any] | None = None
    _concord_maintenance_stores_for: (
        Callable[[], ConcordMaintenanceStores[Any]] | None
    ) = None
    endpoint_for: ComponentEndpointOpener | None = None

    def require_lane(self, name: str) -> Lane:
        return self.lanes.require(name)

    def require_endpoint_id(self, slot: str) -> str:
        endpoint_id = self.endpoints.get(slot)
        if endpoint_id is None:
            raise KeyError(
                f"Component {self.runtime_name!r} has no endpoint slot {slot!r}"
            )
        return endpoint_id

    def endpoint_address(self, slot: str) -> EndpointAddress:
        return endpoint_address(slot, self.require_endpoint_id(slot))

    def open_endpoint(
        self,
        slot: str,
        *,
        session_id: str | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> AbstractAsyncContextManager[EndpointSession]:
        if self.endpoint_for is None:
            raise RuntimeError(
                "Deckr component context does not provide endpoint sessions"
            )
        return self.endpoint_for(
            slot,
            session_id=session_id,
            metadata=metadata,
        )

    def require_beacon(self) -> Beacon:
        if self.beacon is None:
            raise RuntimeError("Deckr component context does not provide Beacon")
        return self.beacon

    def require_concord(self) -> Concord:
        if self.concord is None:
            raise RuntimeError("Deckr component context does not provide Concord")
        return self.concord

    def service_provider(
        self,
        endpoint: EndpointSession,
        protocol: ServiceProtocol,
        *,
        service_id: str,
        backend_status: ServiceBackendStatus | str | None = None,
        diagnostics: Mapping[str, object] | None = None,
        labels: Mapping[str, str] | None = None,
    ) -> ServiceProvider:
        """Return the canonical provider lifecycle for this component."""

        from deckr.services import ServiceBackendStatus, ServiceProvider

        return ServiceProvider(
            endpoint=endpoint,
            beacon=self.require_beacon(),
            concord=self.require_concord(),
            protocol=protocol,
            service_id=service_id,
            backend_status=(
                ServiceBackendStatus.AVAILABLE
                if backend_status is None
                else ServiceBackendStatus(backend_status)
            ),
            diagnostics=diagnostics,
            labels=labels,
        )

    def hardware_manager(
        self,
        endpoint: EndpointSession,
        *,
        manager_id: str,
        devices: Mapping[str, DeviceDescriptor] | None = None,
        labels: Mapping[str, str] | None = None,
        command_handler=None,
        reset_handler=None,
    ) -> HardwareManagerContext:
        """Return the canonical hardware lifecycle for this component."""

        from deckr.hardware import HardwareManagerContext, HardwareManagerRuntime

        return HardwareManagerContext(
            HardwareManagerRuntime(
                endpoint=endpoint,
                beacon=self.require_beacon(),
                concord=self.require_concord(),
                manager_id=manager_id,
                initial_devices=devices,
                labels=labels,
                command_handler=command_handler,
                reset_handler=reset_handler,
            )
        )

    def kv_bucket(self, policy: KvBucketPolicy) -> Any:
        if policy.bucket in RESERVED_AUTHORITY_BUCKET_POLICIES:
            raise ValueError(
                f"KV bucket {policy.bucket!r} is reserved for a typed Deckr "
                "authority-store capability"
            )
        if self.kv_bucket_for is None:
            raise RuntimeError("Deckr component context does not provide KV buckets")
        return self.kv_bucket_for(policy)

    def _concord_maintenance_stores(
        self,
    ) -> ConcordMaintenanceStores[Any]:
        if self._concord_maintenance_stores_for is None:
            raise RuntimeError(
                "Deckr component context does not provide Concord maintenance stores"
            )
        return self._concord_maintenance_stores_for()
