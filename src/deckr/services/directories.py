"""ServiceDirectoryRegistry."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping
from typing import Any

import anyio

from deckr.beacon import Beacon, BeaconDirectory
from deckr.core.kv import KvUnavailable
from deckr.services._client_helpers import _directory_key, _DirectoryKey
from deckr.services.runtime import (
    ServiceDescriptor,
    ServiceProtocol,
    ServiceUnavailable,
    parse_service_descriptor,
)


class ServiceDirectoryRegistry:
    def __init__(
        self,
        beacon: Beacon,
        directories: dict[_DirectoryKey, BeaconDirectory[ServiceDescriptor]],
        task_group: anyio.abc.TaskGroup,
    ) -> None:
        self._beacon = beacon
        self._task_group = task_group
        self._directories = directories

    def directory(
        self,
        protocol: ServiceProtocol,
    ) -> BeaconDirectory[ServiceDescriptor]:
        """Return the managed directory for one service protocol."""

        key = _directory_key(protocol)
        directory = self._directories.get(key)
        if directory is None:
            directory = BeaconDirectory(
                self._beacon,
                protocol.feature_id,
                lambda candidate: parse_service_descriptor(candidate, protocol),
                log_label=f"DeckrServices[{protocol.namespace}]",
            )
            directory.start(self._task_group)
            self._directories[key] = directory
        return directory

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
        """Resolve a service descriptor from a current managed directory view."""

        directory = self.directory(protocol)
        try:
            if not directory.is_current():
                raise KvUnavailable("service discovery is not current")
            return directory.resolve(predicate, select=select)
        except (
            KvUnavailable,
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
        ) as exc:
            raise ServiceUnavailable(
                unavailable_code,
                unavailable_message,
                diagnostics,
            ) from exc

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
        """Wait for a descriptor and expose discovery failures as service errors."""

        try:
            return await self.directory(protocol).wait_for(
                predicate,
                select=select,
                **({"timeout": timeout_seconds} if timeout_seconds is not None else {}),
            )
        except (
            KvUnavailable,
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
        ) as exc:
            raise ServiceUnavailable(
                unavailable_code,
                unavailable_message,
                diagnostics,
            ) from exc
