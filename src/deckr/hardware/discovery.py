"""A shared, typed hardware directory over the runtime's Beacon view."""

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass

import anyio

from deckr.beacon import Beacon, Candidate
from deckr.contracts.messages import EndpointAddress
from deckr.core.util.owned_tasks import OwnedTaskGroup
from deckr.hardware._state import HardwareStateChannel
from deckr.hardware.descriptors import DeviceDescriptor, DeviceRef
from deckr.hardware.profiles import (
    HARDWARE_FEATURE_ID,
    ProfileCapacity,
    hardware_payload_from_advertisement,
)
from deckr.hardware.state import HardwareUnavailable

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class HardwareCandidate:
    manager_endpoint: EndpointAddress
    manager_session_id: str
    device_ref: DeviceRef
    descriptor: DeviceDescriptor
    capacity: ProfileCapacity
    advertisement_key: str
    revision: int

    @property
    def identity(self) -> tuple[str, str, str]:
        return (
            str(self.manager_endpoint),
            self.manager_session_id,
            self.device_ref.device_id,
        )


def hardware_candidates(candidate: Candidate) -> tuple[HardwareCandidate, ...]:
    payload = hardware_payload_from_advertisement(candidate.advertisement)
    return tuple(
        HardwareCandidate(
            payload.manager_endpoint,
            payload.session_id,
            device.device_ref,
            device.descriptor,
            device.capacity,
            candidate.key,
            candidate.revision,
        )
        for _key, device in sorted(payload.devices.items())
    )


@dataclass(frozen=True, slots=True)
class HardwareDiscoveryState:
    version: int
    current: bool
    candidates: tuple[HardwareCandidate, ...]
    closed: bool = False


class HardwareDirectory:
    def __init__(self, beacon: Beacon, task_group: anyio.abc.TaskGroup) -> None:
        self._beacon = beacon
        self._tasks = OwnedTaskGroup(
            task_group, not_started_message="Hardware runtime must be running"
        )
        self._current = False
        self._closed = False
        self._source_closed = False
        self._candidates: tuple[HardwareCandidate, ...] = ()
        self._state = HardwareStateChannel(self._snapshot)

    def _snapshot(self) -> HardwareDiscoveryState:
        return HardwareDiscoveryState(
            0,
            not self._closed and self._current and self._beacon.is_current(),
            self._candidates,
            self._closed or self._source_closed,
        )

    @property
    def state(self) -> HardwareDiscoveryState:
        return self._state.state

    def watch_state(
        self,
    ) -> AbstractAsyncContextManager[AsyncIterator[HardwareDiscoveryState]]:
        return self._state.watch()

    async def start(self) -> None:
        group = await self._tasks.start()
        group.start_soon(self._run)

    def candidates(self) -> tuple[HardwareCandidate, ...]:
        if not self.state.current:
            raise HardwareUnavailable(
                "discovery_unavailable", "Hardware discovery is not current"
            )
        return self._candidates

    async def wait_for_device(
        self,
        predicate: Callable[[HardwareCandidate], bool],
        *,
        timeout_seconds: float | None = None,
    ) -> HardwareCandidate:
        with anyio.fail_after(timeout_seconds):
            async with self.watch_state() as states:
                async for state in states:
                    if state.current:
                        for candidate in state.candidates:
                            if predicate(candidate):
                                return candidate
        raise HardwareUnavailable("discovery_closed", "Hardware discovery closed")

    def require_current(self, selected: HardwareCandidate) -> None:
        # Capacity and advertisement revisions can change without invalidating
        # the descriptor/session selected for a new negotiation.
        if not any(
            candidate.identity == selected.identity
            and candidate.descriptor == selected.descriptor
            and candidate.device_ref == selected.device_ref
            for candidate in self.candidates()
        ):
            raise HardwareUnavailable(
                "candidate_changed", "Selected hardware candidate is no longer current"
            )

    async def _run(self) -> None:
        try:
            async with self._beacon.watch(HARDWARE_FEATURE_ID) as states:
                async for snapshot in states:
                    parsed: dict[tuple[str, str, str], HardwareCandidate] = {}
                    for source in sorted(
                        snapshot.candidates, key=lambda item: (item.revision, item.key)
                    ):
                        try:
                            candidates = hardware_candidates(source)
                        except (TypeError, ValueError):
                            continue
                        for candidate in candidates:
                            parsed[candidate.identity] = candidate
                    self._candidates = tuple(parsed[key] for key in sorted(parsed))
                    self._current = snapshot.current
                    await self._state.publish()
        except Exception:
            logger.exception("Hardware discovery watch failed")
        finally:
            with anyio.CancelScope(shield=True):
                self._current = False
                self._source_closed = True
                await self._state.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        with anyio.CancelScope(shield=True):
            await self._tasks.aclose()
            await self._state.aclose()
