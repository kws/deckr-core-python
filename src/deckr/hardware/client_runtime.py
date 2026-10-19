"""Runtime-owned discovery and reference-counted endpoint message routers."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import anyio

from deckr.beacon import Beacon
from deckr.concord import Concord
from deckr.contracts.authority import ContractPointer
from deckr.contracts.messages import HARDWARE_MESSAGES_LANE
from deckr.core.util.owned_tasks import OwnedTaskGroup
from deckr.hardware.discovery import HardwareDirectory
from deckr.hardware.state import HardwareUnavailable
from deckr.lanes import EndpointSession

if TYPE_CHECKING:
    from deckr.hardware.client import DeckrHardware
    from deckr.hardware.managed_claim import ManagedHardwareClaim

MAX_HARDWARE_CLIENTS = 256
MAX_ENDPOINT_CLAIMS = 64
logger = logging.getLogger(__name__)


class HardwareMessageRouter:
    def __init__(
        self, endpoint: EndpointSession, task_group: anyio.abc.TaskGroup
    ) -> None:
        self.endpoint = endpoint
        self.users = 0
        self._claims: dict[ContractPointer, ManagedHardwareClaim] = {}
        self._admitted: set[ManagedHardwareClaim] = set()
        self._tasks = OwnedTaskGroup(
            task_group, not_started_message="Hardware runtime is not running"
        )
        self._closed = False

    async def start(self):
        group = await self._tasks.start()
        try:
            await group.start(self._run)
        except BaseException:
            await self._tasks.aclose()
            raise

    def admit(self, claim: ManagedHardwareClaim) -> None:
        if self._closed:
            raise HardwareUnavailable(
                "hardware_subscription_closed", "Hardware subscription is closed"
            )
        if len(self._admitted) >= MAX_ENDPOINT_CLAIMS:
            raise HardwareUnavailable(
                "claim_limit", "Endpoint supports at most 64 hardware claims"
            )
        self._admitted.add(claim)

    def add(self, claim: ManagedHardwareClaim) -> None:
        if self._closed:
            raise HardwareUnavailable(
                "hardware_subscription_closed", "Hardware subscription is closed"
            )
        self._claims[claim._agreement.contract.pointer] = claim

    def remove(self, claim):
        self._admitted.discard(claim)
        for key, value in tuple(self._claims.items()):
            if value is claim:
                del self._claims[key]

    async def _run(self, *, task_status=anyio.TASK_STATUS_IGNORED):
        started = False
        try:
            async with self.endpoint.subscribe(HARDWARE_MESSAGES_LANE) as messages:
                task_status.started()
                started = True
                async for message in messages:
                    claim = self._claims.get(message.contract)
                    if claim is not None:
                        await claim._deliver(message)
        except Exception:
            if not started:
                raise
            logger.exception("Hardware endpoint subscription failed")
        finally:
            self._closed = True
            with anyio.CancelScope(shield=True):
                for claim in tuple(self._admitted):
                    await claim._fail(
                        "hardware_subscription_closed",
                        "Hardware message subscription closed",
                    )

    async def aclose(self):
        self._closed = True
        await self._tasks.aclose()


class HardwareRuntime:
    """Host-scoped sharing; never cached globally or across connections."""

    def __init__(
        self, beacon: Beacon, concord: Concord, task_group: anyio.abc.TaskGroup
    ) -> None:
        self.beacon = beacon
        self.concord = concord
        self.task_group = task_group
        self.directory = HardwareDirectory(beacon, task_group)
        self._started = False
        self._closed = False
        self._lock = anyio.Lock()
        self._close_lock = anyio.Lock()
        self._routers: dict[tuple[str, str], HardwareMessageRouter] = {}
        self._clients: set[DeckrHardware] = set()
        self._pending_clients: set[DeckrHardware] = set()

    def client(self, endpoint: EndpointSession) -> DeckrHardware:
        from deckr.hardware.client import DeckrHardware

        return DeckrHardware(self, endpoint)

    async def acquire(self, client: DeckrHardware) -> HardwareMessageRouter:
        # Admit before awaiting startup so runtime shutdown can cancel an entry
        # blocked in transport setup, including one waiting for the route lock.
        if self._closed:
            raise anyio.ClosedResourceError
        if len(self._clients | self._pending_clients) >= MAX_HARDWARE_CLIENTS:
            raise HardwareUnavailable(
                "client_limit", "Runtime supports at most 256 hardware clients"
            )
        self._pending_clients.add(client)
        try:
            return await self._acquire_router(client)
        finally:
            self._pending_clients.discard(client)

    async def _acquire_router(self, client: DeckrHardware) -> HardwareMessageRouter:
        async with self._lock:
            if self._closed:
                raise anyio.ClosedResourceError
            if not self._started:
                await self.directory.start()
                self._started = True
            key = (str(client.endpoint.address), client.endpoint.session_id)
            router = self._routers.get(key)
            if router is None:
                router = HardwareMessageRouter(client.endpoint, self.task_group)
                await router.start()
                self._routers[key] = router
            elif router.endpoint is not client.endpoint:
                raise ValueError(
                    "Reuse the same endpoint session object for shared hardware clients"
                )
            elif router._closed:
                raise HardwareUnavailable(
                    "hardware_subscription_closed",
                    "Close clients using the terminated endpoint route before reopening it",
                )
            router.users += 1
            self._clients.add(client)
            return router

    async def release(self, client: DeckrHardware) -> None:
        async with self._lock:
            if client not in self._clients:
                return
            self._clients.remove(client)
            key = (str(client.endpoint.address), client.endpoint.session_id)
            router = self._routers[key]
            router.users -= 1
            if not router.users:
                del self._routers[key]
                await router.aclose()

    async def aclose(self):
        self._closed = True
        with anyio.CancelScope(shield=True):
            async with self._close_lock:
                clients = tuple(self._clients | self._pending_clients)
                async with anyio.create_task_group() as cleanup:
                    for client in clients:
                        cleanup.start_soon(client.aclose)
                await self.directory.aclose()
