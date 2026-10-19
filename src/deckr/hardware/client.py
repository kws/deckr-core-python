"""Endpoint-owned managed hardware clients."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

import anyio

from deckr.hardware.claim_spec import HardwareClaimSpec
from deckr.hardware.client_runtime import MAX_ENDPOINT_CLAIMS, HardwareRuntime
from deckr.hardware.discovery import HardwareCandidate
from deckr.hardware.managed_claim import ManagedHardwareClaim
from deckr.hardware.state import HardwareUnavailable
from deckr.lanes import EndpointSession

if TYPE_CHECKING:
    from deckr.hardware.recovery import HardwareRecoveryPolicy, ManagedHardwareUse


class DeckrHardware:
    def __init__(self, resources: HardwareRuntime, endpoint: EndpointSession) -> None:
        if endpoint.address.family != "controller":
            raise ValueError("Hardware claims require a controller endpoint")
        self.endpoint = endpoint
        self._resources = resources
        self.directory = resources.directory
        self._router = None
        self._entered = False
        self._closed = False
        self._closed_event = anyio.Event()
        self._claims: set[ManagedHardwareClaim] = set()
        self._uses: set[ManagedHardwareUse] = set()
        self._close_lock = anyio.Lock()
        self._enter_scope: anyio.CancelScope | None = None
        self._enter_done = anyio.Event()

    @property
    def closed(self) -> bool:
        return self._closed

    async def __aenter__(self) -> DeckrHardware:
        if self._entered or self.closed:
            raise RuntimeError("Hardware clients cannot be re-entered")
        self._entered = True
        try:
            with anyio.CancelScope() as scope:
                self._enter_scope = scope
                self._router = await self._resources.acquire(self)
                self._require_open()
                return self
            raise anyio.ClosedResourceError
        except BaseException:
            self._closed = True
            self._closed_event.set()
            with anyio.CancelScope(shield=True):
                await self._resources.release(self)
            raise
        finally:
            self._enter_scope = None
            self._enter_done.set()

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()

    def _require_open(self) -> None:
        if self.closed or self._router is None:
            raise anyio.ClosedResourceError

    def claim(self, spec: HardwareClaimSpec) -> ManagedHardwareClaim:
        self._require_open()
        return ManagedHardwareClaim(self, spec)

    def use(
        self, spec: HardwareClaimSpec, *, recover: HardwareRecoveryPolicy | None = None
    ) -> ManagedHardwareUse:
        """Own a claim and optionally recover using an explicit selection policy."""
        self._require_open()
        from deckr.hardware.recovery import ManagedHardwareUse

        return ManagedHardwareUse(self, spec, recover=recover)

    async def wait_for_device(
        self,
        predicate: Callable[[HardwareCandidate], bool],
        *,
        timeout_seconds: float | None = None,
    ) -> HardwareCandidate:
        self._require_open()
        selected = None
        failure = None
        done = anyio.Event()

        async def select():
            nonlocal selected, failure
            try:
                selected = await self.directory.wait_for_device(predicate)
            except Exception as exc:
                failure = exc
            finally:
                done.set()

        async def closed():
            await self._closed_event.wait()
            done.set()

        with anyio.fail_after(timeout_seconds):
            async with anyio.create_task_group() as group:
                group.start_soon(select)
                group.start_soon(closed)
                await done.wait()
                group.cancel_scope.cancel()
        self._require_open()
        if failure is not None:
            raise failure
        assert selected is not None
        return selected

    def _admit(self, claim: ManagedHardwareClaim) -> None:
        self._require_open()
        if len(self._claims) >= MAX_ENDPOINT_CLAIMS:
            raise HardwareUnavailable(
                "claim_limit", "Client supports at most 64 claims"
            )
        self._router.admit(claim)
        self._claims.add(claim)

    def _forget(self, claim: ManagedHardwareClaim) -> None:
        self._claims.discard(claim)

    async def aclose(self) -> None:
        with anyio.CancelScope(shield=True):
            async with self._close_lock:
                if self.closed:
                    return
                self._closed = True
                self._closed_event.set()
                if self._enter_scope is not None:
                    self._enter_scope.cancel()
                    await self._enter_done.wait()
                async with anyio.create_task_group() as cleanup:
                    for use in tuple(self._uses):
                        cleanup.start_soon(use.aclose)
                    for claim in tuple(self._claims):
                        cleanup.start_soon(claim.aclose)
                await self._resources.release(self)
