from __future__ import annotations

import random
from collections import defaultdict

import anyio

from deckr._concord._models import (
    ConcordConflict,
    ConcordConflictCode,
    ConcordUnavailable,
    ConcordUnavailableCode,
    ContractState,
    ContractValidityStatus,
)
from deckr._concord._repository import ConcordRepository
from deckr._concord._view import ConcordView
from deckr.core.identity import ContractPointer, EndpointAddress, parse_endpoint_address

_OWNER_RETRY_INITIAL_SECONDS = 0.1
_OWNER_RETRY_MAX_SECONDS = 5.0


class ConcordOwnerBootstrap:
    """Cancel proven authority loss without displacing competing live owners.

    Registry absence and session changes are not failure evidence. Profile
    owners must implement any exclusive admission or deliberate replacement.
    """

    def __init__(self, store: ConcordRepository, view: ConcordView) -> None:
        self._store = store
        self._view = view
        self._locks: defaultdict[str, anyio.Lock] = defaultdict(anyio.Lock)
        self._active_lock = anyio.Lock()
        self._active: defaultdict[str, dict[ContractPointer, str]] = defaultdict(dict)
        self._released: defaultdict[str, set[ContractPointer]] = defaultdict(set)

    async def claim(
        self,
        endpoint: str | EndpointAddress,
        session_id: str,
        pointer: ContractPointer,
    ) -> None:
        address = parse_endpoint_address(endpoint)
        key = str(address)
        async with self._locks[key]:
            await self._cleanup_stale(address, session_id)
            async with self._active_lock:
                self._active[key][pointer] = session_id

    async def bootstrap(
        self,
        endpoint: str | EndpointAddress,
        session_id: str,
    ) -> None:
        """Confirm stale local proposals are cancelled before owner readiness."""

        address = parse_endpoint_address(endpoint)
        async with self._locks[str(address)]:
            await self._cleanup_stale(address, session_id)

    async def release(
        self,
        endpoint: str | EndpointAddress,
        pointer: ContractPointer,
    ) -> None:
        key = str(parse_endpoint_address(endpoint))
        async with self._active_lock:
            if self._active[key].pop(pointer, None) is not None:
                self._released[key].add(pointer)

    async def _cancel_until_resolved(
        self,
        pointer: ContractPointer,
        endpoint: EndpointAddress,
    ) -> None:
        delay = _OWNER_RETRY_INITIAL_SECONDS
        while True:
            try:
                await self._store.cancel_pointer(
                    pointer,
                    endpoint,
                    reason="concord_owner_bootstrap_stale_contract",
                )
                return
            except ConcordConflict as exc:
                if exc.code != ConcordConflictCode.REVISION_CHANGED:
                    raise
                pass
            except ConcordUnavailable:
                pass
            await anyio.sleep(random.uniform(delay * 0.5, delay))
            delay = min(delay * 2, _OWNER_RETRY_MAX_SECONDS)

    async def _cleanup_stale(
        self,
        endpoint: EndpointAddress,
        session_id: str,
    ) -> None:
        await self._view.wait_current()
        snapshot = await self._view.snapshot(
            participant=endpoint,
            state=ContractState.OPEN,
        )
        key = str(endpoint)
        async with self._active_lock:
            current = set(self._active[key])
            self._released[key].intersection_update(
                item.contract.pointer for item in snapshot.contracts
            )
            released = set(self._released[key])
        for state in snapshot.contracts:
            pointer = state.contract.pointer
            if state.record.created_by != endpoint or pointer in current:
                continue
            # No local session assertions: the other runtime may legitimately
            # hold a different session, including while the contract is pending.
            validity = await self._store.validate_exact(state.contract)
            if validity.status == ContractValidityStatus.UNAVAILABLE:
                raise ConcordUnavailable(
                    ConcordUnavailableCode.STORE_UNAVAILABLE,
                    "Owner bootstrap exact state unavailable",
                )
            proven_loss = validity.status in {
                ContractValidityStatus.MISSING_TOKEN,
                ContractValidityStatus.INVALID_TOKEN,
                ContractValidityStatus.GENERATION_MISMATCH,
                ContractValidityStatus.SESSION_MISMATCH,
                ContractValidityStatus.TERMS_HASH_MISMATCH,
            }
            if pointer in released or proven_loss:
                await self._cancel_until_resolved(pointer, endpoint)
                async with self._active_lock:
                    self._released[key].discard(pointer)


__all__ = ["ConcordOwnerBootstrap"]
