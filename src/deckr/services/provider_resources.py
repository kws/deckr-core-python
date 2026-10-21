"""Provider-owned retained resource scopes and ordered protected projections."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Collection, Hashable, Mapping
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

import anyio

from deckr.contracts.authority import ContractPointer
from deckr.core.kv import KvConflict, KvUnavailable
from deckr.core.util.operations import operation_guard
from deckr.core.util.owned_tasks import OwnedTaskGroup
from deckr.services.errors import ServiceUnavailable, ServiceUseAuthorizationError
from deckr.services.protocol import ServiceViewRef
from deckr.services.provider import ServiceProvider
from deckr.services.provider_requests import ServiceRequestContext
from deckr.services.view_context import ServiceViewWriteContext
from deckr.services.view_store import ServiceViewStore

R = TypeVar("R", bound=Hashable)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ScopeKey:
    contract: ContractPointer
    endpoint: str
    session: str
    subscriber: str


@dataclass(eq=False)
class _Scope(Generic[R]):
    resources: frozenset[R]
    context: ServiceViewWriteContext
    tokens: dict[R, object]


@dataclass
class _Deletion:
    context: ServiceViewWriteContext
    revision: int | None


class ServiceResourceRegistry(Generic[R]):
    """Own resource scopes, revocation, publication ordering and cleanup retries.

    Backend acquisition belongs in ``on_change``. It observes the current union
    through ``resources`` and must reconcile rather than depend on every event.
    It runs outside request contexts. Missing backend resources do not alter
    service agreement authority.
    """

    def __init__(
        self,
        provider: ServiceProvider,
        store: ServiceViewStore,
        *,
        view_for_resource: Callable[[R], ServiceViewRef],
        on_change: Callable[[], Awaitable[None]],
        retry_seconds: float = 0.25,
        max_scopes: int = 256,
        max_resources_per_scope: int = 1024,
    ) -> None:
        if max_scopes < 1 or max_resources_per_scope < 1 or retry_seconds <= 0:
            raise ValueError("Resource registry bounds must be positive")
        self._max_scopes = max_scopes
        self._max_resources_per_scope = max_resources_per_scope
        self.provider = provider
        self.store = store
        self._view = view_for_resource
        self._on_change = on_change
        self._retry_seconds = retry_seconds
        self._scopes: dict[_ScopeKey, _Scope[R]] = {}
        self._revisions: dict[tuple[ContractPointer, R], int] = {}
        self._deletions: dict[tuple[ContractPointer, R], _Deletion] = {}
        self._retirements: dict[ContractPointer, Any] = {}
        self._retired: set[ContractPointer] = set()
        self._lock = anyio.Lock()
        self._changed = anyio.Event()
        self._backend_dirty = False
        self._closed = False
        self._tasks: OwnedTaskGroup | None = None

    @property
    def resources(self) -> frozenset[R]:
        return frozenset(
            resource
            for key, scope in self._scopes.items()
            if key.contract not in self._retired
            for resource in scope.resources
        )

    @property
    def cleanup_pending(self) -> bool:
        return bool(self._deletions or self._retirements or self._backend_dirty)

    async def start(self, task_group: anyio.abc.TaskGroup) -> None:
        self._tasks = OwnedTaskGroup(
            task_group, not_started_message="Resource registry is not started"
        )
        with operation_guard(None):
            tasks = await self._tasks.start()
            tasks.start_soon(self._watch_authority)
            tasks.start_soon(self._maintenance)

    def _key(self, request: ServiceRequestContext, subscriber: str) -> _ScopeKey:
        if not subscriber or subscriber.strip() != subscriber:
            raise ValueError("subscriberId must be a non-empty trimmed string")
        return _ScopeKey(
            request.authorization.view_write_context.contract,
            str(request.message.sender),
            str(request.message.sender_session_id),
            subscriber,
        )

    def _check_open(self) -> None:
        if self._closed:
            raise ServiceUseAuthorizationError(
                "service_use_closed", "Resource registry is closed"
            )

    async def replace(
        self,
        request: ServiceRequestContext,
        subscriber: str,
        resources: Collection[R],
    ) -> None:
        key = self._key(request, subscriber)
        selected = frozenset(resources)
        if len(selected) > self._max_resources_per_scope:
            raise ServiceUnavailable(
                "resource_scope_capacity", "Too many resources in scope"
            )
        mutated = False
        try:
            await request.authorize()
            async with self._lock:
                self._check_open()
                request.check()
                if key.contract in self._retired:
                    raise ServiceUseAuthorizationError(
                        "contract_local_authority_lost", "Scope agreement is retired"
                    )
                previous = self._scopes.get(key)
                if (
                    previous is None
                    and selected
                    and len(self._scopes) >= self._max_scopes
                ):
                    raise ServiceUnavailable(
                        "resource_scope_capacity", "Too many subscriber scopes"
                    )
                scope = _Scope(
                    selected,
                    request.authorization.view_write_context,
                    {
                        resource: previous.tokens[resource]
                        if previous is not None and resource in previous.tokens
                        else object()
                        for resource in selected
                    },
                )
                if selected:
                    self._scopes[key] = scope
                else:
                    self._scopes.pop(key, None)
                mutated = True
                if previous is not None:
                    for resource in previous.resources - scope.resources:
                        if not self._retained(key.contract, resource):
                            self._queue_delete(previous.context, resource)
                for resource in scope.resources:
                    self._deletions.pop((key.contract, resource), None)
                self._notify()
                await self._delete_pending()
                if any(
                    pointer == key.contract for pointer, _resource in self._deletions
                ):
                    raise ServiceUnavailable(
                        "scope_cleanup_pending", "Scope removal could not complete"
                    )
                request.check()
        except BaseException:
            if mutated:
                # Revoke local scopes before attempting fallible remote cleanup.
                self._retired.add(key.contract)
                self._retirements[key.contract] = request.authorization
                self._notify()
                with anyio.move_on_after(1, shield=True):
                    await self._retire_pending()
                    await self.drop_contract(key.contract)
            raise

    async def require_resource(
        self,
        request: ServiceRequestContext,
        subscriber: str,
        resource: R,
    ) -> None:
        key = self._key(request, subscriber)
        await request.authorize()
        async with self._lock:
            scope = self._scopes.get(key)
            token = scope.tokens.get(resource) if scope is not None else None

            def check() -> None:
                self._check_open()
                current = self._scopes.get(key)
                if (
                    key.contract in self._retired
                    or token is None
                    or current is None
                    or current.tokens.get(resource) is not token
                ):
                    raise ServiceUseAuthorizationError(
                        "resource_not_in_scope",
                        "Resource is no longer in this subscriber scope",
                        {"resource": str(resource), "subscriberId": subscriber},
                    )

            request.restrict(check)

    async def publish(
        self,
        resource: R,
        payload: Mapping[str, Any],
        *,
        is_current: Callable[[], bool] | None = None,
    ) -> None:
        async def check_source(pointer: ContractPointer) -> None:
            if (
                self._closed
                or pointer in self._retired
                or not self._retained(pointer, resource)
            ):
                raise ServiceUnavailable(
                    "resource_scope_retired", "Projection scope is no longer current"
                )
            if is_current is not None and not is_current():
                raise ServiceUnavailable(
                    "resource_source_changed", "Projection source is no longer current"
                )

        async with self._lock:
            if self._closed:
                return
            for pointer in self._contexts(resource):
                contract = self.provider.contract(pointer)
                if contract is None:
                    continue

                async def check_projection(pointer=pointer) -> None:
                    await check_source(pointer)

                try:
                    entry = await contract.view_access(
                        self.store, require_current=check_projection
                    ).put(self._view(resource), payload)
                except ServiceUnavailable:
                    continue
                self._revisions[pointer, resource] = entry.revision
                self._deletions.pop((pointer, resource), None)

    async def invalidate(self, resource: R) -> None:
        """Remove a backend projection while keeping the desired scope."""
        async with self._lock:
            for context in self._contexts(resource).values():
                self._queue_delete(context, resource)
            self._changed.set()
            await self._delete_pending()

    def _contexts(self, resource: R) -> dict[ContractPointer, ServiceViewWriteContext]:
        return {
            key.contract: scope.context
            for key, scope in self._scopes.items()
            if key.contract not in self._retired and resource in scope.resources
        }

    def _retained(self, pointer: ContractPointer, resource: R) -> bool:
        return any(
            key.contract == pointer and resource in scope.resources
            for key, scope in self._scopes.items()
        )

    def _queue_delete(self, context: ServiceViewWriteContext, resource: R) -> None:
        key = context.contract, resource
        self._deletions[key] = _Deletion(context, self._revisions.get(key))

    def _notify(self) -> None:
        self._backend_dirty = True
        self._changed.set()

    async def drop_contract(self, pointer: ContractPointer) -> None:
        self._retired.add(pointer)
        async with self._lock:
            for key, scope in tuple(self._scopes.items()):
                if key.contract == pointer:
                    del self._scopes[key]
                    for resource in scope.resources:
                        self._queue_delete(scope.context, resource)
            self._notify()

    async def reconcile(self) -> None:
        """Compare owned scopes with current authority, including unseen removals."""
        for pointer in {key.contract for key in self._scopes}:
            if pointer in self._retired or self.provider.contract(pointer) is None:
                await self.drop_contract(pointer)
        for pointer in tuple(self._retired):
            if (
                pointer not in self._retirements
                and self.provider.contract(pointer) is None
            ):
                self._retired.discard(pointer)

    async def _watch_authority(self) -> None:
        async with self.provider.watch_state() as states:
            async for _state in states:
                await self.reconcile()

    async def _delete_pending(self) -> None:
        # Caller holds the publication/scope lock. Exact cleanup is allowed after
        # revocation, but uses the owned fence and last observed write revision.
        for key, deletion in tuple(self._deletions.items()):
            try:
                with anyio.move_on_after(0.5) as timeout:
                    await self.store.delete(
                        view=self._view(key[1]),
                        context=deletion.context,
                        revision=deletion.revision,
                    )
                if timeout.cancel_called:
                    continue
            except KvConflict:
                # A different revision is not ours to remove.
                pass
            except KvUnavailable:
                continue
            except Exception:
                logger.warning("Service view cleanup will retry", exc_info=True)
                continue
            self._deletions.pop(key, None)
            self._revisions.pop(key, None)

    async def _retire_pending(self) -> None:
        for pointer, authorization in tuple(self._retirements.items()):
            try:
                await self.provider.cancel_authorized(
                    authorization, reason="service_scope_mutation_failed"
                )
            except Exception:
                logger.warning(
                    "Service scope termination remains pending", exc_info=True
                )
            else:
                self._retirements.pop(pointer, None)

    async def _maintenance(self) -> None:
        while True:
            if not self.cleanup_pending:
                await self._changed.wait()
            self._changed = anyio.Event()
            await self.reconcile()
            if self._backend_dirty:
                self._backend_dirty = False
                try:
                    await self._on_change()
                except Exception:
                    self._backend_dirty = True
                    logger.warning(
                        "Service resource reconciliation will retry", exc_info=True
                    )
            with anyio.move_on_after(1):
                await self._retire_pending()
            async with self._lock:
                await self._delete_pending()
            await anyio.sleep(self._retry_seconds)

    async def aclose(self) -> None:
        self._closed = True
        for pointer in {key.contract for key in self._scopes}:
            await self.drop_contract(pointer)
        if self._tasks is not None:
            await self._tasks.aclose()
        with anyio.move_on_after(2, shield=True):
            try:
                await self._on_change()
            except Exception:
                logger.warning("Backend cleanup failed", exc_info=True)
            await self._retire_pending()
            async with self._lock:
                await self._delete_pending()
        if self._deletions or self._retirements:
            raise ServiceUnavailable(
                "service_cleanup_incomplete", "Resource cleanup did not complete"
            )
