from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import anyio
import pytest

from deckr.concord import ConcordAgreementSpec
from deckr.contracts.messages import endpoint_address, entity_subject, service_address
from deckr.core.kv import KvUnavailable
from deckr.core.util.operations import guarded_run_sync
from deckr.services import (
    ServiceMessageBody,
    ServiceMessageDefinition,
    ServiceOperationDefinition,
    ServiceProtocol,
    ServiceRequestContext,
    ServiceResourceRegistry,
    ServiceUseAuthorizationError,
    ServiceViewFamilyDefinition,
    ServiceViewRef,
    ServiceViewStore,
    service_message,
    service_view_key,
)
from deckr.testing import MemoryJsonKvBucket
from deckr.testing.runtime import mock_deckr

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def resources(*, result_schema=None):
    protocol = ServiceProtocol(
        namespace="example.resources",
        feature_id="example.resources",
        advertisement_profile="example.resources.advertisement.v1",
        use_profile="example.resources.use.v1",
        operations={"set": ServiceOperationDefinition()},
        messages={
            "set": ServiceMessageDefinition(
                operation="set",
                resultSchema=result_schema,
                intent="command",
                exchangePattern="request_reply",
                direction="consumer_to_service",
            )
        },
        view_families={
            "items": ServiceViewFamilyDefinition(storeName="views", writer="service")
        },
    )
    deckr = mock_deckr()
    service = service_address("resources")
    consumer = endpoint_address("action_provider", "test")
    async with (
        deckr,
        deckr.endpoint(service, session_id="service-session") as endpoint,
        deckr.endpoint(consumer, session_id="consumer-session") as client,
        deckr.service_provider(endpoint, protocol, service_id="resources") as provider,
        deckr.concord.propose(
            client,
            ConcordAgreementSpec(
                profile=protocol.use_profile,
                participants=(service, consumer),
                participant_sessions={str(service): endpoint.session_id},
            ),
        ) as agreement,
    ):
        await agreement.wait_for_state(lambda state: state.valid, timeout_seconds=2)
        message = service_message(
            sender=consumer,
            sender_session_id=client.session_id,
            recipient=service,
            recipient_session_id=endpoint.session_id,
            subject=entity_subject(
                "service",
                serviceId="resources",
                namespace=protocol.namespace,
                name="set",
            ),
            body=ServiceMessageBody(
                serviceNamespace=protocol.namespace,
                name="set",
                intent="command",
                exchangePattern="request_reply",
            ),
            contract=agreement.contract.pointer,
        )
        authorization = await provider.authorize(message)
        bucket = MemoryJsonKvBucket(bucket="views")
        store = ServiceViewStore(bucket=bucket)
        registry = ServiceResourceRegistry(
            provider,
            store,
            view_for_resource=lambda item: ServiceViewRef(
                "views", service_view_key("resources", "items", item)
            ),
            on_change=AsyncMock(),
            retry_seconds=0.001,
        )
        async with anyio.create_task_group() as tasks:
            store.start(tasks)
            await store.wait_current()
            try:
                yield (
                    provider,
                    registry,
                    bucket,
                    lambda: ServiceRequestContext(provider, message, authorization),
                    tasks,
                )
            finally:
                await registry.aclose()
                await store.aclose()
                tasks.cancel_scope.cancel()


async def test_deferred_command_stops_on_real_concord_cancellation():
    async with resources() as (provider, registry, _bucket, context, tasks):
        request = context()
        await registry.replace(request, "sub", ["Lamp"])
        await registry.require_resource(request, "sub", "Lamp")
        entered = anyio.Event()
        done = anyio.Event()
        effects = []

        async def handler(request):
            entered.set()
            await anyio.sleep_forever()
            effects.append("sent")

        async def execute():
            with pytest.raises(ServiceUseAuthorizationError):
                await request.execute(handler, timeout_seconds=2)
            done.set()

        tasks.start_soon(execute)
        await entered.wait()
        await provider.cancel_authorized(request.authorization, reason="test")
        with anyio.fail_after(2):
            await done.wait()
        assert effects == []


async def test_worker_checks_scope_after_waiting_for_thread_capacity():
    async with resources() as (_provider, registry, _bucket, context, tasks):
        request = context()
        await registry.replace(request, "sub", ["Lamp"])
        await registry.require_resource(request, "sub", "Lamp")
        limiter = anyio.CapacityLimiter(1)
        await limiter.acquire()
        started, done = anyio.Event(), anyio.Event()
        effects = []

        async def handler(request):
            started.set()
            await guarded_run_sync(effects.append, "sent", limiter=limiter)
            return request.reply()

        async def execute():
            with pytest.raises(ServiceUseAuthorizationError, match="scope"):
                await request.execute(handler, timeout_seconds=2)
            done.set()

        tasks.start_soon(execute)
        await started.wait()
        await registry.replace(context(), "sub", [])
        await registry.replace(context(), "sub", ["Lamp"])
        limiter.release()
        with anyio.fail_after(2):
            await done.wait()
        assert effects == []


async def test_scope_clear_orders_after_waiting_publication(monkeypatch):
    async with resources() as (_provider, registry, bucket, context, tasks):
        await registry.replace(context(), "sub", ["Lamp"])
        waiting, release, cleared = anyio.Event(), anyio.Event(), anyio.Event()
        original = registry.store.wait_current

        async def wait_current():
            waiting.set()
            await release.wait()
            await original()

        monkeypatch.setattr(registry.store, "wait_current", wait_current)
        tasks.start_soon(registry.publish, "Lamp", {"state": "ON"})
        await waiting.wait()

        async def clear():
            await registry.replace(context(), "sub", [])
            cleared.set()

        tasks.start_soon(clear)
        await anyio.sleep(0)
        assert not cleared.is_set()
        release.set()
        await cleared.wait()
        assert await bucket.items() == ()
        assert registry.resources == frozenset()


async def test_reconcile_unseen_contract_removal_and_retry_cleanup(monkeypatch):
    async with resources() as (provider, registry, bucket, context, tasks):
        request = context()
        await registry.replace(request, "sub", ["Lamp"])
        await registry.publish("Lamp", {"state": "ON"})
        delete = registry.store.delete
        monkeypatch.setattr(
            registry.store, "delete", AsyncMock(side_effect=KvUnavailable("offline"))
        )
        await provider.cancel_authorized(request.authorization, reason="test")
        await provider.wait_for_state(
            lambda state: not state.contracts, timeout_seconds=2
        )
        await registry.reconcile()
        assert not registry.resources
        assert registry.cleanup_pending
        # Starting after cancellation sees only the current empty snapshot.
        await registry.start(tasks)
        monkeypatch.setattr(registry.store, "delete", delete)
        with anyio.fail_after(2):
            while await bucket.items():
                await anyio.sleep(0.001)


async def test_cancelled_scope_mutation_revokes_and_terminates_agreement(monkeypatch):
    async with resources() as (provider, registry, _bucket, context, tasks):
        request = context()
        await registry.replace(request, "sub", ["Lamp"])
        await registry.publish("Lamp", {"state": "ON"})
        entered, done = anyio.Event(), anyio.Event()
        delete = registry.store.delete

        async def blocked_delete(**kwargs):
            entered.set()
            await anyio.sleep_forever()

        monkeypatch.setattr(registry.store, "delete", blocked_delete)
        scope = anyio.CancelScope()

        async def clear():
            with scope:
                await registry.replace(context(), "sub", [])
            done.set()

        tasks.start_soon(clear)
        await entered.wait()
        scope.cancel()
        with anyio.fail_after(2):
            await done.wait()
        assert not registry.resources
        assert provider.contract(request.authorization.contract) is None
        monkeypatch.setattr(registry.store, "delete", delete)


async def test_revoked_contract_cannot_publish_after_store_wait(monkeypatch):
    async with resources() as (provider, registry, bucket, context, tasks):
        request = context()
        await registry.replace(request, "sub", ["Lamp"])
        entered, release, done = anyio.Event(), anyio.Event(), anyio.Event()
        original = registry.store.wait_current

        async def wait_current():
            entered.set()
            await release.wait()
            await original()

        monkeypatch.setattr(registry.store, "wait_current", wait_current)

        async def publish():
            await registry.publish("Lamp", {"state": "ON"})
            done.set()

        tasks.start_soon(publish)
        await entered.wait()
        await provider.cancel_authorized(request.authorization, reason="test")
        await provider.wait_for_state(
            lambda state: not state.contracts, timeout_seconds=2
        )
        release.set()
        await done.wait()
        assert await bucket.items() == ()


async def test_locally_retired_scope_cannot_publish_while_contract_remains_live(
    monkeypatch,
):
    async with resources() as (provider, registry, bucket, context, tasks):
        request = context()
        pointer = request.authorization.view_write_context.contract
        await registry.replace(request, "sub", ["Lamp"])
        entered, release, published, retired = (
            anyio.Event(),
            anyio.Event(),
            anyio.Event(),
            anyio.Event(),
        )
        original = registry.store.wait_current

        async def wait_current():
            entered.set()
            await release.wait()
            await original()

        async def publish():
            await registry.publish("Lamp", {"state": "ON"})
            published.set()

        async def retire():
            await registry.drop_contract(pointer)
            retired.set()

        monkeypatch.setattr(registry.store, "wait_current", wait_current)
        tasks.start_soon(publish)
        await entered.wait()
        tasks.start_soon(retire)
        with anyio.fail_after(1):
            while pointer not in registry._retired:
                await anyio.sleep(0)
        assert provider.contract(pointer) is not None
        release.set()
        with anyio.fail_after(1):
            await published.wait()
            await retired.wait()
        assert await bucket.items() == ()


async def test_dispatch_backpressure_bounds_inflight_tasks(monkeypatch):
    from types import SimpleNamespace

    from deckr.services.provider_requests import serve_requests

    async with resources() as (provider, registry, _bucket, context, tasks):
        request = context()
        consumed = []
        entered = []
        release, completed = anyio.Event(), anyio.Event()

        async def requests():
            for number in range(20):
                consumed.append(number)
                yield SimpleNamespace(
                    message=request.message,
                    authorization=request.authorization,
                    failure=None,
                )

        async def handler(context):
            entered.append(context)
            await release.wait()
            return context.reply()

        async def serve():
            await serve_requests(provider, handler, max_concurrency=2)
            completed.set()

        monkeypatch.setattr(provider, "requests", requests)
        reply = AsyncMock()
        monkeypatch.setattr(provider.endpoint, "reply_to", reply)
        tasks.start_soon(serve)
        with anyio.fail_after(2):
            while len(entered) < 2:
                await anyio.sleep(0.001)
        await anyio.sleep(0.01)
        assert len(entered) == 2
        assert len(consumed) == 3  # Two workers and one unspawned request.
        release.set()
        with anyio.fail_after(2):
            await completed.wait()
        assert reply.await_count == 20


async def test_request_timeout_closes_context_and_releases_worker():
    async with resources() as (_provider, _registry, _bucket, context, _tasks):
        request = context()

        async def handler(_request):
            await anyio.sleep_forever()

        with pytest.raises(TimeoutError):
            await request.execute(handler, timeout_seconds=0.01)
        with pytest.raises(ServiceUseAuthorizationError, match="active"):
            request.check()


async def test_scope_capacity_rejection_leaves_existing_authority_intact():
    from deckr.services import ServiceUnavailable

    async with resources() as (provider, registry, _bucket, context, _tasks):
        registry._max_scopes = 1
        registry._max_resources_per_scope = 1
        request = context()
        await registry.replace(request, "first", ["Lamp"])
        with pytest.raises(ServiceUnavailable, match="subscriber"):
            await registry.replace(context(), "second", ["Door"])
        with pytest.raises(ServiceUnavailable, match="resources"):
            await registry.replace(context(), "first", ["Lamp", "Door"])
        assert registry.resources == frozenset({"Lamp"})
        assert provider.contract(request.authorization.contract) is not None
        await registry.replace(context(), "first", [])
        await registry.replace(context(), "second", ["Door"])
        assert registry.resources == frozenset({"Door"})


async def test_source_invalidation_is_rechecked_after_store_wait(monkeypatch):
    async with resources() as (_provider, registry, bucket, context, tasks):
        await registry.replace(context(), "sub", ["Lamp"])
        current = True
        entered, release, done = anyio.Event(), anyio.Event(), anyio.Event()
        original = registry.store.wait_current

        async def wait_current():
            entered.set()
            await release.wait()
            await original()

        monkeypatch.setattr(registry.store, "wait_current", wait_current)

        async def publish():
            await registry.publish("Lamp", {"state": "ON"}, is_current=lambda: current)
            done.set()

        tasks.start_soon(publish)
        await entered.wait()
        current = False
        release.set()
        await done.wait()
        assert await bucket.items() == ()


async def test_dispatch_rejects_invalid_handler_result(monkeypatch):
    from types import SimpleNamespace

    from deckr.services import ServicePayloadSchema
    from deckr.services.provider_requests import serve_requests

    schema = ServicePayloadSchema(
        schemaId="example.result",
        schema={
            "type": "object",
            "required": ["count"],
            "properties": {"count": {"type": "integer"}},
        },
    )
    async with resources(result_schema=schema) as (
        provider,
        _registry,
        _bucket,
        context,
        _tasks,
    ):
        request = context()

        async def requests():
            yield SimpleNamespace(
                message=request.message,
                authorization=request.authorization,
                failure=None,
            )

        async def handler(request):
            return request.reply({"count": "invalid"})

        reply = AsyncMock()
        monkeypatch.setattr(provider, "requests", requests)
        monkeypatch.setattr(provider.endpoint, "reply_to", reply)
        await serve_requests(provider, handler)
        body = reply.await_args.kwargs["body"]
        assert body["status"] == "error"
        assert body["error"]["code"] == "invalid_service_reply"


async def test_replacement_preserves_authority_for_continuously_retained_resource():
    async with resources() as (_provider, registry, _bucket, context, _tasks):
        await registry.replace(context(), "sub", ["Lamp"])
        command = context()
        await registry.require_resource(command, "sub", "Lamp")
        await registry.replace(context(), "sub", ["Lamp", "Door"])
        command.check()
        await registry.replace(context(), "sub", ["Lamp"])
        command.check()
        await registry.replace(context(), "sub", [])
        await registry.replace(context(), "sub", ["Lamp"])
        with pytest.raises(ServiceUseAuthorizationError):
            command.check()
