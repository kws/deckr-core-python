"""Service extensions must preserve authority throughout a retained-view stream."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import anyio
import pytest

from deckr.concord import ContractRecord, canonical_json_hash
from deckr.contracts.authority import ContractPointer
from deckr.contracts.messages import controller_address, service_address
from deckr.services.consumer_views import ServiceConsumerViews
from deckr.services.errors import ServiceUnavailable
from deckr.services.managed_views import (
    _LeaseManagedServiceViewAccess,
    _ServiceManagedServiceViewAccess,
)
from deckr.services.profile import is_service_use_record
from deckr.services.protocol import ServiceViewFamily, ServiceViewRef, ServiceViewWriter
from deckr.services.view_context import ServiceViewReadContext, ServiceViewWriteContext
from deckr.services.view_store import ServiceViewStore
from deckr.testing import MemoryJsonKvBucket


@pytest.mark.parametrize(
    "extra",
    [
        {"terms": None},
        {"termsHash": None},
        {"terms": {}, "termsHash": canonical_json_hash({})},
        {"termsHash": "hash"},
    ],
)
def test_service_use_rejects_present_terms_even_when_null(extra):
    record = ContractRecord.from_wire(
        {
            "contractId": "contract",
            "generation": 1,
            "attachedParticipants": [],
            "profile": "example.use.v1",
            "participants": ["controller:main", "service:example"],
            **extra,
        }
    )
    assert not is_service_use_record(
        record, profile="example.use.v1", service=service_address("example")
    )


@pytest.mark.parametrize(
    "participants,profile,expected",
    [
        (["controller:main", "service:example"], "example.use.v1", True),
        (
            ["controller:main", "controller:other", "service:example"],
            "example.use.v1",
            False,
        ),
        (["controller:main"], "example.use.v1", False),
        (["controller:main", "service:example"], "other.use.v1", False),
    ],
)
def test_service_use_profile_requires_the_exact_two_party_shape(
    participants, profile, expected
):
    record = ContractRecord.from_wire(
        {
            "contractId": "contract",
            "generation": 1,
            "attachedParticipants": [],
            "profile": profile,
            "participants": participants,
        }
    )
    assert (
        is_service_use_record(
            record, profile="example.use.v1", service=service_address("example")
        )
        is expected
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("side", ["consumer", "provider"])
@pytest.mark.parametrize("idle", [True, False])
async def test_managed_view_checks_authority_on_delivery_and_idle_revocation(
    side, idle
):
    revoked = anyio.Event()
    waiting = anyio.Event()
    context = SimpleNamespace(view_read_context=lambda: object())

    async def check():
        if revoked.is_set():
            raise ServiceUnavailable("contract_local_authority_lost", "revoked")
        return context

    @asynccontextmanager
    async def watch_state():
        async def states():
            yield object()
            await revoked.wait()
            yield object()
            await anyio.sleep_forever()

        yield states()

    @asynccontextmanager
    async def watch_data(_context, _view, *, require_authority=None):
        async def values():
            yield "initial"
            waiting.set()
            if idle:
                await anyio.sleep_forever()
            else:
                revoked.set()
                yield "must not be delivered"

        yield values()

    store = SimpleNamespace(watch=watch_data)
    if side == "consumer":
        access = _LeaseManagedServiceViewAccess(
            store,
            lease=SimpleNamespace(
                require_authority=check,
                agreement=SimpleNamespace(watch_state=watch_state),
            ),
            read_context=object(),
        )
    else:
        access = _ServiceManagedServiceViewAccess(
            store,
            contract=SimpleNamespace(
                _context=check, _participant=SimpleNamespace(watch_state=watch_state)
            ),
        )
    async with access.watch(ServiceViewRef("views", "item")) as changes:
        assert await anext(changes) == "initial"
        async with anyio.create_task_group() as group:

            async def revoke():
                await waiting.wait()
                revoked.set()

            if idle:
                group.start_soon(revoke)
            with (
                anyio.fail_after(1),
                pytest.raises(ServiceUnavailable, match="revoked"),
            ):
                await anext(changes)


@pytest.mark.asyncio
@pytest.mark.parametrize("side", ["consumer", "consumer_access", "provider"])
@pytest.mark.parametrize("operation", ["read", "create", "put", "update", "delete"])
@pytest.mark.parametrize("revoke", [False, True])
async def test_view_access_rechecks_authority_after_readiness_wait(
    side, operation, revoke, monkeypatch
):
    bucket = MemoryJsonKvBucket(bucket="views")
    store = ServiceViewStore(bucket=bucket)
    view = ServiceViewRef("views", "state.current")
    local_side = (
        ServiceViewWriter.SERVICE if side == "provider" else ServiceViewWriter.CONSUMER
    )
    writer = local_side
    if operation == "read":
        writer = (
            ServiceViewWriter.CONSUMER
            if local_side == ServiceViewWriter.SERVICE
            else ServiceViewWriter.SERVICE
        )
    fields = dict(
        service_id="demo",
        service_namespace="example.demo",
        service_endpoint=service_address("demo"),
        service_session_id="service-session",
        consumer_endpoint=controller_address("main"),
        consumer_session_id="consumer-session",
        contract=ContractPointer(contractId="original-agreement", generation=1),
        views={
            "state": ServiceViewFamily(
                storeName="views", keyPrefix="state.", writer=writer
            )
        },
    )
    write_context = ServiceViewWriteContext(writer=writer, **fields)
    read_context = ServiceViewReadContext(reader=local_side, **fields)
    context = SimpleNamespace(
        view_write_context=lambda: write_context,
        view_read_context=lambda: read_context,
    )
    revoked = False
    checked = anyio.Event()
    waiting = anyio.Event()
    recovered = anyio.Event()

    async def check():
        checked.set()
        if revoked:
            raise ServiceUnavailable("contract_local_authority_lost", "revoked")
        return context

    async with anyio.create_task_group() as group:
        store.start(group)
        await store.wait_current()
        revision = None
        if operation != "create":
            entry = await store.put(
                view=view, payload={"value": "before"}, context=write_context
            )
            revision = entry.revision
        before = await bucket.items()
        mutations = bucket.mutation_count
        wait_current = store.wait_current

        async def delayed_current():
            waiting.set()
            await recovered.wait()
            await wait_current()

        monkeypatch.setattr(store, "wait_current", delayed_current)
        lease = SimpleNamespace(
            require_authority=check,
            contract=fields["contract"],
            descriptor=SimpleNamespace(
                service_id=fields["service_id"],
                namespace=fields["service_namespace"],
                endpoint=fields["service_endpoint"],
                session_id=fields["service_session_id"],
                views=fields["views"],
            ),
        )
        if side == "provider":
            access = _ServiceManagedServiceViewAccess(
                store, contract=SimpleNamespace(_context=check)
            )
        elif side == "consumer_access":
            access = _LeaseManagedServiceViewAccess(
                store,
                lease=lease,
                read_context=read_context,
                write_context=write_context,
            )
        else:
            access = ServiceConsumerViews(
                SimpleNamespace(
                    address=fields["consumer_endpoint"],
                    session_id=fields["consumer_session_id"],
                ),
                group,
                lambda _policy: bucket,
                {"views": store},
            )

        async def recover():
            nonlocal revoked
            await waiting.wait()
            assert checked.is_set()
            revoked = revoke
            recovered.set()

        async def operate():
            args = (view,) if side != "consumer" else (lease, view)
            name = operation if side != "consumer" else f"_{operation}_view"
            kwargs = {"revision": revision} if operation in {"update", "delete"} else {}
            if operation in {"create", "put", "update"}:
                args = (*args, {"value": "after"})
            return await getattr(access, name)(*args, **kwargs)

        group.start_soon(recover)
        with anyio.fail_after(1):
            if revoke:
                with pytest.raises(ServiceUnavailable, match="revoked"):
                    await operate()
                assert bucket.mutation_count == mutations
                assert await bucket.items() == before
            else:
                result = await operate()
                if operation == "read":
                    assert (
                        result["value"] if side == "consumer" else result.value["value"]
                    ) == "before"
                else:
                    assert bucket.mutation_count == mutations + 1
        await store.aclose()
        group.cancel_scope.cancel()
