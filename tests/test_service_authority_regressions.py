"""Service extensions must preserve authority throughout a retained-view stream."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import anyio
import pytest

from deckr.concord import ContractRecord, canonical_json_hash
from deckr.contracts.messages import service_address
from deckr.services.errors import ServiceUnavailable
from deckr.services.managed_views import (
    _LeaseManagedServiceViewAccess,
    _ServiceManagedServiceViewAccess,
)
from deckr.services.profile import is_service_use_record
from deckr.services.protocol import ServiceViewRef


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
    async def watch_data(_context, _view):
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
