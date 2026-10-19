"""Protocol authority must not depend on discovery or local ownership guesses."""

from contextlib import asynccontextmanager
from dataclasses import dataclass

import anyio
import pytest
from pydantic import ValidationError

from deckr.beacon import AdvertisementRecord, Beacon, parse_beacon_advertisement_key
from deckr.concord import (
    Concord,
    ConcordAgreementSpec,
    ContractRecord,
    ParticipantTokenRecord,
)
from deckr.core.identity import EndpointAddress
from deckr.core.kv import KvViewStatus
from deckr.testing import MemoryJsonKvBucket


@dataclass(frozen=True)
class Session:
    address: EndpointAddress
    session_id: str


@asynccontextmanager
async def runtimes():
    contracts = MemoryJsonKvBucket(bucket="contracts")
    tokens = MemoryJsonKvBucket(bucket="tokens", ttl_seconds=60)
    first, second = Concord(contracts, tokens), Concord(contracts, tokens)
    async with anyio.create_task_group() as group:
        first.start(group)
        second.start(group)
        await first.wait_current()
        await second.wait_current()
        try:
            yield first, second
        finally:
            await second.aclose()
            await first.aclose()
            group.cancel_scope.cancel()


@pytest.mark.asyncio
@pytest.mark.parametrize("pending", [False, True])
@pytest.mark.parametrize("same_session", [False, True])
async def test_bootstrap_preserves_other_runtime_authority(pending, same_session):
    owner = EndpointAddress("worker:example")
    peer = EndpointAddress("scheduler:main")
    spec = ConcordAgreementSpec(
        profile="example.job.v1", participants=(owner, peer) if pending else (owner,)
    )
    async with (
        runtimes() as (first, second),
        first.propose(Session(owner, "first"), spec) as agreement,
    ):
        if not pending:
            await agreement.wait_for_state(lambda state: state.valid)
        await second.bootstrap_owner(
            Session(owner, "first" if same_session else "second")
        )
        record = await second.contract_record(agreement.contract)
        assert record is not None and record.state == "open"
        assert agreement.state.local_authority


@pytest.mark.asyncio
async def test_coalesced_source_recovery_cannot_restore_authority():
    owner = EndpointAddress("worker:example")
    async with (
        runtimes() as (first, _second),
        first.propose(
            Session(owner, "first"),
            ConcordAgreementSpec(profile="example.job.v1", participants=(owner,)),
        ) as agreement,
    ):
        await agreement.wait_for_state(lambda state: state.valid)
        source = first._repository.token_source._bucket
        # Hold the projection lock across both transitions to force the
        # listener to observe only the recovered snapshot.
        async with first._view._broadcaster.lock:
            await source._set_status(KvViewStatus.STALE)
            await source._set_status(KvViewStatus.READY)
            assert not agreement.state.local_authority
        assert not agreement.state.local_authority


def advertisement():
    return dict(
        advertisementId="a1",
        featureId="f1",
        advertiser="worker:example",
        endpoint="worker:example",
        sessionId="session",
        refreshSeq=1,
        ttlSeconds=30,
    )


@pytest.mark.parametrize(
    "key,value", [("refreshSeq", True), ("ttlSeconds", "30"), ("createdAt", 0)]
)
def test_beacon_wire_ingestion_does_not_coerce_invalid_types(key, value):
    with pytest.raises(ValidationError):
        AdvertisementRecord.from_wire({**advertisement(), key: value})


@pytest.mark.parametrize("value", [True, "1"])
def test_concord_wire_ingestion_does_not_coerce_generations(value):
    with pytest.raises(ValidationError):
        ContractRecord.from_wire(
            dict(
                contractId="a",
                generation=value,
                participants=["worker:example"],
                attachedParticipants=[],
            )
        )
    with pytest.raises(ValidationError):
        ParticipantTokenRecord.from_wire(
            dict(
                contractId="a",
                generation=value,
                participant="worker:example",
                sessionId="s",
                tokenId="t",
                refreshSeq=1,
                ttlSeconds=30,
            )
        )


@pytest.mark.parametrize(
    "key",
    [
        "advertisements.by_feature.b64_a.a1",
        "advertisements.by_feature.b64_ZjE.a1",
        "advertisements.by_feature.b64_!@.a1",
    ],
)
def test_beacon_rejects_malformed_and_noncanonical_keys(key):
    assert parse_beacon_advertisement_key(key) is None


@pytest.mark.asyncio
async def test_malformed_beacon_key_does_not_stop_discovery():
    bucket = MemoryJsonKvBucket(bucket="ads", ttl_seconds=30)
    await bucket.put("advertisements.by_feature.b64_a.a1", advertisement())
    beacon = Beacon(bucket)
    async with anyio.create_task_group() as group:
        beacon.start(group)
        with anyio.fail_after(1):
            await beacon.wait_current()
        assert beacon.is_current()
        await beacon.aclose()
        group.cancel_scope.cancel()


@pytest.mark.asyncio
async def test_source_becomes_stale_before_blocked_watch_cleanup():
    from types import SimpleNamespace

    from deckr.core.kv import KvUnavailable, KvWatchBarrier
    from deckr.core.kv_source import KvMaterializedSource

    fail = anyio.Event()
    cleanup_started = anyio.Event()
    release_cleanup = anyio.Event()
    finished = anyio.Event()

    @asynccontextmanager
    async def watch(_prefix):
        async def changes():
            yield KvWatchBarrier(0)
            await fail.wait()
            raise KvUnavailable("lost consumer")

        try:
            yield changes()
        finally:
            cleanup_started.set()
            await release_cleanup.wait()

    source = KvMaterializedSource(bucket=SimpleNamespace(bucket="test", watch=watch))

    async def run():
        with pytest.raises(KvUnavailable, match="lost consumer"):
            await source._consume_one_watch()
        finished.set()

    async with anyio.create_task_group() as group:
        group.start_soon(run)
        await source.wait_current()
        generation = source.generation
        fail.set()
        await cleanup_started.wait()
        assert not source.is_current()
        assert source.generation > generation
        release_cleanup.set()
        await finished.wait()


@pytest.mark.asyncio
async def test_owner_bootstrap_unavailable_is_not_permission_to_cancel(monkeypatch):
    from unittest.mock import AsyncMock

    from deckr.concord import (
        ConcordUnavailable,
        ContractValidity,
        ContractValidityStatus,
    )

    owner = EndpointAddress("worker:example")
    async with (
        runtimes() as (first, second),
        first.propose(
            Session(owner, "first"),
            ConcordAgreementSpec(profile="example.job.v1", participants=(owner,)),
        ) as agreement,
    ):
        await agreement.wait_for_state(lambda state: state.valid)
        monkeypatch.setattr(
            second._repository,
            "validate_exact",
            AsyncMock(
                return_value=ContractValidity(ContractValidityStatus.UNAVAILABLE)
            ),
        )
        with pytest.raises(ConcordUnavailable):
            await second.bootstrap_owner(Session(owner, "second"))
        assert (await first.contract_record(agreement.contract)).state == "open"
        # A release from a non-owner must not manufacture local-loss evidence.
        await second._owner_bootstrap.release(owner, agreement.contract.pointer)
        assert (
            agreement.contract.pointer
            not in second._owner_bootstrap._released[str(owner)]
        )


@pytest.mark.asyncio
async def test_attachment_cannot_span_a_coalesced_source_recovery(monkeypatch):
    from deckr._concord._lease import ConcordParticipantLease
    from deckr.concord import ConcordConflict, ConcordConflictCode

    owner = EndpointAddress("worker:example")
    async with runtimes() as (first, _second):
        contract = await first._create_contract((owner,), profile="example.job.v1")
        lease = ConcordParticipantLease(
            first._repository,
            first._view,
            contract=contract,
            participant=owner,
            session_id="session",
        )
        original = first._repository.prepare_attach

        async def prepare(*args, **kwargs):
            token = await original(*args, **kwargs)
            source = first._repository.token_source._bucket
            async with first._view._broadcaster.lock:
                await source._set_status(KvViewStatus.STALE)
                await source._set_status(KvViewStatus.READY)
            return token

        monkeypatch.setattr(first._repository, "prepare_attach", prepare)
        with pytest.raises(ConcordConflict) as caught:
            await lease.attach()
        assert caught.value.code is ConcordConflictCode.LEASE_CLOSED
        assert lease.token is None and lease.owned_token is None
        assert not (await first.contract_record(contract)).attached_participants
        await lease.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("accept", [True, False])
async def test_other_session_participant_preserves_unattached_pending_proposal(accept):
    from deckr.concord import ConcordParticipantPolicy, ConcordParticipantSelection

    owner, participant = (
        EndpointAddress("worker:example"),
        EndpointAddress("scheduler:main"),
    )
    async with (
        runtimes() as (first, second),
        first.propose(
            Session(owner, "owner"),
            ConcordAgreementSpec(
                profile="example.job.v1",
                participants=(owner, participant),
                participant_sessions={str(participant): "intended-session"},
            ),
        ) as agreement,
    ):

        def select(candidates):
            return ConcordParticipantSelection(
                [c.contract.pointer for c in candidates] if accept else []
            )

        async with second.participant(
            Session(participant, "other-session"),
            ConcordParticipantPolicy(profile="example.job.v1", select=select),
        ) as context:
            assert not context.managed_contracts
            assert (await first.contract_record(agreement.contract)).state == "open"
            assert agreement.state.local_authority


def test_concord_wire_does_not_normalize_session_identity():
    with pytest.raises(ValidationError):
        ContractRecord.from_wire(
            {
                "contractId": "a",
                "generation": 1,
                "participants": ["worker:example"],
                "attachedParticipants": [],
                "participantSessions": {"worker:example": " padded "},
            }
        )


@pytest.mark.parametrize("value", ["2026-10-19T12:00:00", "2026-10-19"])
def test_beacon_wire_dates_require_timezone(value):
    with pytest.raises(ValidationError):
        AdvertisementRecord.from_wire({**advertisement(), "createdAt": value})


@pytest.mark.parametrize(
    "extra", [{"createdAt": "2026-10-19T12:00:00"}, {"cancelRevision": -1}]
)
def test_contract_wire_dates_and_revisions_are_constrained(extra):
    with pytest.raises(ValidationError):
        ContractRecord.from_wire(
            {
                "contractId": "contract",
                "generation": 1,
                "participants": ["worker:example"],
                "attachedParticipants": [],
                **extra,
            }
        )
