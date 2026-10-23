"""A confirmed process exit permits successor recovery without waiting for TTL."""

import json
import time
from dataclasses import dataclass

import anyio
import pytest

from deckr.core.identity import EndpointAddress
from deckr.core.restart import VerifiedRestart, load_restart_handoff
from deckr.testing.concord import ConcordRuntimeHarness

PROVIDER = EndpointAddress("service:restart")
CLIENT = EndpointAddress("controller:restart")


def handoff(session="dead"):
    return dict(
        schema="dev.deckr.runtime.restart_handoff.v1",
        instanceId="provider",
        predecessors=[
            dict(
                predecessorId="container:process-start",
                exitCode=137,
                participants=[dict(endpoint=str(PROVIDER), sessionId=session)],
            )
        ],
    )


@dataclass(frozen=True)
class Session:
    address: EndpointAddress = PROVIDER
    session_id: str = "successor"


async def contract(h, name, session, *, owned=False, attached=True):
    c = h.concord
    result = await c._create_contract(
        (CLIENT, PROVIDER),
        contract_id=name,
        created_by=PROVIDER if owned else CLIENT,
        participant_sessions={CLIENT: "client", PROVIDER: session},
    )
    await c._attach(result, CLIENT, "client")
    token = await c._attach(result, PROVIDER, session) if attached else None
    return result, token


@pytest.mark.asyncio
async def test_verified_restart_cleans_peer_created_and_owned_before_expiry_preserving_live_sessions():
    h = ConcordRuntimeHarness(
        restart=VerifiedRestart.from_handoff(handoff(), instance_id="provider")
    )
    dead, dead_token = await contract(h, "dead", "dead")
    owned, _ = await contract(h, "owned", "dead", owned=True)
    live, _ = await contract(h, "competing", "live")
    fresh, _ = await contract(h, "successor", "successor")
    pending, _ = await contract(h, "pending-reference", "dead", attached=False)
    await h.materialize()
    assert (await h.concord.validate_exact(dead)).valid
    start = time.monotonic()
    with anyio.fail_after(2):
        await h.concord.bootstrap_owner(Session())
    assert time.monotonic() - start < 2 < 120
    assert await h.token_entry(dead_token) is not None, "cleanup precedes token expiry"
    for item in (dead, owned):
        assert (await h.concord.validate_exact(item)).status == "cancelled"
    for item in (live, fresh):
        assert (await h.concord.validate_exact(item)).valid
    assert (await h.concord.validate_exact(pending)).status == "not_yet_fulfilled"
    # Replayed proof cannot cancel the successor or a competing live instance.
    await h.materialize()
    await h.concord.bootstrap_owner(Session(session_id="next-successor"))
    assert (await h.concord.validate_exact(fresh)).valid
    assert (await h.concord.validate_exact(live)).valid


@pytest.mark.asyncio
@pytest.mark.parametrize("proof", [None, "unrelated"])
async def test_absence_or_wrong_session_is_not_exit_evidence(proof):
    h = ConcordRuntimeHarness(
        restart=VerifiedRestart.from_handoff(handoff(proof), instance_id="provider")
        if proof
        else None
    )
    old, _ = await contract(h, "old", "dead")
    await h.materialize()
    await h.concord.bootstrap_owner(Session())
    assert (await h.concord.validate_exact(old)).valid


@pytest.mark.asyncio
async def test_failed_cleanup_cannot_report_readiness_or_reuse_predecessor_session(
    monkeypatch,
):
    h = ConcordRuntimeHarness(
        restart=VerifiedRestart.from_handoff(handoff(), instance_id="provider")
    )
    old, _ = await contract(h, "old", "dead")
    await h.materialize()
    with pytest.raises(ValueError, match="fresh session"):
        await h.concord.bootstrap_owner(Session(session_id="dead"))
    entered = anyio.Event()

    async def blocked(*args, **kwargs):
        entered.set()
        await anyio.sleep_forever()

    monkeypatch.setattr(h.concord._repository, "cancel_pointer", blocked)
    with anyio.move_on_after(0.05) as scope:
        await h.concord.bootstrap_owner(Session())
        pytest.fail("unconfirmed cleanup allowed readiness")
    assert scope.cancel_called and entered.is_set()
    assert (await h.concord.validate_exact(old)).valid


def test_handoff_rejects_wrong_instance_unknown_fields_duplicate_identities_and_invalid_exit(
    tmp_path,
):
    invalid_predecessors = [
        dict(
            handoff()["predecessors"][0],
            participants=handoff()["predecessors"][0]["participants"] * 2,
        ),
        dict(handoff()["predecessors"][0], exitCode=True),
        dict(handoff()["predecessors"][0], exitCode=2**31),
    ]
    for value in [
        dict(handoff(), instanceId="other"),
        dict(handoff(), untrusted=True),
        *[dict(handoff(), predecessors=[item]) for item in invalid_predecessors],
    ]:
        with pytest.raises(ValueError):
            VerifiedRestart.from_handoff(value, instance_id="provider")
    path = tmp_path / "handoff.json"
    assert load_restart_handoff(path, instance_id="provider") is None
    path.write_text(json.dumps(handoff()))
    assert load_restart_handoff(path, instance_id="provider").contains(PROVIDER, "dead")
    path.write_text('{"schema":1,"schema":2}')
    with pytest.raises(ValueError, match="duplicate"):
        load_restart_handoff(path, instance_id="provider")


@pytest.mark.asyncio
async def test_runtime_endpoint_readiness_waits_for_confirmed_cleanup(monkeypatch):
    from types import SimpleNamespace

    from deckr.contracts.lanes import DEFAULT_MESSAGE_CONTRACT_REGISTRY
    from deckr.runtime import Deckr
    from deckr.testing.runtime import mock_message_bus

    runtime = Deckr(
        message_bus=mock_message_bus(DEFAULT_MESSAGE_CONTRACT_REGISTRY),
        restart=VerifiedRestart.from_handoff(handoff(), instance_id="provider"),
    )
    async with runtime:
        old, _ = await contract(
            SimpleNamespace(concord=runtime.concord), "predecessor", "dead"
        )
        entered, release, ready = anyio.Event(), anyio.Event(), anyio.Event()
        cancel = runtime.concord._repository.cancel_pointer

        async def gated(*args, **kwargs):
            entered.set()
            await release.wait()
            return await cancel(*args, **kwargs)

        monkeypatch.setattr(runtime.concord._repository, "cancel_pointer", gated)

        async def start():
            async with runtime.endpoint(PROVIDER) as endpoint:
                assert endpoint.session_id != "dead"
                assert (await runtime.concord.validate_exact(old)).status == "cancelled"
                ready.set()

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(start)
            with anyio.fail_after(2):
                await entered.wait()
                assert not ready.is_set()
                release.set()
                await ready.wait()
        assert runtime.endpoint_sessions == ()


@pytest.mark.asyncio
async def test_discovery_cleanup_preserves_other_sessions_and_replay_is_harmless():
    from deckr._beacon._models import beacon_advertisement_key
    from deckr.beacon import Beacon
    from deckr.testing import MemoryJsonKvBucket

    bucket = MemoryJsonKvBucket(bucket="beacon", ttl_seconds=300)
    keys = {}
    for session in ("dead", "live", "successor"):
        key = beacon_advertisement_key(
            feature_id="example.restart.v1", advertisement_id=session
        )
        keys[session] = key
        await bucket.put(
            key,
            dict(
                advertisementId=session,
                featureId="example.restart.v1",
                endpoint=str(PROVIDER),
                advertiser=str(PROVIDER),
                sessionId=session,
                refreshSeq=1,
                ttlSeconds=300,
            ),
        )
    beacon = Beacon(bucket)
    proof = VerifiedRestart.from_handoff(handoff(), instance_id="provider")
    async with anyio.create_task_group() as tasks:
        beacon.start(tasks)
        await beacon.wait_current()
        await beacon.cleanup_restart(proof, PROVIDER)
        await beacon.cleanup_restart(proof, PROVIDER)
        assert await bucket.get(keys["dead"]) is None
        assert await bucket.get(keys["live"]) is not None
        assert await bucket.get(keys["successor"]) is not None
        await beacon.aclose()
        tasks.cancel_scope.cancel()


def test_retained_exit_history_covers_both_predecessors_but_not_successor():
    value = handoff()
    previous = handoff("older")["predecessors"][0]
    previous["predecessorId"] = "container:older"
    value["predecessors"].append(previous)
    proof = VerifiedRestart.from_handoff(value, instance_id="provider")
    assert proof.contains(PROVIDER, "dead") and proof.contains(PROVIDER, "older")
    assert not proof.contains(PROVIDER, "successor")
