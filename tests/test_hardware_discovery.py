from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from hardware_client_helpers import eventually, hardware_setup, interrupt_store_watch

from deckr.beacon import BEACON_ADVERTISEMENT_STORE_POLICY, BeaconAdvertisementSpec
from deckr.concord import CONCORD_TOKEN_BUCKET_POLICY
from deckr.core.kv import KvUnavailable
from deckr.hardware import (
    HARDWARE_FEATURE_ID,
    HardwareClaimSpec,
    HardwareUnavailable,
    same_hardware,
)

pytestmark = pytest.mark.asyncio


async def test_stale_discovery_preserves_existing_claim_and_recovers(monkeypatch):
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:
        bucket = h.runtime._message_bus.kv_buckets[
            BEACON_ADVERTISEMENT_STORE_POLICY.bucket
        ]
        resume = await interrupt_store_watch(bucket, monkeypatch)
        try:
            await eventually(lambda: not h.client.directory.state.current)
            assert h.client.directory.state.candidates
            assert claim.state.local_authority
            assert h.manager.state.authority_current
            assert not h.manager.state.discovery_current
            with pytest.raises(HardwareUnavailable, match="not current"):
                h.client.directory.candidates()
            with pytest.raises(HardwareUnavailable, match="not current"):
                async with h.client.claim(h.spec):
                    pass
            await claim.command(
                "device-0", "raster.bitmap", "clear", control_id="key.0.0"
            )
        finally:
            resume.set()
        await h.client.wait_for_device(lambda _candidate: True, timeout_seconds=2)
        assert h.client.directory.state.current


async def test_authority_source_loss_is_permanent_until_successor(monkeypatch):
    async with (
        hardware_setup() as h,
        h.client.use(h.spec, recover=same_hardware) as use,
    ):
        old = use.claim
        pointer = old.state.contract
        bucket = h.runtime._message_bus.kv_buckets[CONCORD_TOKEN_BUCKET_POLICY.bucket]
        resume = await interrupt_store_watch(bucket, monkeypatch)
        try:
            await old.wait_for_state(lambda s: not s.local_authority, timeout_seconds=2)
            assert h.client.directory.state.current
            with pytest.raises(HardwareUnavailable):
                await old.command(
                    "device-0", "raster.bitmap", "clear", control_id="key.0.0"
                )
        finally:
            resume.set()
        successor = await use.wait_for_state(
            lambda s: s.local_authority and s.contract != pointer, timeout_seconds=3
        )
        assert successor.contract.generation == 1
        assert not old.state.local_authority


async def test_empty_current_discovery_differs_from_stale_and_invalid_ad_is_ignored():
    async with (
        hardware_setup() as h,
        h.runtime.beacon.advertise(
            h.manager_endpoint,
            BeaconAdvertisementSpec(
                feature_id=HARDWARE_FEATURE_ID, payload={"invalid": True}
            ),
        ),
    ):
        await h.manager.remove_device("device-0")
        await eventually(
            lambda: h.client.directory.state.current
            and not h.client.directory.state.candidates
        )
        assert h.client.directory.candidates() == ()
        with pytest.raises(TimeoutError):
            await h.client.wait_for_device(
                lambda _candidate: True, timeout_seconds=0.02
            )


async def test_old_candidate_is_rejected_after_descriptor_change():
    async with hardware_setup() as h:
        descriptor = h.devices["device-0"].model_copy(
            update={"fingerprint": "new-device"}
        )
        await h.manager.set_device(descriptor)
        await h.client.wait_for_device(
            lambda c: c.descriptor.fingerprint == "new-device", timeout_seconds=2
        )
        with pytest.raises(HardwareUnavailable, match="no longer current"):
            async with h.client.claim(h.spec):
                pass


async def test_publication_failure_is_separate_from_authority(monkeypatch):
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:
        await h.manager.wait_for_state(lambda s: bool(s.claims), timeout_seconds=2)
        monkeypatch.setattr(
            h.manager._advertiser,
            "update",
            AsyncMock(side_effect=KvUnavailable("offline")),
        )
        await h.manager.set_device(
            h.devices["device-0"].model_copy(update={"display_name": "Renamed"})
        )
        state = h.manager.state
        assert state.failure.code == "discovery_unavailable"
        assert not state.advertisement_published
        assert state.authority_current and state.claims
        assert claim.state.local_authority


async def test_directory_shutdown_ends_pending_wait_with_a_final_state():
    async with hardware_setup() as h:
        async with h.client.directory.watch_state() as states:
            assert (await anext(states)).current
            await h.client.directory.aclose()
            assert [state async for state in states][-1].closed
        with pytest.raises(HardwareUnavailable, match="closed"):
            await h.client.wait_for_device(lambda _candidate: True)


@pytest.mark.parametrize(
    "field,value",
    [
        ("timeout_seconds", 0),
        ("timeout_seconds", float("inf")),
        ("event_buffer_size", 0),
        ("event_buffer_size", 4097),
        ("event_buffer_size", 1.5),
    ],
)
async def test_claim_spec_rejects_unbounded_configuration(field, value):
    async with hardware_setup() as h:
        with pytest.raises(ValueError):
            replace(h.spec, **{field: value})


async def test_claim_spec_enforces_one_session_and_unique_devices():
    async with hardware_setup() as h:
        candidate = h.spec.candidates[0]
        for candidates in [
            (),
            (candidate, candidate),
            (candidate, replace(candidate, manager_session_id="different")),
        ]:
            with pytest.raises(ValueError):
                HardwareClaimSpec(candidates)
