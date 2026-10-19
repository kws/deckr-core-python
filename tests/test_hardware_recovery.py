from dataclasses import replace

import anyio
import pytest
from hardware_client_helpers import hardware_setup

from deckr.hardware import HardwarePhase, HardwareUnavailable, same_hardware

pytestmark = pytest.mark.asyncio


async def test_recovery_is_opt_in_and_old_claim_never_regains_authority():
    async with hardware_setup() as h, h.client.use(h.spec) as use:
        old = use.claim
        await old.aclose()
        state = await use.wait_for_state(
            lambda s: s.phase == HardwarePhase.UNAVAILABLE, timeout_seconds=2
        )
        assert not state.local_authority
        with pytest.raises(HardwareUnavailable):
            _ = use.claim


async def test_opt_in_recovery_creates_fresh_generation_one_without_replay():
    async with (
        hardware_setup() as h,
        h.client.use(h.spec, recover=same_hardware) as use,
    ):
        old = use.claim
        first = old.state.contract
        await use.command("device-0", "raster.bitmap", "clear", control_id="key.0.0")
        calls = h.runtime._message_bus.publish.call_count
        await old.aclose()
        successor = await use.wait_for_state(
            lambda s: s.local_authority and s.contract != first, timeout_seconds=2
        )
        assert successor.contract.generation == 1
        assert successor.contract.contract_id != first.contract_id
        assert not old.state.local_authority
        assert h.runtime._message_bus.publish.call_count == calls


async def test_same_hardware_policy_never_substitutes_or_chooses_ambiguous_session():
    async with hardware_setup() as h:
        candidate = h.spec.candidates[0]
        assert same_hardware(h.spec, ()) is None
        assert (
            same_hardware(
                h.spec, (candidate, replace(candidate, manager_session_id="other"))
            )
            is None
        )
        changed = candidate.descriptor.model_copy(update={"fingerprint": "replacement"})
        assert same_hardware(h.spec, (replace(candidate, descriptor=changed),)) is None
        successor = replace(candidate, manager_session_id="next-session")
        assert same_hardware(h.spec, (successor,)).candidates == (successor,)


async def test_material_device_change_waits_for_policy_selection():
    async with (
        hardware_setup() as h,
        h.client.use(h.spec, recover=same_hardware) as use,
    ):
        original = use.claim
        replacement = h.devices["device-0"].model_copy(
            update={"fingerprint": "replacement"}
        )
        await h.manager.set_device(replacement)
        await use.wait_for_state(
            lambda s: s.phase == HardwarePhase.RECONNECTING, timeout_seconds=2
        )
        await h.client.wait_for_device(
            lambda c: c.descriptor.fingerprint == "replacement", timeout_seconds=2
        )
        assert not use.state.local_authority
        assert not original.state.local_authority
        await h.manager.set_device(h.devices["device-0"])
        await use.wait_for_state(lambda s: s.local_authority, timeout_seconds=2)


async def test_bad_recovery_policy_fails_only_its_use_context():
    def bad_policy(_previous, _candidates):
        raise ValueError("invalid user selection")

    async with hardware_setup() as h, h.client.use(h.spec, recover=bad_policy) as use:
        await use.claim.aclose()
        state = await use.wait_for_state(
            lambda s: s.failure is not None
            and s.failure.code == "recovery_policy_failed",
            timeout_seconds=2,
        )
        assert state.phase == HardwarePhase.UNAVAILABLE
        assert h.runtime.is_running


async def test_closing_client_ends_pending_discovery_without_closing_shared_directory():
    async with hardware_setup() as h, h.runtime.hardware(h.endpoint) as other:
        done = anyio.Event()

        async def waiting():
            with pytest.raises(anyio.ClosedResourceError):
                await other.wait_for_device(lambda _candidate: False)
            done.set()

        async with anyio.create_task_group() as group:
            group.start_soon(waiting)
            await anyio.sleep(0)
            await other.aclose()
            with anyio.fail_after(2):
                await done.wait()
        assert h.client.directory.state.current
