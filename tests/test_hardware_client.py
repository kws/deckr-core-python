from dataclasses import FrozenInstanceError, replace

import anyio
import pytest
from hardware_client_helpers import eventually, hardware_setup

from deckr.hardware import HardwarePhase, HardwareUnavailable, messages as hw

pytestmark = pytest.mark.asyncio


async def test_public_claim_groups_devices_and_closes_its_authority():
    async with hardware_setup(count=2) as h:
        async with h.client.claim(h.spec) as claim:
            state = claim.state
            assert state.local_authority and state.phase == HardwarePhase.READY
            assert state.contract.generation == 1
            assert set(state.descriptors) == set(h.devices)
            with pytest.raises(TypeError):
                state.descriptors["bad"] = next(iter(h.devices.values()))
            with pytest.raises(FrozenInstanceError):
                state.local_authority = False
            await h.manager.wait_for_state(
                lambda s: len(s.claims) == 1, timeout_seconds=2
            )
            assert set(h.manager.state.claims[0].device_ids) == set(h.devices)
        assert not claim.state.local_authority
        assert claim.state.phase == HardwarePhase.CLOSED
        await h.manager.wait_for_state(lambda s: not s.claims, timeout_seconds=2)
        with pytest.raises(HardwareUnavailable):
            await claim.command(
                "device-0", "raster.bitmap", "clear", control_id="key.0.0"
            )


async def test_directory_and_endpoint_router_are_shared_until_last_client_closes():
    async with hardware_setup() as h:
        bus = h.runtime._message_bus
        calls = bus.subscribe.call_count
        async with h.runtime.hardware(h.endpoint) as other:
            assert other.directory is h.client.directory
            assert bus.subscribe.call_count == calls
            await h.client.aclose()
            async with other.claim(h.spec) as claim:
                assert claim.state.local_authority
            assert other.directory.state.current
        assert all(context.exited for context in bus.subscriptions[1:])


async def test_command_validates_scope_and_schema_before_publishing():
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:
        message = await claim.command(
            "device-0", "raster.bitmap", "clear", control_id="key.0.0"
        )
        assert message.contract == claim.state.contract
        assert message.recipient_session_id == h.manager_endpoint.session_id
        assert (
            hw.hardware_body_from_message(message).device_ref.fingerprint
            == h.devices["device-0"].fingerprint
        )
        calls = h.runtime._message_bus.publish.call_count
        for device_id, capability_id, command_type, control_id, params in [
            ("wrong", "raster.bitmap", "clear", "key.0.0", {}),
            ("device-0", "wrong", "clear", "key.0.0", {}),
            ("device-0", "raster.bitmap", "unknown", "key.0.0", {}),
            ("device-0", "raster.bitmap", "clear", "wrong", {}),
            ("device-0", "raster.bitmap", "set_frame", "key.0.0", {"width": "bad"}),
        ]:
            with pytest.raises(HardwareUnavailable):
                await claim.command(
                    device_id,
                    capability_id,
                    command_type,
                    control_id=control_id,
                    params=params,
                )
        assert h.runtime._message_bus.publish.call_count == calls


async def test_state_reply_is_typed_and_checked_against_descriptor():
    async with hardware_setup() as h:

        async def handle(message):
            body = hw.hardware_body_from_message(message)
            reply = hw.CapabilityStateReplyMessage(
                deviceRef=body.device_ref,
                controlId=body.control_id,
                capabilityId=body.capability_id,
                stateType=body.state_type,
                value={"lit": True},
            )
            await h.manager_endpoint.reply_to(
                message,
                message_type=hw.CAPABILITY_STATE_REPLY,
                body=hw.hardware_body_to_dict(reply),
            )
            return True

        h.manager.command_handler = handle
        async with h.client.claim(h.spec) as claim:
            await h.manager.wait_for_state(lambda s: bool(s.claims), timeout_seconds=2)
            reply = await claim.request_state(
                "device-0", "raster.bitmap", control_id="key.0.0", state_type="bitmap"
            )
            assert reply.value == {"lit": True}


async def test_competing_claim_times_out_without_disturbing_current_claim():
    async with hardware_setup() as h, h.client.claim(h.spec) as winner:
        with pytest.raises((TimeoutError, HardwareUnavailable)):
            async with h.client.claim(replace(h.spec, timeout_seconds=0.1)):
                pytest.fail("exclusive device admitted twice")
        assert winner.state.local_authority


async def test_manager_watch_has_initial_and_final_immutable_state():
    async with hardware_setup() as h:
        async with h.manager.watch_state() as states:
            state = await anext(states)
            assert state.phase == HardwarePhase.READY
            assert state.discovery_current and state.authority_current
            assert state.advertisement_published
            with pytest.raises(TypeError):
                state.inventory["bad"] = h.devices["device-0"]
            await h.manager.stop()
            final = [state async for state in states][-1]
            assert final.phase == HardwarePhase.CLOSED
            assert not final.claims and not final.authority_current
        async with h.manager.watch_state() as states:
            assert [state.phase async for state in states] == [HardwarePhase.CLOSED]


async def test_claim_loss_is_observable_before_driver_reset_completes():
    reset_started, allow_reset = anyio.Event(), anyio.Event()

    async def reset(_device_id):
        reset_started.set()
        await allow_reset.wait()

    async with (
        hardware_setup(reset_handler=reset) as h,
        h.client.claim(h.spec) as claim,
    ):
        await h.manager.wait_for_state(lambda s: bool(s.claims), timeout_seconds=2)
        await claim.aclose()
        with anyio.fail_after(2):
            await reset_started.wait()
            assert not h.manager.state.claims
            state = await h.manager.wait_for_state(lambda s: not s.claims)
            assert not state.claims
        allow_reset.set()


async def test_client_close_cancels_pending_negotiation():
    async with (
        hardware_setup() as h,
        h.client.claim(h.spec),
        h.runtime.hardware(h.endpoint) as other,
    ):
        finished = anyio.Event()

        async def negotiate():
            try:
                async with other.claim(h.spec):
                    pytest.fail("competing claim unexpectedly won")
            except (anyio.ClosedResourceError, HardwareUnavailable):
                pass
            finally:
                finished.set()

        async with anyio.create_task_group() as group:
            group.start_soon(negotiate)
            await eventually(lambda: bool(other._claims))
            await other.aclose()
            with anyio.fail_after(2):
                await finished.wait()
