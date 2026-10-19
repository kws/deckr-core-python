"""Managed hardware boundaries from messaging lifecycle and SEM-HW-003/005."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import anyio
import pytest
from hardware_client_helpers import descriptor, hardware_setup, wired_deckr

from deckr.hardware import (
    HARDWARE_FEATURE_ID,
    CapabilityRef,
    DeviceRef,
    HardwareClaimSpec,
    messages as hw,
)

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "control_id,capability_id,command_type,params",
    [
        ("absent", "raster.bitmap", "clear", {}),
        ("key.0.0", "absent", "clear", {}),
        ("key.0.0", "button.momentary", "clear", {}),
        ("key.0.0", "raster.bitmap", "unknown", {}),
        ("key.0.0", "raster.bitmap", "set_frame", {}),
    ],
)
async def test_manager_rejects_commands_outside_descriptor_before_driver_dispatch(
    control_id,
    capability_id,
    command_type,
    params,
):
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:
        await h.manager.wait_for_state(lambda s: bool(s.claims), timeout_seconds=2)
        handler = h.manager.command_handler = AsyncMock(return_value=True)
        ref = CapabilityRef(
            deviceRef=DeviceRef(managerId="manager-a", deviceId="device-0"),
            controlId=control_id,
            capabilityId=capability_id,
        )
        body = hw.ControlCommandMessage(
            deviceRef=ref.device_ref,
            controlId=control_id,
            capabilityId=capability_id,
            commandType=command_type,
            params=params,
        )
        # Use the wire entry point so client-side checks cannot mask a missing
        # receiver check. The rejected reply proves the message was processed.
        reply = await h.endpoint.request(
            lane="hardware_messages",
            recipient=h.manager_endpoint.address,
            recipient_session_id=h.manager_endpoint.session_id,
            contract=claim.state.contract,
            subject=hw.hardware_subject_for_capability(ref),
            message_type=hw.CONTROL_COMMAND,
            body=hw.hardware_body_to_dict(body),
            timeout=0.5,
        )
        rejected = hw.hardware_body_from_message(reply)
        assert isinstance(rejected, hw.CommandRejectedMessage)
        assert rejected.command_type == command_type
        handler.assert_not_awaited()


async def test_device_removal_revokes_grouped_claim_before_storage_cleanup(monkeypatch):
    async with hardware_setup(count=2) as h, h.client.claim(h.spec) as claim:
        await h.manager.wait_for_state(lambda s: bool(s.claims), timeout_seconds=2)
        handler = h.manager.command_handler = AsyncMock(return_value=True)
        blocked, resume = anyio.Event(), anyio.Event()
        cancel = h.manager._claim_manager._store.cancel

        async def delayed_cancel(*args, **kwargs):
            blocked.set()
            await resume.wait()
            return await cancel(*args, **kwargs)

        monkeypatch.setattr(h.manager._claim_manager._store, "cancel", delayed_cancel)
        async with anyio.create_task_group() as group:
            group.start_soon(h.manager.remove_device, "device-0")
            try:
                with anyio.fail_after(2):
                    await blocked.wait()
                    state = await h.manager.wait_for_state(lambda s: not s.claims)
                assert "device-0" not in state.inventory
                ref = CapabilityRef(
                    deviceRef=DeviceRef(managerId="manager-a", deviceId="device-1"),
                    controlId="key.0.0",
                    capabilityId="raster.bitmap",
                )
                reply = await h.endpoint.request(
                    lane="hardware_messages",
                    recipient=h.manager_endpoint.address,
                    recipient_session_id=h.manager_endpoint.session_id,
                    contract=claim.state.contract,
                    subject=hw.hardware_subject_for_capability(ref),
                    message_type=hw.CONTROL_COMMAND,
                    body=hw.hardware_body_to_dict(
                        hw.ControlCommandMessage(
                            deviceRef=ref.device_ref,
                            controlId=ref.control_id,
                            capabilityId=ref.capability_id,
                            commandType="clear",
                        )
                    ),
                    timeout=0.5,
                )
                assert hw.hardware_body_from_message(reply).reason == "unauthorized"
                handler.assert_not_awaited()
            finally:
                resume.set()
        await claim.wait_for_state(lambda s: not s.local_authority, timeout_seconds=2)


async def test_manager_is_not_discoverable_before_command_subscription_is_ready(
    monkeypatch,
):
    async with (
        wired_deckr() as runtime,
        runtime.endpoint("hardware_manager:manager-a") as manager_endpoint,
        runtime.endpoint("controller:controller-a") as endpoint,
        runtime.hardware(endpoint) as client,
    ):
        subscribe = manager_endpoint.subscribe
        blocked, resume, started = anyio.Event(), anyio.Event(), anyio.Event()

        @asynccontextmanager
        async def delayed_subscribe(*args, **kwargs):
            blocked.set()
            await resume.wait()
            async with subscribe(*args, **kwargs) as messages:
                yield messages

        monkeypatch.setattr(manager_endpoint, "subscribe", delayed_subscribe)
        received = anyio.Event()

        async def handle(_message):
            received.set()
            return True

        context = runtime.hardware_manager(
            manager_endpoint,
            manager_id="manager-a",
            devices={"device-0": descriptor("device-0")},
            command_handler=handle,
        )

        async def start():
            await context.__aenter__()
            started.set()

        try:
            async with anyio.create_task_group() as group:
                group.start_soon(start)
                try:
                    with anyio.fail_after(2):
                        await blocked.wait()
                    assert not runtime.beacon.candidates(HARDWARE_FEATURE_ID)
                finally:
                    resume.set()
                with anyio.fail_after(2):
                    await started.wait()
                candidate = await client.wait_for_device(
                    lambda c: True, timeout_seconds=2
                )
                async with client.claim(HardwareClaimSpec((candidate,))) as claim:
                    await context.runtime.wait_for_state(
                        lambda s: bool(s.claims), timeout_seconds=2
                    )
                    await claim.command(
                        "device-0", "raster.bitmap", "clear", control_id="key.0.0"
                    )
                    with anyio.fail_after(2):
                        await received.wait()
        finally:
            resume.set()
            await context.aclose()
