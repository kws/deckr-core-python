"""In-process message delivery with real public Beacon/Concord lifecycles."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import anyio
from descriptor_fixtures import stream_deck_bitmap_grid
from message_bus_mocks import MockSubscriptionContext, mock_deckr

from deckr.hardware import (
    CapabilityRef,
    DeviceDescriptor,
    DeviceRef,
    HardwareClaimSpec,
    messages as hw,
)
from deckr.lanes import reply_is_accepted


def descriptor(device_id="device-a"):
    payload = stream_deck_bitmap_grid()
    payload["deviceId"] = device_id
    payload["controls"][0]["outputCapabilities"][0]["access"].append("readable")
    payload["controls"][0]["outputCapabilities"][0]["valueSchema"] = {
        "schemaId": "test.bitmap.state.v1",
        "schema": {
            "type": "object",
            "properties": {"lit": {"type": "boolean"}},
            "required": ["lit"],
            "additionalProperties": False,
        },
    }
    return DeviceDescriptor.model_validate(payload)


def wired_deckr():
    runtime = mock_deckr()
    bus = runtime._message_bus
    routes = []
    replies = {}

    def subscribe(lane, endpoint, *, endpoint_session_id):
        context = MockSubscriptionContext()
        routes.append((lane, endpoint, endpoint_session_id, context))
        bus.subscriptions.append(context)
        return context

    async def publish(message):
        for lane, endpoint, session_id, context in tuple(routes):
            if (
                context.entered
                and not context.exited
                and message.lane == lane
                and getattr(message.recipient, "endpoint", None) == endpoint
                and message.recipient_session_id in {None, session_id}
            ):
                await context._send.send(message)

    async def publish_reply(message, *, request):
        send = replies.get(request.message_id)
        if send is not None:
            await send.send(message)

    async def request(message, *, timeout, accept):
        send, receive = anyio.create_memory_object_stream(16)
        replies[message.message_id] = send
        try:
            async with send, receive:
                with anyio.fail_after(timeout):
                    await bus.publish(message)
                    async for reply in receive:
                        if await reply_is_accepted(
                            reply, request=message, accept=accept
                        ):
                            return reply
        finally:
            replies.pop(message.message_id, None)

    bus.subscribe.side_effect = subscribe
    bus.publish.side_effect = publish
    bus.publish_reply.side_effect = publish_reply
    bus.request.side_effect = request
    return runtime


@asynccontextmanager
async def hardware_setup(
    *, count=1, event_buffer_size=64, reset_handler=None, labels=None
):
    async with (
        wired_deckr() as runtime,
        runtime.endpoint("hardware_manager:manager-a") as manager_endpoint,
        runtime.endpoint("controller:controller-a") as endpoint,
    ):
        devices = {
            f"device-{index}": descriptor(f"device-{index}") for index in range(count)
        }
        async with (
            runtime.hardware_manager(
                manager_endpoint,
                manager_id="manager-a",
                devices=devices,
                reset_handler=reset_handler,
                labels=labels,
            ) as manager,
            runtime.hardware(endpoint) as client,
        ):
            candidates = tuple(
                [
                    await client.wait_for_device(
                        lambda c, device_id=device_id: c.device_ref.device_id
                        == device_id,
                        timeout_seconds=2,
                    )
                    for device_id in devices
                ]
            )
            yield SimpleNamespace(
                runtime=runtime,
                manager=manager,
                manager_endpoint=manager_endpoint,
                client=client,
                endpoint=endpoint,
                devices=devices,
                spec=HardwareClaimSpec(
                    candidates,
                    timeout_seconds=2,
                    event_buffer_size=event_buffer_size,
                ),
            )


async def eventually(predicate):
    with anyio.fail_after(2):
        while not predicate():
            await anyio.sleep(0)


async def interrupt_store_watch(store, monkeypatch):
    resume = anyio.Event()
    original = store.watch

    @asynccontextmanager
    async def paused_watch(*args, **kwargs):
        await resume.wait()
        async with original(*args, **kwargs) as changes:
            yield changes

    monkeypatch.setattr(store, "watch", paused_watch)
    for send in tuple(store._watchers):
        await send.aclose()
    return resume


def input_message(h, claim, *, sequence=0, event_type="down", value=None):
    ref = CapabilityRef(
        deviceRef=DeviceRef(
            managerId="manager-a",
            deviceId="device-0",
            fingerprint=h.devices["device-0"].fingerprint,
        ),
        controlId="key.0.0",
        capabilityId="button.momentary",
    )
    return hw.hardware_message(
        sender=h.manager_endpoint.address,
        sender_session_id=h.manager_endpoint.session_id,
        recipient=h.endpoint.address,
        recipient_session_id=h.endpoint.session_id,
        message_type=hw.CONTROL_INPUT,
        contract=claim.state.contract,
        subject=hw.hardware_subject_for_capability(ref),
        body=hw.ControlInputMessage(
            deviceRef=ref.device_ref,
            controlId=ref.control_id,
            capabilityId=ref.capability_id,
            eventType=event_type,
            sequence=sequence,
            value=value or {"eventType": event_type},
        ),
    )
