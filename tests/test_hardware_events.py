import anyio
import pytest
from hardware_client_helpers import hardware_setup, input_message

from deckr.contracts.messages import endpoint_target, hardware_manager_address
from deckr.hardware import HardwareUnavailable

pytestmark = pytest.mark.asyncio


async def test_ordered_inputs_are_never_coalesced():
    async with (
        hardware_setup() as h,
        h.client.claim(h.spec) as claim,
        claim.events() as events,
    ):
        for index, kind in enumerate(["down", "up", "down", "up"]):
            await h.runtime._message_bus.publish(
                input_message(h, claim, sequence=index, event_type=kind)
            )
        with anyio.fail_after(2):
            received = [await anext(events) for _ in range(4)]
        assert [event.body.sequence for event in received] == list(range(4))
        assert [event.body.event_type for event in received] == [
            "down",
            "up",
            "down",
            "up",
        ]


async def test_invalid_inputs_are_discarded_without_hiding_next_valid_event():
    async with (
        hardware_setup() as h,
        h.client.claim(h.spec) as claim,
        claim.events() as events,
    ):
        valid = input_message(h, claim, sequence=99)
        invalid = input_message(h, claim, sequence=1)
        for message in [
            invalid.model_copy(update={"sender_session_id": "old-session"}),
            invalid.model_copy(update={"sender": hardware_manager_address("other")}),
            invalid.model_copy(update={"recipient_session_id": "old-controller"}),
            invalid.model_copy(
                update={"recipient": endpoint_target("controller:other")}
            ),
            invalid.model_copy(
                update={"contract": valid.contract.model_copy(update={"generation": 2})}
            ),
            invalid.model_copy(
                update={
                    "subject": valid.subject.model_copy(
                        update={"identifiers": {"deviceId": "other"}}
                    )
                }
            ),
            input_message(h, claim, value={"unexpected": True}),
            input_message(h, claim, event_type="down", value={"eventType": "up"}),
        ]:
            # Deliver through the endpoint subscription to test the inner boundary,
            # including messages a real transport would normally filter already.
            await h.runtime._message_bus.subscriptions[-1]._send.send(message)
        await h.runtime._message_bus.publish(valid)
        with anyio.fail_after(2):
            assert (await anext(events)).body.sequence == 99
        assert claim.state.local_authority


async def test_input_overflow_revokes_claim_and_fails_consumer_explicitly():
    async with (
        hardware_setup(event_buffer_size=1) as h,
        h.client.claim(h.spec) as claim,
        claim.events() as events,
    ):
        await h.runtime._message_bus.publish(input_message(h, claim, sequence=1))
        await h.runtime._message_bus.publish(input_message(h, claim, sequence=2))
        state = await claim.wait_for_state(
            lambda s: not s.local_authority, timeout_seconds=2
        )
        assert state.failure.code == "input_overflow"
        with pytest.raises(HardwareUnavailable, match="fell behind"):
            await anext(events)
        await h.manager.wait_for_state(lambda s: not s.claims, timeout_seconds=2)


async def test_idle_consumer_wakes_on_claim_loss():
    async with (
        hardware_setup() as h,
        h.client.claim(h.spec) as claim,
        claim.events() as events,
    ):
        ended = anyio.Event()

        async def consume():
            with pytest.raises(HardwareUnavailable):
                await anext(events)
            ended.set()

        async with anyio.create_task_group() as group:
            group.start_soon(consume)
            await h.manager.remove_device("device-0")
            with anyio.fail_after(2):
                await ended.wait()


async def test_event_consumer_is_exclusive_and_reopening_discards_previous_backlog():
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:
        async with claim.events():
            with pytest.raises(RuntimeError, match="one ordered"):
                async with claim.events():
                    pass
            await h.runtime._message_bus.publish(input_message(h, claim, sequence=1))
            with anyio.fail_after(2):
                while claim._receive.statistics().current_buffer_used == 0:
                    await anyio.sleep(0)
        async with claim.events() as events:
            await h.runtime._message_bus.publish(input_message(h, claim, sequence=2))
            assert (await anext(events)).body.sequence == 2


async def test_request_does_not_return_success_after_authority_loss():
    from deckr.hardware import messages as hw

    async with hardware_setup() as h, h.client.claim(h.spec) as claim:

        async def request(message, **_kwargs):
            body = hw.hardware_body_from_message(message)
            await claim.aclose()
            return hw.hardware_message(
                sender=h.manager_endpoint.address,
                sender_session_id=h.manager_endpoint.session_id,
                recipient=h.endpoint.address,
                recipient_session_id=h.endpoint.session_id,
                message_type=hw.CAPABILITY_STATE_REPLY,
                subject=message.subject,
                contract=message.contract,
                body=hw.CapabilityStateReplyMessage(
                    deviceRef=body.device_ref,
                    controlId=body.control_id,
                    capabilityId=body.capability_id,
                    value={"lit": True},
                ),
            )

        h.runtime._message_bus.request.side_effect = request
        with pytest.raises(HardwareUnavailable):
            await claim.request_state("device-0", "raster.bitmap", control_id="key.0.0")


async def test_closed_event_iterator_cannot_consume_from_a_new_listener():
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:
        async with claim.events() as old:
            pass
        async with claim.events() as current:
            await h.runtime._message_bus.publish(input_message(h, claim, sequence=2))
            with pytest.raises((anyio.ClosedResourceError, StopAsyncIteration)):
                await anext(old)
            assert (await anext(current)).body.sequence == 2


async def test_expiry_is_checked_again_at_event_delivery(monkeypatch):
    from deckr.contracts.messages import message_is_expired

    async with (
        hardware_setup() as h,
        h.client.claim(h.spec) as claim,
        claim.events() as events,
    ):
        expires_in_queue = input_message(h, claim, sequence=1)
        await h.runtime._message_bus.publish(expires_in_queue)
        with anyio.fail_after(2):
            while claim._receive.statistics().current_buffer_used == 0:
                await anyio.sleep(0)
        monkeypatch.setattr(
            "deckr.hardware.managed_claim.message_is_expired",
            lambda message: message.message_id == expires_in_queue.message_id
            or message_is_expired(message),
        )
        await h.runtime._message_bus.publish(input_message(h, claim, sequence=2))
        assert (await anext(events)).body.sequence == 2


async def test_command_does_not_report_success_after_authority_loss():
    async with hardware_setup() as h, h.client.claim(h.spec) as claim:

        async def publish(_message):
            await claim.aclose()

        h.runtime._message_bus.publish.side_effect = publish
        with pytest.raises(HardwareUnavailable):
            await claim.command(
                "device-0", "raster.bitmap", "clear", control_id="key.0.0"
            )
