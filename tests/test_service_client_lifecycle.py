"""Client-owned source cleanup required by the messaging lifecycle boundary."""

import anyio
import pytest
from message_bus_mocks import mock_deckr
from test_service_protocol_example import EXAMPLE_PROTOCOL

from deckr.concord import ConcordUnavailable, ConcordUnavailableCode
from deckr.services import ServiceUnavailable, ServiceViewRef, service_view_key


@pytest.mark.asyncio
async def test_service_client_exit_closes_materialized_view_watchers():
    store_name = "org_example_presence_status_v1"
    async with (
        mock_deckr() as runtime,
        runtime.endpoint("service:presence-main") as provider_endpoint,
        runtime.endpoint("controller:controller-a") as endpoint,
        runtime.service_provider(
            provider_endpoint, EXAMPLE_PROTOCOL, service_id="presence-main"
        ) as provider,
    ):
        for _ in range(3):
            async with (
                runtime.services(endpoint) as services,
                services.use(provider.descriptor, timeout_seconds=2) as service,
            ):
                view = ServiceViewRef(
                    store_name=store_name,
                    key=service_view_key("presence-main", "status", "current"),
                )
                assert await service.read_view(view) is None
                bucket = runtime._message_bus.kv_buckets[store_name]
                assert len(bucket._watchers) == 1
            with anyio.fail_after(2):
                while bucket._watchers:
                    await anyio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_phase", ["recovery", "shutdown", "pending"])
async def test_recovery_cleanup_is_diagnostic_but_current_shutdown_reports_failure(
    monkeypatch, failure_phase
):
    async with (
        mock_deckr() as runtime,
        runtime.endpoint("service:presence-main") as provider_endpoint,
        runtime.endpoint("controller:controller-a") as endpoint,
        runtime.service_provider(
            provider_endpoint, EXAMPLE_PROTOCOL, service_id="presence-main"
        ),
        runtime.services(endpoint) as services,
    ):
        context = services.use_matching(EXAMPLE_PROTOCOL, timeout_seconds=2)
        service = await context.__aenter__()
        old = service._lease
        store = old.agreement._store
        withdraw = store.withdraw
        failed_contract = old.contract.contract_id
        entered, release = anyio.Event(), anyio.Event()

        async def fail_selected_token(handle):
            if (
                handle.contract_id == failed_contract
                and handle.participant == endpoint.address
            ):
                entered.set()
                if failure_phase == "pending":
                    await release.wait()
                raise ConcordUnavailable(
                    ConcordUnavailableCode.STORE_UNAVAILABLE,
                    "Injected token cleanup failure",
                )
            return await withdraw(handle)

        monkeypatch.setattr(store, "withdraw", fail_selected_token)
        await old.agreement.cancel("test recovery")
        if failure_phase == "pending":
            with anyio.fail_after(2):
                await entered.wait()

                async def close():
                    with pytest.raises(
                        ServiceUnavailable, match="Injected token cleanup"
                    ):
                        await context.__aexit__(None, None, None)

                async with anyio.create_task_group() as tasks:
                    tasks.start_soon(close)
                    while not service._close_started:
                        await anyio.sleep(0)
                    release.set()
            assert service.state.cleanup_failures == 1
            return
        with anyio.fail_after(2):
            while (
                not service.state.local_authority
                or service._lease is old
                or service.state.cleanup_failures == 0
            ):
                await anyio.sleep(0)
        fresh = service._lease
        assert fresh.contract.contract_id != old.contract.contract_id
        assert not old.agreement.state.local_authority
        assert service.state.cleanup_failures == 1
        assert "Injected token cleanup" in service.state.last_cleanup_failure.message
        fail_shutdown = failure_phase == "shutdown"
        failed_contract = fresh.contract.contract_id if fail_shutdown else None
        if fail_shutdown:
            with pytest.raises(ServiceUnavailable, match="Injected token cleanup"):
                await context.__aexit__(None, None, None)
        else:
            await context.__aexit__(None, None, None)
        assert service.state.cleanup_failures == (2 if fail_shutdown else 1)
        assert not fresh.agreement.state.local_authority
