"""Client-owned source cleanup required by the messaging lifecycle boundary."""

import anyio
import pytest
from message_bus_mocks import mock_deckr
from test_service_protocol_example import EXAMPLE_PROTOCOL

from deckr.services import ServiceViewRef, service_view_key


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
