from __future__ import annotations

from collections.abc import Mapping

from deckr.services import (
    ServiceBackendStatus,
    ServiceExchangePattern,
    ServiceMessageDefinition,
    ServiceMessageDirection,
    ServiceMessageIntent,
    ServiceOperationDefinition,
    ServiceProtocol,
    ServiceViewFamilyDefinition,
    ServiceViewWriter,
)

EXAMPLE_PROTOCOL = ServiceProtocol(
    namespace="org.example.presence",
    feature_id="org.example.presence",
    advertisement_profile="org.example.presence.advertisement.v1",
    use_profile="org.example.presence.service_use.v1",
    operations={
        "presence.report": ServiceOperationDefinition(
            description="Report the caller's current presence state",
        ),
    },
    messages={
        "presence.report": ServiceMessageDefinition(
            operation="presence.report",
            intent=ServiceMessageIntent.COMMAND,
            exchangePattern=ServiceExchangePattern.REQUEST_REPLY,
            direction=ServiceMessageDirection.CONSUMER_TO_SERVICE,
        ),
    },
    view_families={
        "status": ServiceViewFamilyDefinition(
            storeName="org_example_presence_status_v1",
            writer=ServiceViewWriter.SERVICE,
        ),
    },
)


def example_advertisement_payload(
    *,
    service_id: str,
    session_id: str,
    backend_status: ServiceBackendStatus = ServiceBackendStatus.AVAILABLE,
    diagnostics: Mapping[str, object] | None = None,
) -> dict[str, object]:
    return EXAMPLE_PROTOCOL.advertisement_payload(
        service_id=service_id,
        session_id=session_id,
        backend_status=backend_status,
        diagnostics=diagnostics or {},
    ).to_dict()


def test_service_protocol_example() -> None:
    protocol = EXAMPLE_PROTOCOL
    assert tuple(protocol.operations) == ("presence.report",)
    assert tuple(protocol.messages) == ("presence.report",)
    assert protocol.view_families["status"].writer == "service"

    payload = example_advertisement_payload(
        service_id="presence-main",
        session_id="session-1",
    )
    assert payload["serviceId"] == "presence-main"
    assert payload["supportedOperations"] == ["presence.report"]
