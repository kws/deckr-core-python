"""ServiceConsumerMessaging."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from deckr.contracts.messages import SERVICES_LANE, DeckrMessage, entity_subject
from deckr.lanes import EndpointSession
from deckr.services._client_helpers import (
    _assert_consumer_to_service,
    _assert_response_envelope,
    _authorize_inbound_message,
    _contract_pointer,
    _is_valid_response_body,
    _message_definition,
)
from deckr.services.messages import (
    SERVICE_MESSAGE,
    ServiceExchangePattern,
    ServiceMessageBody,
    service_body,
)
from deckr.services.payload_validation import validated_service_body
from deckr.services.runtime import ServiceUnavailable, ServiceUseLease


class ServiceConsumerMessaging:
    def __init__(self, endpoint: EndpointSession) -> None:
        self._endpoint = endpoint

    async def _send(
        self,
        lease: ServiceUseLease,
        name: str,
        params: Mapping[str, Any] | None = None,
        *,
        event: Mapping[str, Any] | None = None,
        causation_id: str | None = None,
    ) -> Any:
        """Send an authorized one-way service message over the services lane."""

        definition = _message_definition(
            lease,
            name,
            exchange_pattern=ServiceExchangePattern.ONE_WAY,
        )
        _assert_consumer_to_service(definition, name)
        await lease.require_authority()
        descriptor = lease.descriptor
        return await self._endpoint.send(
            causation_id=causation_id,
            lane=SERVICES_LANE,
            recipient=descriptor.endpoint,
            recipient_session_id=descriptor.session_id,
            subject=entity_subject(
                "service",
                serviceId=descriptor.service_id,
                namespace=descriptor.namespace,
                name=name,
            ),
            message_type=SERVICE_MESSAGE,
            body=validated_service_body(
                definition,
                serviceNamespace=descriptor.namespace,
                name=name,
                intent=definition.intent,
                exchangePattern=definition.exchange_pattern,
                params=dict(params or {}),
                event=dict(event) if event is not None else None,
            ),
            contract=_contract_pointer(lease),
        )

    async def _request(
        self,
        lease: ServiceUseLease,
        name: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> ServiceMessageBody:
        """Send an authorized request-reply service message over the services lane."""

        definition = _message_definition(
            lease,
            name,
            exchange_pattern=ServiceExchangePattern.REQUEST_REPLY,
        )
        _assert_consumer_to_service(definition, name)
        await lease.require_authority()
        descriptor = lease.descriptor
        reply = await self._endpoint.request(
            lane=SERVICES_LANE,
            recipient=descriptor.endpoint,
            recipient_session_id=descriptor.session_id,
            subject=entity_subject(
                "service",
                serviceId=descriptor.service_id,
                namespace=descriptor.namespace,
                name=name,
            ),
            message_type=SERVICE_MESSAGE,
            body=validated_service_body(
                definition,
                serviceNamespace=descriptor.namespace,
                name=name,
                intent=definition.intent,
                exchangePattern=definition.exchange_pattern,
                params=dict(params or {}),
            ),
            **({"timeout": timeout_seconds} if timeout_seconds is not None else {}),
            contract=_contract_pointer(lease),
        )
        await lease.require_authority()
        body = service_body(reply)
        if _is_valid_response_body(body, descriptor.namespace, name, definition):
            _assert_response_envelope(
                reply,
                sender=descriptor.endpoint,
                sender_session_id=descriptor.session_id,
                recipient=self._endpoint.address,
                recipient_session_id=self._endpoint.session_id,
                contract=lease.contract,
                service_id=descriptor.service_id,
                service_namespace=descriptor.namespace,
                name=name,
            )
            return body
        raise ServiceUnavailable(
            "invalid_service_response",
            "Service returned an invalid response",
            {
                "serviceId": descriptor.service_id,
                "serviceNamespace": descriptor.namespace,
                "name": name,
            },
        )

    async def _authorize_inbound_message(
        self,
        lease: ServiceUseLease,
        message: DeckrMessage,
        *,
        name: str | None = None,
    ) -> ServiceMessageBody:
        """Validate service-to-consumer traffic against the active lease."""

        await lease.require_authority()
        return _authorize_inbound_message(
            lease,
            message,
            local_endpoint=self._endpoint.address,
            local_session_id=self._endpoint.session_id,
            name=name,
        )
