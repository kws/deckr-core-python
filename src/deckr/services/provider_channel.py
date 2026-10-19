"""Provider channel."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from deckr.concord import ConcordManagedContract, ConcordParticipant, ContractHandle
from deckr.contracts.authority import ContractPointer
from deckr.contracts.messages import SERVICES_LANE, DeckrMessage
from deckr.lanes import EndpointSession
from deckr.services._client_helpers import (
    _assert_response_envelope,
    _assert_service_to_consumer,
    _contract_diagnostics,
    _is_valid_response_body,
    _message_definition_from_mapping,
    _service_message_subject,
)
from deckr.services.managed_views import _ServiceManagedServiceViewAccess
from deckr.services.messages import (
    SERVICE_MESSAGE,
    ServiceExchangePattern,
    ServiceMessageBody,
    service_body,
)
from deckr.services.runtime import (
    ServiceManagedContractContext,
    ServiceProtocol,
    ServiceUnavailable,
    service_managed_contract_context,
)
from deckr.services.views import ManagedServiceViewAccess, ServiceViewStore


class ManagedServiceContract:
    """Service-side helpers bound to one managed service-use contract."""

    def __init__(
        self,
        *,
        endpoint: EndpointSession,
        participant: ConcordParticipant,
        contract: ContractHandle,
        protocol: ServiceProtocol,
        service_id: str,
    ) -> None:
        self._endpoint = endpoint
        self._participant = participant
        self._contract = contract
        self._protocol = protocol
        self._service_id = service_id

    async def send(
        self,
        name: str,
        params: Mapping[str, Any] | None = None,
        *,
        event: Mapping[str, Any] | None = None,
        subject=None,
    ) -> DeckrMessage:
        context = await self._context()
        definition = _message_definition_from_mapping(
            context.protocol.messages,
            name,
            exchange_pattern=ServiceExchangePattern.ONE_WAY,
            service_id=context.service_id,
        )
        _assert_service_to_consumer(definition, name)
        return await self._endpoint.send(
            lane=SERVICES_LANE,
            recipient=context.consumer_endpoint,
            recipient_session_id=context.consumer_session_id,
            subject=subject or _service_message_subject(context, name),
            message_type=SERVICE_MESSAGE,
            body=ServiceMessageBody(
                serviceNamespace=context.service_namespace,
                name=name,
                intent=definition.intent,
                exchangePattern=definition.exchange_pattern,
                params=dict(params or {}),
                event=dict(event) if event is not None else None,
            ).to_dict(),
            contract=context.contract,
        )

    async def request(
        self,
        name: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> ServiceMessageBody:
        context = await self._context()
        definition = _message_definition_from_mapping(
            context.protocol.messages,
            name,
            exchange_pattern=ServiceExchangePattern.REQUEST_REPLY,
            service_id=context.service_id,
        )
        _assert_service_to_consumer(definition, name)
        reply = await self._endpoint.request(
            lane=SERVICES_LANE,
            recipient=context.consumer_endpoint,
            recipient_session_id=context.consumer_session_id,
            subject=_service_message_subject(context, name),
            message_type=SERVICE_MESSAGE,
            body=ServiceMessageBody(
                serviceNamespace=context.service_namespace,
                name=name,
                intent=definition.intent,
                exchangePattern=definition.exchange_pattern,
                params=dict(params or {}),
            ).to_dict(),
            **({"timeout": timeout_seconds} if timeout_seconds is not None else {}),
            contract=context.contract,
        )
        body = service_body(reply)
        if _is_valid_response_body(
            body,
            context.service_namespace,
            name,
            definition,
        ):
            _assert_response_envelope(
                reply,
                sender=context.consumer_endpoint,
                sender_session_id=context.consumer_session_id,
                recipient=self._endpoint.address,
                recipient_session_id=self._endpoint.session_id,
                contract=context.contract,
                service_id=context.service_id,
                service_namespace=context.service_namespace,
                name=name,
            )
            return body
        raise ServiceUnavailable(
            "invalid_service_response",
            "Consumer returned an invalid service response",
            {
                "serviceId": context.service_id,
                "serviceNamespace": context.service_namespace,
                "name": name,
            },
        )

    def view_access(self, store: ServiceViewStore) -> ManagedServiceViewAccess:
        return _ServiceManagedServiceViewAccess(
            store,
            contract=self,
        )

    async def _context(self) -> ServiceManagedContractContext:
        if (
            self._participant.participant != self._endpoint.address
            or self._participant.session_id != self._endpoint.session_id
        ):
            raise ServiceUnavailable(
                "contract_session_mismatch",
                "Managed service contract endpoint does not match the participant session",
                _contract_diagnostics(
                    self._contract,
                    service_id=self._service_id,
                    service_session_id=self._endpoint.session_id,
                ),
            )
        managed = await self._managed_contract()
        validity = managed.validity
        if not validity.valid or validity.contract is None:
            raise ServiceUnavailable(
                f"contract_{validity.status.value}",
                "Managed service contract is not valid",
                {
                    **_contract_diagnostics(
                        self._contract,
                        service_id=self._service_id,
                        service_session_id=self._endpoint.session_id,
                    ),
                    "status": validity.status.value,
                    "reason": validity.reason,
                },
            )
        try:
            context = service_managed_contract_context(
                managed,
                protocol=self._protocol,
                service_id=self._service_id,
                record=validity.contract,
                validity=validity,
            )
        except ValueError as exc:
            raise ServiceUnavailable(
                "invalid_service_contract",
                "Managed service contract does not match the service protocol",
                _contract_diagnostics(
                    self._contract,
                    service_id=self._service_id,
                    service_session_id=self._endpoint.session_id,
                ),
            ) from exc
        expected_pointer = ContractPointer(
            contractId=self._contract.contract_id,
            generation=self._contract.generation,
        )
        if (
            context.service_endpoint != self._endpoint.address
            or context.service_session_id != self._endpoint.session_id
            or context.contract != expected_pointer
        ):
            raise ServiceUnavailable(
                "contract_session_mismatch",
                "Managed service contract context is stale",
                _contract_diagnostics(
                    self._contract,
                    service_id=self._service_id,
                    service_session_id=self._endpoint.session_id,
                ),
            )
        return context

    async def _managed_contract(self) -> ConcordManagedContract:
        managed = self._participant.managed_contract(self._contract)
        if managed is None:
            raise ServiceUnavailable(
                "contract_not_managed",
                "Managed service contract is not managed by this participant",
                _contract_diagnostics(
                    self._contract,
                    service_id=self._service_id,
                    service_session_id=self._endpoint.session_id,
                ),
            )
        return managed
