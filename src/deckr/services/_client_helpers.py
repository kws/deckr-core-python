"""Client helpers."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from time import monotonic
from typing import Any

import anyio

from deckr.concord import ConcordConflict, ConcordManagedContract, ContractHandle
from deckr.contracts.authority import ContractPointer
from deckr.contracts.messages import DeckrMessage, EndpointTarget, entity_subject
from deckr.services.messages import (
    SERVICE_MESSAGE,
    ServiceExchangePattern,
    ServiceMessageBody,
    ServiceMessageDirection,
    service_body,
)
from deckr.services.runtime import (
    ServiceDescriptor,
    ServiceManagedContractContext,
    ServiceMessageDefinition,
    ServiceProtocol,
    ServiceUnavailable,
    ServiceUseLease,
    ServiceViewRef,
    terminal_concord_conflict_status,
)
from deckr.services.state import ManagedServiceFailure

logger = logging.getLogger(__name__)


_DirectoryKey = tuple[str, str, str, str]


_MAX_MATCHING_SERVICE_CANDIDATES = 64


def _directory_key(protocol: ServiceProtocol) -> _DirectoryKey:
    return (
        protocol.namespace,
        protocol.feature_id,
        protocol.advertisement_profile,
        protocol.use_profile,
    )


def _deadline(timeout_seconds: float | None) -> float | None:
    if timeout_seconds is None:
        return None
    return monotonic() + max(0.0, float(timeout_seconds))


def _remaining_timeout(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return max(0.0, deadline - monotonic())


def _lease_pointer(lease: ServiceUseLease) -> ContractPointer:
    return ContractPointer(
        contractId=lease.contract.contract_id,
        generation=lease.contract.generation,
    )


def _managed_failure(error: ServiceUnavailable) -> ManagedServiceFailure:
    return ManagedServiceFailure(
        code=error.code,
        message=error.message,
        diagnostics=error.diagnostics,
    )


async def _wait_for_service_authority_loss(
    lease: ServiceUseLease,
) -> ServiceUnavailable:
    try:
        await lease.agreement.wait_for_state(lambda state: not state.valid)
    except (anyio.BrokenResourceError, anyio.ClosedResourceError):
        return ServiceUnavailable(
            "service_use_closed",
            "Managed service agreement closed while authoritative",
            {"serviceId": lease.descriptor.service_id},
        )
    try:
        await lease.require_authority()
    except ServiceUnavailable as exc:
        return exc
    return ServiceUnavailable(
        "contract_local_authority_lost",
        "Managed service capability lost local authority",
        {"serviceId": lease.descriptor.service_id},
    )


def _message_definition(
    lease: ServiceUseLease,
    name: str,
    *,
    exchange_pattern: ServiceExchangePattern,
) -> ServiceMessageDefinition:
    return _message_definition_from_mapping(
        lease.descriptor.supported_messages,
        name,
        exchange_pattern=exchange_pattern,
        service_id=lease.descriptor.service_id,
    )


def _message_definition_from_mapping(
    messages: Mapping[str, ServiceMessageDefinition],
    name: str,
    *,
    exchange_pattern: ServiceExchangePattern,
    service_id: str,
) -> ServiceMessageDefinition:
    definition = messages.get(name)
    if definition is None:
        raise ServiceUnavailable(
            "unsupported_service_message",
            "Service descriptor does not support this message",
            {"name": name, "serviceId": service_id},
        )
    if definition.exchange_pattern != exchange_pattern:
        raise ServiceUnavailable(
            "unsupported_service_exchange_pattern",
            "Service message uses a different exchange pattern",
            {
                "name": name,
                "exchangePattern": definition.exchange_pattern.value,
                "expectedExchangePattern": exchange_pattern.value,
            },
        )
    return definition


def _assert_consumer_to_service(
    definition: ServiceMessageDefinition,
    name: str,
) -> None:
    if definition.direction in {
        ServiceMessageDirection.CONSUMER_TO_SERVICE,
        ServiceMessageDirection.BIDIRECTIONAL,
    }:
        return
    raise ServiceUnavailable(
        "unsupported_service_message_direction",
        "Service message direction does not allow consumer-to-service sends",
        {"name": name, "direction": definition.direction.value},
    )


def _assert_service_to_consumer(
    definition: ServiceMessageDefinition,
    name: str,
) -> None:
    if definition.direction in {
        ServiceMessageDirection.SERVICE_TO_CONSUMER,
        ServiceMessageDirection.BIDIRECTIONAL,
    }:
        return
    raise ServiceUnavailable(
        "unsupported_service_message_direction",
        "Service message direction does not allow service-to-consumer sends",
        {"name": name, "direction": definition.direction.value},
    )


def _authorize_inbound_message(
    lease: ServiceUseLease,
    message: DeckrMessage,
    *,
    local_endpoint: Any,
    local_session_id: str,
    name: str | None,
) -> ServiceMessageBody:
    if message.message_type != SERVICE_MESSAGE:
        raise ServiceUnavailable(
            "invalid_service_message",
            "Inbound service traffic must use serviceMessage",
            {"messageType": message.message_type},
        )
    try:
        body = service_body(message)
    except (TypeError, ValueError) as exc:
        raise ServiceUnavailable(
            "invalid_service_message",
            "Inbound service message body is invalid",
        ) from exc
    descriptor = lease.descriptor
    expected_name = body.name if name is None else name
    definition = _message_definition_from_mapping(
        descriptor.supported_messages,
        expected_name,
        exchange_pattern=body.exchange_pattern,
        service_id=descriptor.service_id,
    )
    _assert_service_to_consumer(definition, expected_name)
    if (
        body.service_namespace != descriptor.namespace
        or body.name != expected_name
        or body.intent != definition.intent
    ):
        raise ServiceUnavailable(
            "scope_mismatch",
            "Inbound service message does not match the active lease",
            {
                "serviceId": descriptor.service_id,
                "serviceNamespace": body.service_namespace,
                "expectedNamespace": descriptor.namespace,
                "name": body.name,
                "expectedName": expected_name,
            },
        )
    _assert_response_envelope(
        message,
        sender=descriptor.endpoint,
        sender_session_id=descriptor.session_id,
        recipient=local_endpoint,
        recipient_session_id=local_session_id,
        contract=lease.contract,
        service_id=descriptor.service_id,
        service_namespace=descriptor.namespace,
        name=expected_name,
    )
    return body


def _is_valid_response_body(
    body: ServiceMessageBody,
    service_namespace: str,
    name: str,
    definition: ServiceMessageDefinition,
) -> bool:
    return (
        body.service_namespace == service_namespace
        and body.name == name
        and body.intent == definition.intent
        and body.exchange_pattern == definition.exchange_pattern
        and body.status is not None
    )


def _assert_response_envelope(
    message: DeckrMessage,
    *,
    sender: Any,
    sender_session_id: str,
    recipient: Any,
    recipient_session_id: str,
    contract: Any,
    service_id: str,
    service_namespace: str,
    name: str,
) -> None:
    pointer = (
        ContractPointer(
            contractId=contract.contract_id,
            generation=contract.generation,
        )
        if hasattr(contract, "contract_id")
        else ContractPointer.model_validate(contract)
    )
    if (
        message.sender != sender
        or message.sender_session_id != sender_session_id
        or not isinstance(message.recipient, EndpointTarget)
        or message.recipient.endpoint != recipient
        or message.recipient_session_id != recipient_session_id
        or message.contract != pointer
        or not _service_subject_matches(
            message,
            service_id=service_id,
            namespace=service_namespace,
            name=name,
        )
    ):
        raise ServiceUnavailable(
            "invalid_service_response",
            "Service response envelope does not match the active service-use lease",
            {
                "serviceId": service_id,
                "serviceNamespace": service_namespace,
                "name": name,
            },
        )


def _service_message_subject(
    context: ServiceManagedContractContext,
    name: str,
) -> Any:
    return entity_subject(
        "service",
        serviceId=context.service_id,
        namespace=context.service_namespace,
        name=name,
    )


def _managed_consumer_sessions(
    managed: ConcordManagedContract,
    *,
    service_endpoint: Any,
) -> dict[str, str]:
    consumers = [
        participant
        for participant in managed.record.participants
        if participant != service_endpoint
    ]
    if len(consumers) != 1:
        return {}
    consumer_endpoint = consumers[0]
    consumer_token = managed.validity.tokens.get(str(consumer_endpoint))
    if consumer_token is None:
        return {}
    return {str(consumer_endpoint): consumer_token.session_id}


def _contract_diagnostics(
    contract: ContractHandle,
    *,
    service_id: str,
    service_session_id: str,
) -> dict[str, Any]:
    return {
        "contractId": contract.contract_id,
        "generation": contract.generation,
        "profile": contract.profile,
        "serviceId": service_id,
        "serviceSessionId": service_session_id,
    }


def _service_subject_matches(
    message: DeckrMessage,
    *,
    service_id: str,
    namespace: str,
    name: str,
) -> bool:
    subject = message.subject
    identifiers = subject.identifiers
    return (
        subject.kind == "service"
        and identifiers.get("serviceId") == service_id
        and identifiers.get("namespace") == namespace
        and identifiers.get("name") == name
    )


def _contract_pointer(lease: ServiceUseLease) -> dict[str, int | str]:
    return {
        "contractId": lease.contract.contract_id,
        "generation": lease.contract.generation,
    }


def _service_view_unavailable(view: ServiceViewRef) -> ServiceUnavailable:
    return ServiceUnavailable(
        "service_view_unavailable",
        "Service view is unavailable",
        {"storeName": view.store_name, "key": view.key},
    )


def _service_use_conflict(
    descriptor: ServiceDescriptor,
    exc: ConcordConflict,
    *,
    agreement: Any | None,
) -> ServiceUnavailable:
    diagnostics = {
        "reason": str(exc),
        "contractId": None,
        "generation": None,
        "profile": descriptor.use_profile,
        "serviceId": descriptor.service_id,
        "serviceSessionId": descriptor.session_id,
    }
    if agreement is not None:
        diagnostics["contractId"] = agreement.contract.contract_id
        diagnostics["generation"] = agreement.contract.generation
    terminal_status = terminal_concord_conflict_status(exc)
    return ServiceUnavailable(
        (
            f"contract_{terminal_status.value}"
            if terminal_status is not None
            else "service_use_conflict"
        ),
        "Service-use contract could not be established",
        diagnostics,
    )


def _retryable_service_negotiation_failure(error: ServiceUnavailable) -> bool:
    """Retry only a proposal whose transport/view authority was unavailable.

    Contract rejection, session fencing, token loss, and malformed authority are
    terminal for that exact candidate attempt.  Retrying those outcomes would
    turn a deterministic rejection into an unbounded proposal loop.
    """

    return error.code == "contract_unavailable"
