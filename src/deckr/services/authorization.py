"""Authorization."""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass

from deckr.concord import (
    ConcordManagedContract,
    ConcordParticipant,
    ContractHandle,
    ContractRecord,
)
from deckr.contracts.authority import ContractPointer
from deckr.contracts.messages import (
    SERVICES_LANE,
    DeckrMessage,
    EndpointAddress,
    message_is_expired,
    service_address,
)
from deckr.services.errors import ServiceUseAuthorizationError
from deckr.services.messages import (
    SERVICE_MESSAGE,
    ServiceMessageBody,
    ServiceMessageDirection,
    service_body,
)
from deckr.services.payload_validation import validate_service_payloads
from deckr.services.profile import is_service_use_record
from deckr.services.protocol import ServiceProtocol, ServiceViewWriter
from deckr.services.view_context import (
    ServiceViewReadContext,
    ServiceViewWriteContext,
    _service_view_context_from_contract,
)


@dataclass(frozen=True, slots=True)
class AuthorizedServiceMessage:
    contract: ContractHandle
    record: ContractRecord
    body: ServiceMessageBody
    view_read_context: ServiceViewReadContext
    view_write_context: ServiceViewWriteContext


async def authorize_service_message(
    participant: ConcordParticipant,
    message: DeckrMessage,
    *,
    service_id: str,
    protocol: ServiceProtocol,
    name: str | None = None,
) -> AuthorizedServiceMessage:
    """Validate that a service message is authorized by its exact Concord pointer."""

    if (
        message.lane != SERVICES_LANE
        or message_is_expired(message)
        or getattr(message.recipient, "endpoint", None) != participant.participant
        or message.recipient_session_id != participant.session_id
    ):
        raise ServiceUseAuthorizationError(
            "scope_mismatch",
            "Service envelope is expired or targets another endpoint session",
        )

    try:
        body = service_body(message)
    except (TypeError, ValueError) as exc:
        raise ServiceUseAuthorizationError(
            "invalid_service_message",
            "Service message body is invalid",
        ) from exc
    if message.message_type != SERVICE_MESSAGE:
        raise ServiceUseAuthorizationError(
            "invalid_service_message",
            "Service authorization requires a serviceMessage",
            {"messageType": message.message_type},
        )
    expected_name = body.name if name is None else name
    if not _service_subject_matches(
        message,
        service_id=service_id,
        namespace=protocol.namespace,
        name=expected_name,
    ):
        raise ServiceUseAuthorizationError(
            "scope_mismatch",
            "Service message subject does not match the authorized service message",
            {
                "serviceId": service_id,
                "namespace": protocol.namespace,
                "name": expected_name,
            },
        )
    if body.service_namespace != protocol.namespace or body.name != expected_name:
        raise ServiceUseAuthorizationError(
            "scope_mismatch",
            "Service message does not target the authorized protocol message",
            {
                "serviceNamespace": body.service_namespace,
                "expectedNamespace": protocol.namespace,
                "name": body.name,
                "expectedName": expected_name,
            },
        )
    definition = protocol.messages.get(expected_name)
    if definition is None:
        raise ServiceUseAuthorizationError(
            "scope_mismatch",
            "Service protocol does not declare this message",
            {"name": expected_name},
        )
    if (
        body.intent != definition.intent
        or body.exchange_pattern != definition.exchange_pattern
    ):
        raise ServiceUseAuthorizationError(
            "scope_mismatch",
            "Service message metadata does not match the protocol definition",
            {
                "name": expected_name,
                "intent": body.intent.value,
                "expectedIntent": definition.intent.value,
                "exchangePattern": body.exchange_pattern.value,
                "expectedExchangePattern": definition.exchange_pattern.value,
            },
        )
    if definition.direction not in {
        ServiceMessageDirection.CONSUMER_TO_SERVICE,
        ServiceMessageDirection.BIDIRECTIONAL,
    }:
        raise ServiceUseAuthorizationError(
            "direction_mismatch",
            "Service message direction is not authorized from consumer to service",
            {"name": expected_name, "direction": definition.direction.value},
        )

    pointer = message.contract
    if pointer is None:
        raise ServiceUseAuthorizationError(
            "missing_contract",
            "Service message requires a Concord contract pointer",
        )
    managed = _managed_contract_for_pointer(
        participant.managed_contracts,
        pointer,
    )
    if managed is None:
        raise ServiceUseAuthorizationError(
            "contract_not_managed",
            "Service message contract is not managed by this participant",
            {"contractId": pointer.contract_id, "generation": pointer.generation},
        )

    if not managed.local_authority:
        raise ServiceUseAuthorizationError(
            "contract_local_authority_lost",
            "Service participant no longer holds local contract authority",
            {"contractId": pointer.contract_id, "generation": pointer.generation},
        )

    validity = managed.validity
    if not validity.valid or validity.contract is None:
        raise ServiceUseAuthorizationError(
            f"contract_{validity.status.value}",
            "Service message contract is not valid",
            {
                "contractId": pointer.contract_id,
                "generation": pointer.generation,
                "status": validity.status.value,
                "reason": validity.reason,
            },
        )
    if message.sender not in validity.contract.participants:
        raise ServiceUseAuthorizationError(
            "scope_mismatch",
            "Service message sender is not named by the managed contract",
            {
                "contractId": pointer.contract_id,
                "generation": pointer.generation,
                "sender": str(message.sender),
            },
        )
    sender_token = validity.tokens.get(str(message.sender))
    if sender_token is None or sender_token.session_id != message.sender_session_id:
        raise ServiceUseAuthorizationError(
            "contract_session_mismatch",
            "Service message sender session does not match the managed contract",
            {
                "contractId": pointer.contract_id,
                "generation": pointer.generation,
                "sender": str(message.sender),
                "senderSessionId": message.sender_session_id,
            },
        )

    if not _service_message_contract_match(
        validity.contract,
        participant=participant,
        sender=message.sender,
        service_id=service_id,
        protocol=protocol,
        name=expected_name,
    ):
        raise ServiceUseAuthorizationError(
            "scope_mismatch",
            "Service message contract does not authorize this message",
            {
                "contractId": pointer.contract_id,
                "generation": pointer.generation,
                "serviceId": service_id,
                "name": expected_name,
            },
        )
    context = _service_view_context_from_contract(
        managed,
        protocol=protocol,
        service_id=service_id,
        participant_side=ServiceViewWriter.SERVICE,
        record=validity.contract,
        validity=validity,
    )
    try:
        validate_service_payloads(definition, body)
    except ValueError as exc:
        raise ServiceUseAuthorizationError("invalid_service_payload", str(exc)) from exc
    return AuthorizedServiceMessage(
        contract=managed.contract,
        record=validity.contract,
        body=body,
        view_read_context=context.view_read_context(),
        view_write_context=context.view_write_context(),
    )


def _managed_contract_for_pointer(
    managed_contracts: Collection[ConcordManagedContract],
    pointer: ContractPointer,
) -> ConcordManagedContract | None:
    for managed in managed_contracts:
        contract = managed.contract
        if (
            contract.contract_id == pointer.contract_id
            and contract.generation == pointer.generation
        ):
            return managed
    return None


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


def _service_message_contract_match(
    record: ContractRecord,
    *,
    participant: ConcordParticipant,
    sender: EndpointAddress,
    service_id: str,
    protocol: ServiceProtocol,
    name: str,
) -> bool:
    expected_participants = {str(participant.participant), str(sender)}
    return (
        {str(item) for item in record.participants} == expected_participants
        and is_service_use_record(
            record, profile=protocol.use_profile, service=service_address(service_id)
        )
        and participant.participant == service_address(service_id)
        and name in protocol.messages
    )
