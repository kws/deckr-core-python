"""View context."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from deckr.concord import ConcordManagedContract, ContractRecord
from deckr.contracts.authority import ContractPointer
from deckr.contracts.keys import encode_key_token
from deckr.contracts.messages import EndpointAddress, service_address
from deckr.services._validation import _require_text
from deckr.services.profile import is_service_use_record
from deckr.services.protocol import (
    ServiceProtocol,
    ServiceViewFamily,
    ServiceViewWriter,
    _service_protocol_views,
)


@dataclass(frozen=True, slots=True)
class ServiceViewWriteContext:
    writer: ServiceViewWriter
    service_id: str
    service_namespace: str
    service_endpoint: EndpointAddress
    service_session_id: str
    consumer_endpoint: EndpointAddress
    consumer_session_id: str
    contract: ContractPointer
    views: Mapping[str, ServiceViewFamily]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "service_id",
            _require_text(self.service_id, field_name="service id"),
        )
        object.__setattr__(
            self,
            "service_namespace",
            _require_text(
                self.service_namespace,
                field_name="service namespace",
            ),
        )
        object.__setattr__(
            self,
            "service_session_id",
            _require_text(
                self.service_session_id,
                field_name="service session id",
            ),
        )
        object.__setattr__(
            self,
            "consumer_session_id",
            _require_text(
                self.consumer_session_id,
                field_name="consumer session id",
            ),
        )
        object.__setattr__(self, "views", MappingProxyType(dict(self.views)))


@dataclass(frozen=True, slots=True)
class ServiceViewReadContext:
    reader: ServiceViewWriter
    service_id: str
    service_namespace: str
    service_endpoint: EndpointAddress
    service_session_id: str
    consumer_endpoint: EndpointAddress
    consumer_session_id: str
    contract: ContractPointer
    views: Mapping[str, ServiceViewFamily]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "service_id",
            _require_text(self.service_id, field_name="service id"),
        )
        object.__setattr__(
            self,
            "service_namespace",
            _require_text(
                self.service_namespace,
                field_name="service namespace",
            ),
        )
        object.__setattr__(
            self,
            "service_session_id",
            _require_text(
                self.service_session_id,
                field_name="service session id",
            ),
        )
        object.__setattr__(
            self,
            "consumer_session_id",
            _require_text(
                self.consumer_session_id,
                field_name="consumer session id",
            ),
        )
        object.__setattr__(self, "views", MappingProxyType(dict(self.views)))


@dataclass(frozen=True, slots=True)
class ServiceManagedContractContext:
    protocol: ServiceProtocol
    service_id: str
    service_namespace: str
    service_endpoint: EndpointAddress
    service_session_id: str
    consumer_endpoint: EndpointAddress
    consumer_session_id: str
    contract: ContractPointer
    views: Mapping[str, ServiceViewFamily]
    participant_side: ServiceViewWriter

    def view_read_context(self) -> ServiceViewReadContext:
        return ServiceViewReadContext(
            reader=self.participant_side,
            service_id=self.service_id,
            service_namespace=self.service_namespace,
            service_endpoint=self.service_endpoint,
            service_session_id=self.service_session_id,
            consumer_endpoint=self.consumer_endpoint,
            consumer_session_id=self.consumer_session_id,
            contract=self.contract,
            views=self.views,
        )

    def view_write_context(self) -> ServiceViewWriteContext:
        return ServiceViewWriteContext(
            writer=self.participant_side,
            service_id=self.service_id,
            service_namespace=self.service_namespace,
            service_endpoint=self.service_endpoint,
            service_session_id=self.service_session_id,
            consumer_endpoint=self.consumer_endpoint,
            consumer_session_id=self.consumer_session_id,
            contract=self.contract,
            views=self.views,
        )


def service_managed_contract_context(
    managed: ConcordManagedContract,
    *,
    protocol: ServiceProtocol,
    service_id: str,
    record: ContractRecord | None = None,
    validity: Any | None = None,
) -> ServiceManagedContractContext:
    """Derive service-side message and view context from a managed contract."""

    return _service_view_context_from_contract(
        managed,
        protocol=protocol,
        service_id=service_id,
        participant_side=ServiceViewWriter.SERVICE,
        record=record,
        validity=validity,
    )


def service_view_read_context_from_managed_contract(
    managed: ConcordManagedContract,
    *,
    protocol: ServiceProtocol,
    service_id: str,
) -> ServiceViewReadContext:
    """Return the service-side read context for a managed service-use contract."""

    return service_managed_contract_context(
        managed,
        protocol=protocol,
        service_id=service_id,
    ).view_read_context()


def service_view_write_context_from_managed_contract(
    managed: ConcordManagedContract,
    *,
    protocol: ServiceProtocol,
    service_id: str,
) -> ServiceViewWriteContext:
    """Return the service-side write context for a managed service-use contract."""

    return service_managed_contract_context(
        managed,
        protocol=protocol,
        service_id=service_id,
    ).view_write_context()


def service_view_key(service_id: str, family: str, *tokens: str) -> str:
    parts = ["views", encode_key_token(service_id), encode_key_token(family)]
    parts.extend(encode_key_token(token) for token in tokens)
    return ".".join(parts)


def service_view_prefix(service_id: str, family: str) -> str:
    return service_view_key(service_id, family) + "."


def _service_view_context_from_contract(
    managed: ConcordManagedContract,
    *,
    protocol: ServiceProtocol,
    service_id: str,
    participant_side: ServiceViewWriter,
    record: ContractRecord | None = None,
    validity: Any | None = None,
) -> ServiceManagedContractContext:
    if participant_side != ServiceViewWriter.SERVICE:
        raise ValueError("managed service contexts are service-side only")
    service_id = _require_text(service_id, field_name="service id")
    service_endpoint = service_address(service_id)
    validity = validity or managed.validity
    if not managed.local_authority:
        raise ValueError("managed service contract has no local authority")
    token = getattr(validity, "tokens", {}).get(str(service_endpoint))
    if token is None:
        raise ValueError("managed service contract requires a local participant token")
    if token.participant != service_endpoint:
        raise ValueError(
            "managed contract local participant is not the service endpoint"
        )
    record = record or managed.record
    if not is_service_use_record(
        record, profile=protocol.use_profile, service=service_endpoint
    ):
        raise ValueError("managed service contract does not match service-use profile")
    consumers = [
        participant
        for participant in record.participants
        if participant != service_endpoint
    ]
    if len(consumers) != 1:
        raise ValueError("managed service contract must have exactly one consumer")
    consumer_endpoint = consumers[0]
    consumer_token = getattr(validity, "tokens", {}).get(str(consumer_endpoint))
    if consumer_token is None:
        raise ValueError(
            "managed service contract requires a consumer participant token"
        )
    return ServiceManagedContractContext(
        protocol=protocol,
        service_id=service_id,
        service_namespace=protocol.namespace,
        service_endpoint=service_endpoint,
        service_session_id=token.session_id,
        consumer_endpoint=consumer_endpoint,
        consumer_session_id=consumer_token.session_id,
        contract=ContractPointer(
            contractId=managed.contract.contract_id,
            generation=managed.contract.generation,
        ),
        views=_service_protocol_views(protocol, service_id),
        participant_side=participant_side,
    )
