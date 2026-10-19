"""Discovery."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

from pydantic import Field, field_serializer, field_validator, model_validator

from deckr.beacon import Candidate
from deckr.contracts.messages import EndpointAddress, service_address
from deckr.contracts.models import DeckrModel, JsonObject, freeze_json, thaw_json
from deckr.services._validation import _require_text
from deckr.services.protocol import (
    ServiceBackendStatus,
    ServiceMessageDefinition,
    ServiceProtocol,
    ServiceViewFamily,
    _service_protocol_views,
)


class ServiceAdvertisementPayload(DeckrModel):
    profile: str
    service_id: str = Field(alias="serviceId")
    service_endpoint: EndpointAddress = Field(alias="serviceEndpoint")
    service_namespace: str = Field(alias="serviceNamespace")
    session_id: str = Field(alias="sessionId")
    service_use_profile: str = Field(alias="serviceUseProfile")
    backend_status: ServiceBackendStatus = Field(alias="backendStatus")
    supported_operations: tuple[str, ...] = Field(alias="supportedOperations")
    supported_messages: Mapping[str, ServiceMessageDefinition] = Field(
        alias="supportedMessages"
    )
    views: Mapping[str, ServiceViewFamily]
    diagnostics: JsonObject = Field(default_factory=dict)

    @field_validator(
        "profile",
        "service_id",
        "service_namespace",
        "session_id",
        "service_use_profile",
    )
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _require_text(value, field_name="service advertisement field")

    @field_validator("supported_operations")
    @classmethod
    def _validate_operations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            _require_text(item, field_name="service operation") for item in value
        )

    @field_validator("views", mode="after")
    @classmethod
    def _freeze_views_and_messages(
        cls,
        value: Mapping[str, ServiceViewFamily] | Mapping[str, ServiceMessageDefinition],
    ) -> Mapping[str, ServiceViewFamily] | Mapping[str, ServiceMessageDefinition]:
        return freeze_json(value)

    @field_validator("supported_messages", mode="after")
    @classmethod
    def _freeze_messages(
        cls,
        value: Mapping[str, ServiceMessageDefinition],
    ) -> Mapping[str, ServiceMessageDefinition]:
        return freeze_json(value)

    @field_validator("diagnostics", mode="before")
    @classmethod
    def _thaw_diagnostics(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("diagnostics", mode="after")
    @classmethod
    def _freeze_diagnostics(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return freeze_json(value)

    @field_serializer("views")
    def _serialize_views(
        self,
        value: Mapping[str, ServiceViewFamily],
    ) -> dict[str, dict[str, Any]]:
        return {
            key: item.model_dump(by_alias=True, exclude_none=True, mode="json")
            for key, item in value.items()
        }

    @field_serializer("supported_messages")
    def _serialize_supported_messages(
        self,
        value: Mapping[str, ServiceMessageDefinition],
    ) -> dict[str, dict[str, Any]]:
        return {
            key: item.model_dump(by_alias=True, exclude_none=True, mode="json")
            for key, item in value.items()
        }

    @field_serializer("diagnostics")
    def _serialize_diagnostics(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)

    @model_validator(mode="after")
    def _validate_identity(self) -> ServiceAdvertisementPayload:
        if self.service_endpoint != service_address(self.service_id):
            raise ValueError("serviceEndpoint must equal service:<serviceId>")
        return self

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


@dataclass(frozen=True, slots=True)
class ServiceDescriptor:
    """Profile-validated service fact from Beacon discovery."""

    candidate: Candidate | None
    service_id: str
    namespace: str
    endpoint: EndpointAddress
    session_id: str
    advertisement_profile: str
    use_profile: str
    supported_operations: frozenset[str]
    supported_messages: Mapping[str, ServiceMessageDefinition]
    views: Mapping[str, ServiceViewFamily]
    backend_status: ServiceBackendStatus
    diagnostics: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "supported_operations",
            frozenset(
                _require_text(item, field_name="service operation")
                for item in self.supported_operations
            ),
        )
        object.__setattr__(
            self,
            "supported_messages",
            MappingProxyType(dict(self.supported_messages)),
        )
        object.__setattr__(self, "views", MappingProxyType(dict(self.views)))
        object.__setattr__(self, "diagnostics", freeze_json(dict(self.diagnostics)))


def parse_service_descriptor(
    candidate: Candidate,
    protocol: ServiceProtocol,
) -> ServiceDescriptor | None:
    advertisement = candidate.advertisement
    if advertisement.feature_id != protocol.feature_id or advertisement.payload is None:
        return None
    try:
        payload = ServiceAdvertisementPayload.from_wire(
            thaw_json(advertisement.payload)
        )
    except (TypeError, ValueError):
        return None
    if payload.profile != protocol.advertisement_profile:
        return None
    if payload.service_namespace != protocol.namespace:
        return None
    if payload.service_use_profile != protocol.use_profile:
        return None
    if payload.service_endpoint != advertisement.endpoint:
        return None
    if payload.session_id != advertisement.session_id:
        return None
    if not set(payload.supported_operations).issubset(set(protocol.operations)):
        return None
    if not set(payload.supported_messages).issubset(set(protocol.messages)):
        return None
    for name, message in payload.supported_messages.items():
        expected_message = protocol.messages[name]
        if message.to_dict() != expected_message.to_dict():
            return None
    expected_views = _service_protocol_views(protocol, payload.service_id)
    if {key: family.to_dict() for key, family in payload.views.items()} != {
        key: family.to_dict() for key, family in expected_views.items()
    }:
        return None
    return ServiceDescriptor(
        candidate=candidate,
        service_id=payload.service_id,
        namespace=payload.service_namespace,
        endpoint=payload.service_endpoint,
        session_id=payload.session_id,
        advertisement_profile=payload.profile,
        use_profile=payload.service_use_profile,
        supported_operations=frozenset(payload.supported_operations),
        supported_messages=payload.supported_messages,
        views=expected_views,
        backend_status=payload.backend_status,
        diagnostics=payload.diagnostics,
    )


def service_descriptor_sort_key(
    descriptor: ServiceDescriptor,
) -> tuple[datetime, int, str]:
    if descriptor.candidate is None:
        return (datetime.min.replace(tzinfo=UTC), 0, "")
    advertisement = descriptor.candidate.advertisement
    timestamp = (
        advertisement.updated_at
        or advertisement.created_at
        or datetime.min.replace(tzinfo=UTC)
    )
    return (timestamp, advertisement.refresh_seq, descriptor.candidate.key)


def newest_service_descriptor(
    descriptors: Collection[ServiceDescriptor],
) -> ServiceDescriptor | None:
    if not descriptors:
        return None
    return max(descriptors, key=service_descriptor_sort_key)
