"""Deckr service protocol declarations and retained-view family policy."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from pydantic import Field, field_serializer, field_validator

from deckr.contracts.messages import service_address
from deckr.contracts.models import DeckrModel, JsonObject, freeze_json, thaw_json
from deckr.core.json_schema import local_validator
from deckr.services._validation import (
    _JSON_SCHEMA_CONTRACT_KEYS,
    _require_json_wire_safe,
    _require_text,
)
from deckr.services.messages import (
    ServiceExchangePattern,
    ServiceMessageDirection,
    ServiceMessageIntent,
)

if TYPE_CHECKING:
    from deckr.services.discovery import ServiceAdvertisementPayload

_OPERATION_INTENTS = frozenset(
    {
        ServiceMessageIntent.COMMAND,
        ServiceMessageIntent.QUERY,
    }
)


class ServiceBackendStatus(StrEnum):
    AVAILABLE = "available"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class ServiceViewWriter(StrEnum):
    SERVICE = "service"
    CONSUMER = "consumer"


class ServiceOperationDefinition(DeckrModel):
    description: str | None = None

    @field_validator("description")
    @classmethod
    def _validate_description(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_text(value, field_name="service operation description")

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


class ServicePayloadSchema(DeckrModel):
    """A JSON Schema contract for one service-message payload position."""

    schema_id: str = Field(alias="schemaId")
    json_schema: JsonObject = Field(alias="schema")

    @field_validator("schema_id")
    @classmethod
    def _validate_schema_id(cls, value: str) -> str:
        return _require_text(value, field_name="service payload schema id")

    @field_validator("json_schema", mode="before")
    @classmethod
    def _thaw_schema(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("json_schema", mode="after")
    @classmethod
    def _freeze_schema(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        if not value:
            raise ValueError("service payload schema must not be empty")
        if not any(key in value for key in _JSON_SCHEMA_CONTRACT_KEYS):
            raise ValueError(
                "service payload schema must include a JSON Schema contract keyword"
            )
        _require_json_wire_safe(value, field_name="service payload schema")
        local_validator(thaw_json(value))
        return freeze_json(value)

    @field_serializer("json_schema")
    def _serialize_schema(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


class ServiceMessageDefinition(DeckrModel):
    operation: str | None = None
    intent: ServiceMessageIntent
    exchange_pattern: ServiceExchangePattern = Field(alias="exchangePattern")
    direction: ServiceMessageDirection
    params_schema: ServicePayloadSchema | None = Field(
        default=None,
        alias="paramsSchema",
    )
    result_schema: ServicePayloadSchema | None = Field(
        default=None,
        alias="resultSchema",
    )
    event_schema: ServicePayloadSchema | None = Field(
        default=None,
        alias="eventSchema",
    )
    error_schema: ServicePayloadSchema | None = Field(
        default=None,
        alias="errorSchema",
    )

    @field_validator("operation")
    @classmethod
    def _validate_operation(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_text(value, field_name="service message operation")

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


class ServiceViewFamily(DeckrModel):
    store_name: str = Field(alias="storeName")
    key_prefix: str = Field(alias="keyPrefix")
    writer: ServiceViewWriter

    @field_validator("store_name", "key_prefix")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _require_text(value, field_name="service view family field")

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


class ServiceViewFamilyDefinition(DeckrModel):
    store_name: str = Field(alias="storeName")
    writer: ServiceViewWriter

    @field_validator("store_name")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _require_text(value, field_name="service view family definition field")

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


@dataclass(frozen=True, slots=True)
class ServiceViewRef:
    """Explicit reference to a service-owned current-state view."""

    store_name: str
    key: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "store_name",
            _require_text(self.store_name, field_name="service view store_name"),
        )
        object.__setattr__(
            self,
            "key",
            _require_text(self.key, field_name="service view key"),
        )


@dataclass(frozen=True, slots=True)
class ServiceProtocol:
    namespace: str
    feature_id: str
    advertisement_profile: str
    use_profile: str
    operations: Mapping[str, ServiceOperationDefinition]
    messages: Mapping[str, ServiceMessageDefinition]
    view_families: Mapping[str, ServiceViewFamilyDefinition]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "namespace",
            _require_text(self.namespace, field_name="service namespace"),
        )
        object.__setattr__(
            self,
            "feature_id",
            _require_text(self.feature_id, field_name="service feature id"),
        )
        object.__setattr__(
            self,
            "advertisement_profile",
            _require_text(
                self.advertisement_profile,
                field_name="service advertisement profile",
            ),
        )
        object.__setattr__(
            self,
            "use_profile",
            _require_text(self.use_profile, field_name="service use profile"),
        )
        operations: dict[str, ServiceOperationDefinition] = {}
        for name, definition in self.operations.items():
            key = _require_text(name, field_name="service operation")
            operations[key] = (
                definition
                if isinstance(definition, ServiceOperationDefinition)
                else ServiceOperationDefinition.model_validate(definition)
            )
        object.__setattr__(self, "operations", MappingProxyType(operations))
        messages: dict[str, ServiceMessageDefinition] = {}
        for name, definition in self.messages.items():
            key = _require_text(name, field_name="service message")
            messages[key] = (
                definition
                if isinstance(definition, ServiceMessageDefinition)
                else ServiceMessageDefinition.model_validate(definition)
            )
        _validate_service_message_operations(operations, messages)
        object.__setattr__(self, "messages", MappingProxyType(messages))
        families: dict[str, ServiceViewFamilyDefinition] = {}
        for name, family in self.view_families.items():
            key = _require_text(name, field_name="service view family name")
            families[key] = (
                family
                if isinstance(family, ServiceViewFamilyDefinition)
                else ServiceViewFamilyDefinition.model_validate(family)
            )
        object.__setattr__(self, "view_families", MappingProxyType(families))

    def advertisement_payload(
        self,
        *,
        service_id: str,
        session_id: str,
        backend_status: ServiceBackendStatus | str,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> ServiceAdvertisementPayload:
        from deckr.services.discovery import ServiceAdvertisementPayload

        return ServiceAdvertisementPayload(
            profile=self.advertisement_profile,
            serviceId=service_id,
            serviceEndpoint=service_address(service_id),
            serviceNamespace=self.namespace,
            sessionId=session_id,
            serviceUseProfile=self.use_profile,
            backendStatus=backend_status,
            supportedOperations=tuple(self.operations),
            supportedMessages=self.messages,
            views=_service_protocol_views(self, service_id),
            diagnostics=dict(diagnostics or {}),
        )


def _service_protocol_views(
    protocol: ServiceProtocol,
    service_id: str,
) -> Mapping[str, ServiceViewFamily]:
    from deckr.services.view_context import service_view_prefix

    service_id = _require_text(service_id, field_name="service id")
    return MappingProxyType(
        {
            family: ServiceViewFamily(
                storeName=definition.store_name,
                keyPrefix=service_view_prefix(service_id, family),
                writer=definition.writer,
            )
            for family, definition in protocol.view_families.items()
        }
    )


def _validate_service_message_operations(
    operations: Mapping[str, ServiceOperationDefinition],
    messages: Mapping[str, ServiceMessageDefinition],
) -> None:
    for name, definition in messages.items():
        if definition.operation is not None:
            if definition.operation not in operations:
                raise ValueError(
                    "service message references unknown operation "
                    f"{definition.operation!r}"
                )
            continue
        if definition.direction in {
            ServiceMessageDirection.CONSUMER_TO_SERVICE,
            ServiceMessageDirection.BIDIRECTIONAL,
        }:
            raise ValueError(
                "consumer-to-service and bidirectional service messages require "
                f"a declared operation: {name}"
            )
        if definition.intent in _OPERATION_INTENTS:
            raise ValueError(
                "service command and query messages require a declared operation: "
                f"{name}"
            )
