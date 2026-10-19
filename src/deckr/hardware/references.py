"""References."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import Field, field_serializer, field_validator, model_validator

from deckr.contracts.models import DeckrModel, JsonObject, freeze_json, thaw_json
from deckr.hardware._descriptor_rules import (
    ConnectionStatus,
    ControlGeometryUnit,
    _require_contract_token,
    _require_finite,
    _require_non_empty,
    _require_not_endpoint_address,
)


class DeviceIdentifier(DeckrModel):
    """Stable or diagnostic identifier attached to a device."""

    identifier_type: str = Field(alias="type")
    value: str
    namespace: str | None = None
    issuer: str | None = None

    @field_validator("identifier_type")
    @classmethod
    def _validate_identifier_type(cls, value: str) -> str:
        return _require_contract_token(value, field_name="identifier type")

    @field_validator("value", "namespace", "issuer")
    @classmethod
    def _validate_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_non_empty(value, field_name="identifier text")


class DeviceConnection(DeckrModel):
    """Structured protocol or transport facts about a device connection."""

    connection_id: str = Field(alias="connectionId")
    connection_type: str = Field(alias="type")
    status: ConnectionStatus = "available"
    label: str | None = None
    transport: str | None = None
    facts: JsonObject = Field(default_factory=dict)

    @field_validator("connection_id")
    @classmethod
    def _validate_connection_id(cls, value: str) -> str:
        return _require_not_endpoint_address(value, field_name="connection_id")

    @field_validator("connection_type")
    @classmethod
    def _validate_connection_type(cls, value: str) -> str:
        return _require_contract_token(value, field_name="connection type")

    @field_validator("label", "transport")
    @classmethod
    def _validate_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_non_empty(value, field_name="connection text")

    @field_validator("facts", mode="after")
    @classmethod
    def _freeze_facts(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return freeze_json(value)

    @field_serializer("facts")
    def _serialize_facts(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)


class DeviceSourceReference(DeckrModel):
    """Diagnostic reference to a source descriptor, report, layout, or protocol fact."""

    source_id: str = Field(alias="sourceId")
    source_type: str = Field(alias="type")
    connection_id: str | None = Field(default=None, alias="connectionId")
    label: str | None = None
    facts: JsonObject = Field(default_factory=dict)

    @field_validator("source_id")
    @classmethod
    def _validate_source_id(cls, value: str) -> str:
        return _require_not_endpoint_address(value, field_name="source_id")

    @field_validator("source_type")
    @classmethod
    def _validate_source_type(cls, value: str) -> str:
        return _require_contract_token(value, field_name="source type")

    @field_validator("connection_id", "label")
    @classmethod
    def _validate_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_non_empty(value, field_name="source text")

    @field_validator("facts", mode="after")
    @classmethod
    def _freeze_facts(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return freeze_json(value)

    @field_serializer("facts")
    def _serialize_facts(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)


class DeviceRef(DeckrModel):
    """Reference to a manager-local device in a hardware manager context."""

    manager_id: str = Field(alias="managerId")
    device_id: str = Field(alias="deviceId")
    fingerprint: str | None = None

    @field_validator("manager_id", "device_id")
    @classmethod
    def _validate_identity(cls, value: str) -> str:
        return _require_not_endpoint_address(value, field_name="device reference")

    @field_validator("fingerprint")
    @classmethod
    def _validate_fingerprint(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_non_empty(value, field_name="fingerprint")


class ControlRef(DeckrModel):
    """Reference to a control on a concrete device."""

    device_ref: DeviceRef = Field(alias="deviceRef")
    control_id: str = Field(alias="controlId")

    @field_validator("control_id")
    @classmethod
    def _validate_control_id(cls, value: str) -> str:
        return _require_non_empty(value, field_name="control_id")


class CapabilityRef(DeckrModel):
    """Reference to a device-level or control-level capability."""

    device_ref: DeviceRef | None = Field(default=None, alias="deviceRef")
    control_id: str | None = Field(default=None, alias="controlId")
    capability_id: str = Field(alias="capabilityId")

    @field_validator("control_id")
    @classmethod
    def _validate_control_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_non_empty(value, field_name="control_id")

    @field_validator("capability_id")
    @classmethod
    def _validate_capability_id(cls, value: str) -> str:
        return _require_contract_token(value, field_name="capability_id")


class DescriptorCapabilityRef(DeckrModel):
    """Reference to a capability owned by the enclosing ``DeviceDescriptor``."""

    control_id: str | None = Field(default=None, alias="controlId")
    capability_id: str = Field(alias="capabilityId")

    @field_validator("control_id")
    @classmethod
    def _validate_control_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_non_empty(value, field_name="control_id")

    @field_validator("capability_id")
    @classmethod
    def _validate_capability_id(cls, value: str) -> str:
        return _require_contract_token(value, field_name="capability_id")


class ControlGeometry(DeckrModel):
    """A control's placement on a logical or physical surface."""

    x: float = 0
    y: float = 0
    width: float | None = None
    height: float | None = None
    unit: ControlGeometryUnit = "grid"
    rotation: int | None = None
    layer: int | None = None

    @field_validator("x", "y", "width", "height")
    @classmethod
    def _validate_finite(cls, value: float | None) -> float | None:
        return _require_finite(value, field_name="geometry value")

    @model_validator(mode="after")
    def _validate_geometry(self) -> ControlGeometry:
        if self.width is not None and self.width <= 0:
            raise ValueError("geometry width must be positive")
        if self.height is not None and self.height <= 0:
            raise ValueError("geometry height must be positive")
        if self.unit == "normalized":
            for name, value in {
                "x": self.x,
                "y": self.y,
                "width": self.width,
                "height": self.height,
            }.items():
                if value is not None and not 0 <= value <= 1:
                    raise ValueError(
                        f"normalized geometry {name} must be between 0 and 1"
                    )
        return self
