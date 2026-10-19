"""Capability descriptor."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import (
    Field,
    JsonValue,
    field_serializer,
    field_validator,
    model_validator,
)

from deckr.contracts.models import DeckrModel, freeze_json, thaw_json
from deckr.hardware._descriptor_rules import (
    _DIRECTION_ACCESS,
    _JSON_SCHEMA_CONTRACT_KEYS,
    BUTTON_ACTIVATION_EVENTS,
    BUTTON_MOMENTARY_EVENTS,
    CORE_CAPABILITY_FAMILIES,
    CORE_CAPABILITY_TYPES_BY_FAMILY,
    DECKR_DEVICE_POWER,
    DECKR_INPUT_BUTTON,
    DECKR_INPUT_ENCODER,
    DECKR_INPUT_TOUCH,
    DECKR_OUTPUT_RASTER,
    ENCODER_RELATIVE_EVENTS,
    POWER_COMMAND_TYPES,
    RASTER_COMMAND_TYPES,
    TOUCH_GESTURE_EVENTS,
    CapabilityAccess,
    CapabilityDirection,
    ProjectionOwner,
    ProjectionType,
    _require_contract_token,
    _require_finite,
    _require_globally_qualified_name,
    _require_json_wire_safe,
    _require_non_empty,
    _validate_unique_tuple,
)
from deckr.hardware.references import DescriptorCapabilityRef, DeviceSourceReference


class CapabilitySchema(DeckrModel):
    """A JSON Schema fragment carried by a capability descriptor."""

    schema_id: str | None = Field(default=None, alias="schemaId")
    json_schema: Mapping[str, Any] = Field(alias="schema")

    @field_validator("schema_id")
    @classmethod
    def _validate_schema_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_globally_qualified_name(value, field_name="schema_id")

    @field_validator("json_schema", mode="after")
    @classmethod
    def _freeze_schema(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        if not value:
            raise ValueError("capability schema must not be empty")
        if not any(key in value for key in _JSON_SCHEMA_CONTRACT_KEYS):
            raise ValueError(
                "capability schema must include a JSON Schema contract keyword"
            )
        _require_json_wire_safe(value, field_name="capability schema")
        return freeze_json(value)

    @field_serializer("json_schema")
    def _serialize_schema(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)


class CapabilityConstraint(DeckrModel):
    """A typed generic constraint on a capability value or command parameter."""

    constraint_type: str = Field(alias="type")
    subject: str
    value: JsonValue | None = None
    values: tuple[JsonValue, ...] = Field(default_factory=tuple)
    minimum: float | None = None
    maximum: float | None = None
    step: float | None = None
    unit: str | None = None

    @field_validator("constraint_type")
    @classmethod
    def _validate_constraint_type(cls, value: str) -> str:
        return _require_contract_token(value, field_name="constraint type")

    @field_validator("subject", "unit")
    @classmethod
    def _validate_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_contract_token(value, field_name="constraint subject")

    @field_validator("minimum", "maximum", "step")
    @classmethod
    def _validate_finite_number(cls, value: float | None) -> float | None:
        return _require_finite(value, field_name="capability constraint number")

    @field_validator("value", mode="after")
    @classmethod
    def _freeze_value(cls, value: Any) -> Any:
        return freeze_json(value)

    @field_serializer("value")
    def _serialize_value(self, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("values", mode="after")
    @classmethod
    def _freeze_values(cls, value: tuple[Any, ...]) -> tuple[Any, ...]:
        return freeze_json(value)

    @field_serializer("values")
    def _serialize_values(self, value: tuple[Any, ...]) -> list[Any]:
        return thaw_json(value)

    @model_validator(mode="after")
    def _validate_constraint(self) -> CapabilityConstraint:
        if (
            self.value is None
            and not self.values
            and self.minimum is None
            and self.maximum is None
            and self.step is None
        ):
            raise ValueError("capability constraint must carry a bound or value")
        if (
            self.minimum is not None
            and self.maximum is not None
            and self.minimum > self.maximum
        ):
            raise ValueError("capability constraint minimum must not exceed maximum")
        if self.step is not None and self.step <= 0:
            raise ValueError("capability constraint step must be positive")
        return self


class CapabilityUnit(DeckrModel):
    """Unit metadata for a capability field."""

    subject: str
    unit: str
    symbol: str | None = None
    scale: float = 1.0

    @field_validator("subject", "unit", "symbol")
    @classmethod
    def _validate_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_contract_token(value, field_name="capability unit")

    @field_validator("scale")
    @classmethod
    def _validate_scale(cls, value: float) -> float:
        _require_finite(value, field_name="capability unit scale")
        if value <= 0:
            raise ValueError("capability unit scale must be positive")
        return value


class CapabilityProjection(DeckrModel):
    """Explicit projection or derivation metadata for a capability."""

    projection_type: ProjectionType = Field(default="projection", alias="type")
    owner: ProjectionOwner
    source: DescriptorCapabilityRef
    description: str | None = None

    @field_validator("description")
    @classmethod
    def _validate_description(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_non_empty(value, field_name="projection description")


class CapabilityDescriptor(DeckrModel):
    """Descriptor for one input, output, state, or command capability."""

    capability_id: str = Field(alias="capabilityId")
    family: str
    capability_type: str = Field(alias="type")
    direction: CapabilityDirection
    access: tuple[CapabilityAccess, ...]
    value_schema: CapabilitySchema | None = Field(default=None, alias="valueSchema")
    command_schema: CapabilitySchema | None = Field(default=None, alias="commandSchema")
    constraints: tuple[CapabilityConstraint, ...] = Field(default_factory=tuple)
    units: tuple[CapabilityUnit, ...] = Field(default_factory=tuple)
    event_types: tuple[str, ...] = Field(default_factory=tuple, alias="eventTypes")
    command_types: tuple[str, ...] = Field(default_factory=tuple, alias="commandTypes")
    projection: CapabilityProjection | None = None
    sources: tuple[DeviceSourceReference, ...] = Field(default_factory=tuple)

    @field_validator("capability_id")
    @classmethod
    def _validate_capability_id(cls, value: str) -> str:
        return _require_contract_token(value, field_name="capability_id")

    @field_validator("family")
    @classmethod
    def _validate_family(cls, value: str) -> str:
        family = _require_globally_qualified_name(value, field_name="capability family")
        if family.startswith("dev.deckr.") and family not in CORE_CAPABILITY_FAMILIES:
            raise ValueError(f"unsupported Deckr core capability family: {family}")
        return family

    @field_validator("capability_type")
    @classmethod
    def _validate_capability_type(cls, value: str) -> str:
        return _require_contract_token(value, field_name="capability type")

    @field_validator("access", mode="after")
    @classmethod
    def _validate_access(
        cls, value: tuple[CapabilityAccess, ...]
    ) -> tuple[CapabilityAccess, ...]:
        if not value:
            raise ValueError("capability access must not be empty")
        return _validate_unique_tuple(value, field_name="capability access")

    @field_validator("event_types", "command_types", mode="after")
    @classmethod
    def _validate_type_names(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        names = tuple(
            _require_contract_token(item, field_name="event or command type")
            for item in value
        )
        return _validate_unique_tuple(names, field_name="event or command types")

    @model_validator(mode="after")
    def _validate_capability(self) -> CapabilityDescriptor:
        allowed_access = _DIRECTION_ACCESS[self.direction]
        if not set(self.access) & allowed_access:
            allowed = ", ".join(sorted(allowed_access))
            raise ValueError(
                f"{self.direction} capability access must include one of: {allowed}"
            )
        if self.event_types and self.direction not in {"input", "state"}:
            raise ValueError(
                "event types are only valid on input or state capabilities"
            )
        if self.command_types and self.direction not in {"output", "command"}:
            raise ValueError(
                "command types are only valid on output or command capabilities"
            )
        if self.command_schema is not None and self.direction not in {
            "output",
            "command",
        }:
            raise ValueError(
                "command schema is only valid on output or command capabilities"
            )
        self._validate_core_family_type()
        return self

    def _validate_core_family_type(self) -> None:
        if self.family not in CORE_CAPABILITY_TYPES_BY_FAMILY:
            return
        allowed_types = CORE_CAPABILITY_TYPES_BY_FAMILY[self.family]
        if self.capability_type not in allowed_types:
            allowed = ", ".join(sorted(allowed_types))
            raise ValueError(f"{self.family} capability type must be one of: {allowed}")
        if self.family == DECKR_INPUT_BUTTON:
            self._validate_button_events()
        elif (
            self.family == DECKR_INPUT_ENCODER
            and self.event_types != ENCODER_RELATIVE_EVENTS
        ):
            raise ValueError(
                "dev.deckr.input.encoder relative capabilities emit rotate"
            )
        elif (
            self.family == DECKR_INPUT_TOUCH
            and self.event_types != TOUCH_GESTURE_EVENTS
        ):
            raise ValueError(
                "dev.deckr.input.touch gesture capabilities emit tap and swipe"
            )
        elif (
            self.family == DECKR_OUTPUT_RASTER
            and self.command_types != RASTER_COMMAND_TYPES
        ):
            raise ValueError(
                "dev.deckr.output.raster bitmap capabilities support set_frame and clear"
            )
        elif (
            self.family == DECKR_DEVICE_POWER
            and self.command_types != POWER_COMMAND_TYPES
        ):
            raise ValueError(
                "dev.deckr.device.power screen capabilities support sleep and wake"
            )

    def _validate_button_events(self) -> None:
        if self.capability_type == "activation":
            if self.event_types != BUTTON_ACTIVATION_EVENTS:
                raise ValueError(
                    "dev.deckr.input.button activation capabilities emit press only"
                )
            return
        if self.capability_type == "momentary":
            if self.event_types != BUTTON_MOMENTARY_EVENTS:
                raise ValueError(
                    "dev.deckr.input.button momentary capabilities emit down and up"
                )
