"""Bindings."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import (
    Field,
    JsonValue,
    field_serializer,
    field_validator,
    model_validator,
)

from deckr.actions._body import (
    CapabilityProvenance,
    CapabilityViewKind,
    _require_bound_capability_ref,
    _require_capability_family,
    _require_contract_name,
    _require_non_negative,
    _require_optional_text,
    _require_text,
    _validate_contract_names,
)
from deckr.actions.endpoints import require_provider_instance_id
from deckr.contracts.models import DeckrModel, freeze_json, thaw_json
from deckr.hardware.descriptors import (
    CapabilityDirection,
    CapabilityRef,
    ControlRef,
    DeviceRef,
)


class CapabilityRequirementSelector(DeckrModel):
    """One acceptable capability shape for an action requirement."""

    capability_id: str | None = Field(default=None, alias="capabilityId")
    family: str | None = None
    capability_type: str | None = Field(default=None, alias="type")
    direction: CapabilityDirection | None = None
    event_types: tuple[str, ...] = Field(default_factory=tuple, alias="eventTypes")
    command_types: tuple[str, ...] = Field(default_factory=tuple, alias="commandTypes")

    @field_validator("capability_id")
    @classmethod
    def _validate_capability_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_contract_name(value, field_name="capability id")

    @field_validator("family")
    @classmethod
    def _validate_family(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_capability_family(value, field_name="capability family")

    @field_validator("capability_type")
    @classmethod
    def _validate_capability_type(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_contract_name(value, field_name="capability type")

    @field_validator("event_types")
    @classmethod
    def _validate_event_types(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_contract_names(value, field_name="event type")

    @field_validator("command_types")
    @classmethod
    def _validate_command_types(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_contract_names(value, field_name="command type")

    @model_validator(mode="after")
    def _require_selector_field(self) -> CapabilityRequirementSelector:
        if not any(
            (
                self.capability_id,
                self.family,
                self.capability_type,
                self.direction,
                self.event_types,
                self.command_types,
            )
        ):
            raise ValueError("capability selector must include at least one criterion")
        return self


class CapabilityRequirement(DeckrModel):
    """A named action input, output, state, config, or diagnostic requirement."""

    name: str
    required: bool = True
    preferences: tuple[CapabilityRequirementSelector, ...]
    event_types: tuple[str, ...] = Field(default_factory=tuple, alias="eventTypes")
    command_types: tuple[str, ...] = Field(default_factory=tuple, alias="commandTypes")
    views: tuple[CapabilityViewKind, ...] = Field(default_factory=tuple)

    @field_validator("views")
    @classmethod
    def _validate_views(
        cls, value: tuple[CapabilityViewKind, ...]
    ) -> tuple[CapabilityViewKind, ...]:
        if len(set(value)) != len(value):
            raise ValueError("capability views must be unique")
        return value

    @field_validator("name")
    @classmethod
    def _validate_name(cls, value: str) -> str:
        return _require_text(value, field_name="requirement name")

    @field_validator("event_types")
    @classmethod
    def _validate_event_types(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_contract_names(value, field_name="event type")

    @field_validator("command_types")
    @classmethod
    def _validate_command_types(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_contract_names(value, field_name="command type")

    @field_validator("preferences", mode="after")
    @classmethod
    def _validate_preferences(
        cls, value: tuple[CapabilityRequirementSelector, ...]
    ) -> tuple[CapabilityRequirementSelector, ...]:
        if not value:
            raise ValueError("capability requirement must include preferences")
        return value


class MatchedCapability(DeckrModel):
    """A capability selected by the controller for a binding requirement."""

    requirement_name: str = Field(alias="requirementName")
    capability: CapabilityRef
    family: str
    capability_type: str = Field(alias="type")
    direction: CapabilityDirection
    event_types: tuple[str, ...] = Field(default_factory=tuple, alias="eventTypes")
    command_types: tuple[str, ...] = Field(default_factory=tuple, alias="commandTypes")
    provenance: CapabilityProvenance = "native"
    source: CapabilityRef | None = None

    @field_validator("requirement_name")
    @classmethod
    def _validate_optional_text(cls, value: str) -> str:
        return _require_text(value, field_name="matched capability metadata")

    @field_validator("capability")
    @classmethod
    def _validate_capability(cls, value: CapabilityRef) -> CapabilityRef:
        return _require_bound_capability_ref(value, field_name="matched capability")

    @field_validator("family")
    @classmethod
    def _validate_family(cls, value: str) -> str:
        return _require_capability_family(value, field_name="capability family")

    @field_validator("capability_type")
    @classmethod
    def _validate_capability_type(cls, value: str) -> str:
        return _require_contract_name(value, field_name="capability type")

    @field_validator("event_types")
    @classmethod
    def _validate_event_types(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_contract_names(value, field_name="event type")

    @field_validator("command_types")
    @classmethod
    def _validate_command_types(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _validate_contract_names(value, field_name="command type")


class BindingMetadata(DeckrModel):
    """Action-provider-facing metadata for one controller-owned control binding."""

    provider_instance_id: str = Field(alias="providerInstanceId")
    provider_id: str = Field(alias="providerId")
    action_id: str = Field(alias="actionId")
    action_instance_id: str = Field(alias="actionInstanceId")
    config_id: str = Field(alias="configId")
    context_id: str = Field(alias="contextId")
    binding_id: str = Field(alias="bindingId")
    page_session_id: str | None = Field(default=None, alias="pageSessionId")
    device_ref: DeviceRef = Field(alias="deviceRef")
    control_ref: ControlRef = Field(alias="controlRef")
    item_key: str | None = Field(default=None, alias="itemKey")
    handler: str | None = None
    matched_capabilities: tuple[MatchedCapability, ...] = Field(
        default_factory=tuple,
        alias="matchedCapabilities",
    )
    output_generation: int = Field(default=0, alias="outputGeneration")

    @field_validator(
        "action_id",
        "action_instance_id",
        "config_id",
        "context_id",
        "binding_id",
        "provider_id",
        "provider_instance_id",
    )
    @classmethod
    def _validate_required_ids(cls, value: str) -> str:
        return _require_text(value, field_name="binding metadata id")

    @field_validator("provider_instance_id")
    @classmethod
    def _validate_provider_instance_id(cls, value: str) -> str:
        return require_provider_instance_id(value, field_name="providerInstanceId")

    @field_validator("page_session_id", "item_key", "handler")
    @classmethod
    def _validate_optional_ids(cls, value: str | None) -> str | None:
        return _require_optional_text(value, field_name="binding metadata id")

    @field_validator("output_generation")
    @classmethod
    def _validate_output_generation(cls, value: int) -> int:
        return _require_non_negative(value, field_name="output generation") or 0


class ActionInstanceMetadata(DeckrModel):
    """Action-provider-facing metadata for one controller-owned action instance."""

    provider_instance_id: str = Field(alias="providerInstanceId")
    provider_id: str = Field(alias="providerId")
    action_id: str = Field(alias="actionId")
    action_instance_id: str = Field(alias="actionInstanceId")
    config_id: str = Field(alias="configId")
    context_id: str = Field(alias="contextId")

    @field_validator(
        "provider_instance_id",
        "provider_id",
        "action_id",
        "action_instance_id",
        "config_id",
        "context_id",
    )
    @classmethod
    def _validate_required_ids(cls, value: str) -> str:
        return _require_text(value, field_name="action instance metadata id")

    @field_validator("provider_instance_id")
    @classmethod
    def _validate_provider_instance_id(cls, value: str) -> str:
        return require_provider_instance_id(value, field_name="providerInstanceId")


class PageSessionMetadata(DeckrModel):
    """Action-provider-facing metadata for one dynamic page session."""

    provider_instance_id: str = Field(alias="providerInstanceId")
    provider_id: str = Field(alias="providerId")
    action_instance_id: str = Field(alias="actionInstanceId")
    config_id: str = Field(alias="configId")
    page_id: str = Field(alias="pageId")
    page_session_id: str = Field(alias="pageSessionId")
    generation: int = Field(ge=0, strict=True)
    context_id: str = Field(alias="contextId")
    owner_binding_id: str | None = Field(default=None, alias="ownerBindingId")
    bindings: tuple[BindingMetadata, ...] = Field(default_factory=tuple)

    @field_validator(
        "action_instance_id",
        "config_id",
        "page_id",
        "page_session_id",
        "context_id",
        "provider_instance_id",
        "provider_id",
    )
    @classmethod
    def _validate_required_ids(cls, value: str) -> str:
        return _require_text(value, field_name="page session metadata id")

    @field_validator("provider_instance_id")
    @classmethod
    def _validate_provider_instance_id(cls, value: str) -> str:
        return require_provider_instance_id(value, field_name="providerInstanceId")

    @field_validator("owner_binding_id")
    @classmethod
    def _validate_optional_ids(cls, value: str | None) -> str | None:
        return _require_optional_text(value, field_name="page session metadata id")


class CapabilityInputEvent(DeckrModel):
    """Capability-oriented action input event delivered to an active binding."""

    capability: CapabilityRef
    event_type: str = Field(alias="eventType")
    value: JsonValue | None = None
    sequence: int | None = None
    occurred_at: datetime = Field(alias="occurredAt")
    producer: str | None = None
    source: CapabilityRef | None = None
    view: CapabilityViewKind | None = None

    @field_validator("capability")
    @classmethod
    def _validate_capability(cls, value: CapabilityRef) -> CapabilityRef:
        return _require_bound_capability_ref(value, field_name="input capability")

    @field_validator("event_type")
    @classmethod
    def _validate_event_type(cls, value: str) -> str:
        return _require_contract_name(value, field_name="input event type")

    @field_validator("sequence")
    @classmethod
    def _validate_sequence(cls, value: int | None) -> int | None:
        return _require_non_negative(value, field_name="sequence")

    @field_validator("producer")
    @classmethod
    def _validate_producer(cls, value: str | None) -> str | None:
        return _require_optional_text(value, field_name="producer")

    @field_validator("value", mode="before")
    @classmethod
    def _thaw_value(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("value", mode="after")
    @classmethod
    def _freeze_value(cls, value: Any) -> Any:
        return freeze_json(value)

    @field_serializer("value")
    def _serialize_value(self, value: Any) -> Any:
        return thaw_json(value)

    @field_serializer("occurred_at")
    def _serialize_occurred_at(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
