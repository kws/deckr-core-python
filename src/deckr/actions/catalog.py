"""Catalog."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import Field, field_serializer, field_validator, model_validator

from deckr.actions._body import (
    ActionAvailabilityStatus,
    _require_optional_text,
    _require_text,
)
from deckr.actions.bindings import CapabilityRequirement
from deckr.contracts.models import DeckrModel, JsonObject, freeze_json, thaw_json


class ActionDescriptor(DeckrModel):
    """Action identity advertised by an action provider."""

    action_id: str = Field(alias="actionId")
    name: str | None = None
    provider_id: str | None = Field(default=None, alias="providerId")
    requirements: tuple[CapabilityRequirement, ...] | None = None
    controllers: tuple[str, ...] | None = None
    property_inspector_path: str | None = None
    manifest_defaults: JsonObject | None = None
    settings_schema: JsonObject | None = Field(default=None, alias="settingsSchema")
    provider_settings_schema: JsonObject | None = Field(
        default=None,
        alias="providerSettingsSchema",
    )

    @field_validator("action_id")
    @classmethod
    def _validate_action_id(cls, value: str) -> str:
        return _require_text(value, field_name="action id")

    @field_validator("provider_id")
    @classmethod
    def _validate_provider_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_text(value, field_name="provider id")

    @field_validator("requirements", mode="after")
    @classmethod
    def _validate_requirements(
        cls,
        value: tuple[CapabilityRequirement, ...] | None,
    ) -> tuple[CapabilityRequirement, ...] | None:
        if value is None:
            return None
        names = [requirement.name for requirement in value]
        duplicates = {name for name in names if names.count(name) > 1}
        if duplicates:
            raise ValueError(
                "action requirement names must be unique: "
                + ", ".join(sorted(duplicates))
            )
        return value

    @field_validator(
        "manifest_defaults",
        "settings_schema",
        "provider_settings_schema",
        mode="before",
    )
    @classmethod
    def _thaw_json_object(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator(
        "manifest_defaults",
        "settings_schema",
        "provider_settings_schema",
        mode="after",
    )
    @classmethod
    def _freeze_json_object(
        cls,
        value: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        return freeze_json(value) if value is not None else None

    @field_serializer(
        "manifest_defaults", "settings_schema", "provider_settings_schema"
    )
    def _serialize_json_object(
        self,
        value: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        return thaw_json(value) if value is not None else None

    def to_dict(self) -> dict[str, Any]:
        """Serialize for action registration payloads."""
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


class ActionAvailabilitySelector(DeckrModel):
    """Controller request selector for one provider action."""

    action_id: str = Field(alias="actionId")

    @field_validator("action_id")
    @classmethod
    def _validate_action_id(cls, value: str) -> str:
        return _require_text(value, field_name="availability action id")


class ActionAvailabilityEntry(DeckrModel):
    """Provider-direct availability for one action descriptor."""

    action_id: str = Field(alias="actionId")
    status: ActionAvailabilityStatus
    descriptor: ActionDescriptor | None = None
    reason: str | None = None

    @field_validator("action_id")
    @classmethod
    def _validate_action_id(cls, value: str) -> str:
        return _require_text(value, field_name="availability action id")

    @field_validator("reason")
    @classmethod
    def _validate_reason(cls, value: str | None) -> str | None:
        return _require_optional_text(value, field_name="availability reason")

    @model_validator(mode="after")
    def _validate_descriptor_identity(self) -> ActionAvailabilityEntry:
        if self.status == "available" and self.descriptor is None:
            raise ValueError("available action availability requires descriptor")
        if self.descriptor is not None and self.descriptor.action_id != self.action_id:
            raise ValueError(
                "availability descriptor actionId must match entry actionId"
            )
        return self
