"""Settings."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import Field, field_serializer, field_validator, model_validator

from deckr.actions._body import (
    SettingsProvenance,
    SettingsScope,
    _require_optional_text,
    _require_text,
)
from deckr.actions.endpoints import require_provider_instance_id
from deckr.contracts.keys import decode_key_token, encode_key_token
from deckr.contracts.models import DeckrModel, JsonObject, freeze_json, thaw_json


class SettingsSchemaMetadata(DeckrModel):
    """Schema metadata attached to editable settings targets."""

    schema_id: str | None = Field(default=None, alias="schemaId")
    json_schema: JsonObject | None = Field(default=None, alias="schema")
    stale: bool = False

    @field_validator("json_schema", mode="before")
    @classmethod
    def _thaw_schema(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("json_schema", mode="after")
    @classmethod
    def _freeze_schema(
        cls, value: Mapping[str, Any] | None
    ) -> Mapping[str, Any] | None:
        return freeze_json(value) if value is not None else None

    @field_serializer("json_schema")
    def _serialize_schema(
        self, value: Mapping[str, Any] | None
    ) -> dict[str, Any] | None:
        return thaw_json(value) if value is not None else None


class SettingsTargetRef(DeckrModel):
    """Durable settings identity independent of live context or binding handles."""

    scope: SettingsScope
    controller_id: str = Field(alias="controllerId")
    config_id: str = Field(alias="configId")
    provider_instance_id: str = Field(alias="providerInstanceId")
    provider_id: str = Field(alias="providerId")
    action_id: str | None = Field(default=None, alias="actionId")
    action_instance_id: str | None = Field(default=None, alias="actionInstanceId")
    stable_id: str | None = Field(default=None, alias="stableId")

    @field_validator(
        "controller_id",
        "config_id",
        "provider_id",
    )
    @classmethod
    def _validate_required_ids(cls, value: str) -> str:
        return _require_text(value, field_name="settings target id")

    @field_validator("provider_instance_id")
    @classmethod
    def _validate_provider_instance_id(cls, value: str) -> str:
        return require_provider_instance_id(value, field_name="providerInstanceId")

    @field_validator("action_id", "action_instance_id", "stable_id")
    @classmethod
    def _validate_optional_ids(cls, value: str | None) -> str | None:
        return _require_optional_text(value, field_name="settings target id")

    @model_validator(mode="after")
    def _validate_scope_fields(self) -> SettingsTargetRef:
        if self.scope == "action_provider_instance":
            if self.action_id or self.action_instance_id or self.stable_id:
                raise ValueError(
                    "action provider instance settings target must not include action ids"
                )
        if self.scope == "action_instance":
            missing = [
                name
                for name, value in {
                    "actionId": self.action_id,
                    "actionInstanceId": self.action_instance_id,
                }.items()
                if not value
            ]
            if missing:
                raise ValueError(
                    "action instance settings target missing: " + ", ".join(missing)
                )
        return self

    def key(self) -> str:
        parts = [
            "settings",
            "target",
            encode_key_token(self.scope),
            encode_key_token(self.controller_id),
            encode_key_token(self.config_id),
            encode_key_token(self.provider_instance_id),
            encode_key_token(self.provider_id),
        ]
        if self.scope == "action_instance":
            parts.extend(
                [
                    encode_key_token(self.action_id or ""),
                    encode_key_token(self.action_instance_id or ""),
                    "1" if self.stable_id is not None else "0",
                ]
            )
            if self.stable_id is not None:
                parts.append(encode_key_token(self.stable_id))
        return ".".join(parts)

    def to_dict(self) -> dict[str, Any]:
        """Serialize for settings request payloads."""
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


def settings_target_key(target: SettingsTargetRef) -> str:
    return target.key()


def parse_settings_target_key(key: str) -> SettingsTargetRef | None:
    parts = key.split(".")
    if len(parts) < 7 or parts[:2] != ["settings", "target"]:
        return None
    try:
        scope = decode_key_token(parts[2])
        controller_id = decode_key_token(parts[3])
        config_id = decode_key_token(parts[4])
        provider_instance_id = decode_key_token(parts[5])
        provider_id = decode_key_token(parts[6])
        if scope == "action_provider_instance" and len(parts) == 7:
            return SettingsTargetRef(
                scope="action_provider_instance",
                controllerId=controller_id,
                configId=config_id,
                providerInstanceId=provider_instance_id,
                providerId=provider_id,
            )
        if scope != "action_instance":
            return None
        if len(parts) not in {10, 11}:
            return None
        stable_flag = parts[9]
        if stable_flag == "0":
            if len(parts) != 10:
                return None
            stable_id = None
        elif stable_flag == "1":
            if len(parts) != 11:
                return None
            stable_id = decode_key_token(parts[10])
        else:
            return None
        return SettingsTargetRef(
            scope="action_instance",
            controllerId=controller_id,
            configId=config_id,
            providerInstanceId=provider_instance_id,
            providerId=provider_id,
            actionId=decode_key_token(parts[7]),
            actionInstanceId=decode_key_token(parts[8]),
            stableId=stable_id,
        )
    except ValueError:
        return None


class SettingsTargetDescription(DeckrModel):
    """Editor-facing description of one editable settings target."""

    target: SettingsTargetRef
    provider_instance_id: str = Field(alias="providerInstanceId")
    provider_id: str = Field(alias="providerId")
    action_id: str | None = Field(default=None, alias="actionId")
    label: str | None = None
    placement: JsonObject = Field(default_factory=dict)
    schema_metadata: SettingsSchemaMetadata = Field(
        default_factory=SettingsSchemaMetadata,
        alias="schemaMetadata",
    )
    provenance: tuple[SettingsProvenance, ...] = Field(default_factory=tuple)

    @field_validator("provider_id")
    @classmethod
    def _validate_provider_id(cls, value: str) -> str:
        return _require_text(value, field_name="settings target provider id")

    @field_validator("provider_instance_id")
    @classmethod
    def _validate_provider_instance_id(cls, value: str) -> str:
        return require_provider_instance_id(value, field_name="providerInstanceId")

    @field_validator("action_id", "label")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        return _require_optional_text(value, field_name="settings target description")

    @model_validator(mode="after")
    def _validate_target_mirror(self) -> SettingsTargetDescription:
        if self.provider_instance_id != self.target.provider_instance_id:
            raise ValueError(
                "settings target description providerInstanceId must match target"
            )
        if self.provider_id != self.target.provider_id:
            raise ValueError("settings target description providerId must match target")
        if self.action_id != self.target.action_id:
            raise ValueError("settings target description actionId must match target")
        return self

    @field_validator("placement", mode="before")
    @classmethod
    def _thaw_placement(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("placement", mode="after")
    @classmethod
    def _freeze_placement(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return freeze_json(value)

    @field_serializer("placement")
    def _serialize_placement(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)
