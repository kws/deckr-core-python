"""Lifecycle."""

from __future__ import annotations

from collections.abc import Mapping
from math import isfinite
from typing import Any

from pydantic import Field, field_serializer, field_validator, model_validator

from deckr.actions._body import (
    ActionLifecycleRejectionReason,
    ActionLifecycleRejectionTargetKind,
    ActionMessageBody,
    _require_bound_capability_ref,
    _require_contract_name,
    _require_non_negative,
    _require_optional_text,
)
from deckr.actions.bindings import (
    ActionInstanceMetadata,
    BindingMetadata,
    CapabilityInputEvent,
    PageSessionMetadata,
)
from deckr.actions.rendering import RenderingSupport
from deckr.contracts.models import JsonObject, freeze_json, thaw_json
from deckr.hardware.descriptors import CapabilityRef


class ActionInstanceLifecycleBody(ActionMessageBody):
    metadata: ActionInstanceMetadata
    reason: str | None = None


class BindingAttachedBody(ActionMessageBody):
    binding: BindingMetadata
    rendering: RenderingSupport | None = None

    @field_validator("rendering", mode="before")
    @classmethod
    def _rendering_not_null(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("rendering cannot be null")
        return value

    settings: JsonObject = Field(default_factory=dict)
    internal: JsonObject = Field(default_factory=dict)

    @field_validator("settings", "internal", mode="before")
    @classmethod
    def _thaw_json_object(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("settings", "internal", mode="after")
    @classmethod
    def _freeze_json_object(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return freeze_json(value)

    @field_serializer("settings", "internal")
    def _serialize_json_object(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)


class BindingDetachedBody(ActionMessageBody):
    binding: BindingMetadata
    reason: str


class PageSessionLifecycleBody(ActionMessageBody):
    page_session: PageSessionMetadata = Field(alias="pageSession")
    reason: str | None = None


class ActionLifecycleRejectedBody(ActionMessageBody):
    target_kind: ActionLifecycleRejectionTargetKind = Field(alias="targetKind")
    action_instance: ActionInstanceMetadata | None = Field(
        default=None,
        alias="actionInstance",
    )
    binding: BindingMetadata | None = None
    page_session: PageSessionMetadata | None = Field(
        default=None,
        alias="pageSession",
    )
    reason: ActionLifecycleRejectionReason
    message: str | None = None
    retryable: bool = False
    details: JsonObject = Field(default_factory=dict)

    @field_validator("message")
    @classmethod
    def _validate_message(cls, value: str | None) -> str | None:
        return _require_optional_text(value, field_name="rejection message")

    @field_validator("details", mode="before")
    @classmethod
    def _thaw_details(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("details", mode="after")
    @classmethod
    def _freeze_details(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return freeze_json(value)

    @field_serializer("details")
    def _serialize_details(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)

    @model_validator(mode="after")
    def _validate_target(self) -> ActionLifecycleRejectedBody:
        targets = {
            "action_instance": self.action_instance,
            "binding": self.binding,
            "page_session": self.page_session,
        }
        present = [kind for kind, target in targets.items() if target is not None]
        if present != [self.target_kind]:
            raise ValueError(
                "action lifecycle rejection requires exactly one target matching targetKind"
            )
        return self


class CapabilityInputBody(ActionMessageBody):
    binding: BindingMetadata
    event: CapabilityInputEvent


class BindingOutputBody(ActionMessageBody):
    binding: BindingMetadata
    capability: CapabilityRef
    command_type: str = Field(alias="commandType")
    params: JsonObject = Field(default_factory=dict)
    generation: int

    @field_validator("capability")
    @classmethod
    def _validate_capability(cls, value: CapabilityRef) -> CapabilityRef:
        return _require_bound_capability_ref(value, field_name="output capability")

    @field_validator("command_type")
    @classmethod
    def _validate_command_type(cls, value: str) -> str:
        return _require_contract_name(value, field_name="output command type")

    @field_validator("generation")
    @classmethod
    def _validate_generation(cls, value: int) -> int:
        return _require_non_negative(value, field_name="generation") or 0

    @field_validator("params", mode="before")
    @classmethod
    def _thaw_params(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("params", mode="after")
    @classmethod
    def _freeze_params(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return freeze_json(value)

    @field_serializer("params")
    def _serialize_params(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)


class BindingOverlayBody(ActionMessageBody):
    binding: BindingMetadata
    template: str
    title: str | None = None
    params: JsonObject = Field(default_factory=dict)
    duration_seconds: float | None = Field(default=None, alias="durationSeconds")
    overlay_id: str | None = Field(default=None, alias="overlayId")
    generation: int

    @field_validator("template")
    @classmethod
    def _validate_template(cls, value: str) -> str:
        return _require_contract_name(value, field_name="overlay template")

    @field_validator("title", "overlay_id")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        return _require_optional_text(value, field_name="overlay metadata")

    @field_validator("duration_seconds")
    @classmethod
    def _validate_duration(cls, value: float | None) -> float | None:
        if value is None:
            return None
        if not isfinite(value) or value <= 0:
            raise ValueError("durationSeconds must be a positive finite number")
        return value

    @field_validator("generation")
    @classmethod
    def _validate_generation(cls, value: int) -> int:
        return _require_non_negative(value, field_name="generation") or 0

    @field_validator("params", mode="before")
    @classmethod
    def _thaw_params(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("params", mode="after")
    @classmethod
    def _freeze_params(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return freeze_json(value)

    @field_serializer("params")
    def _serialize_params(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)


class BindingOverlayClearBody(ActionMessageBody):
    binding: BindingMetadata
    overlay_id: str | None = Field(default=None, alias="overlayId")
    generation: int

    @field_validator("overlay_id")
    @classmethod
    def _validate_overlay_id(cls, value: str | None) -> str | None:
        return _require_optional_text(value, field_name="overlay metadata")

    @field_validator("generation")
    @classmethod
    def _validate_generation(cls, value: int) -> int:
        return _require_non_negative(value, field_name="generation") or 0
