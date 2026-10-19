"""Pages."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import Any

from pydantic import Field, field_serializer, field_validator, model_validator

from deckr.actions._body import (
    ActionMessageBody,
    PageChildBindingTargetKind,
    _require_optional_text,
    _require_text,
)
from deckr.actions.endpoints import require_provider_instance_id
from deckr.contracts.models import DeckrModel, JsonObject, freeze_json, thaw_json


class PageChildBindingTarget(DeckrModel):
    """Action target for one dynamic-page child binding."""

    kind: PageChildBindingTargetKind
    action_id: str | None = Field(default=None, alias="actionId")
    provider_instance_id: str | None = Field(default=None, alias="providerInstanceId")
    provider_labels: Mapping[str, str] | None = Field(
        default=None,
        alias="providerLabels",
    )
    instance_key: str | None = Field(default=None, alias="instanceKey")

    @field_validator("kind")
    @classmethod
    def _validate_kind(cls, value: str) -> str:
        return _require_text(value, field_name="page child target kind")

    @field_validator("action_id", "instance_key")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        return _require_optional_text(value, field_name="page child target")

    @field_validator("provider_instance_id")
    @classmethod
    def _validate_provider_instance_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return require_provider_instance_id(value, field_name="providerInstanceId")

    @field_validator("provider_labels", mode="after")
    @classmethod
    def _validate_provider_labels(
        cls,
        value: Mapping[str, str] | None,
    ) -> Mapping[str, str] | None:
        if value is None:
            return None
        return freeze_json(
            {
                _require_text(key, field_name="provider label key"): _require_text(
                    item,
                    field_name="provider label value",
                )
                for key, item in value.items()
            }
        )

    @field_serializer("provider_labels")
    def _serialize_provider_labels(
        self,
        value: Mapping[str, str] | None,
    ) -> dict[str, str] | None:
        if value is None:
            return None
        return thaw_json(value)

    @model_validator(mode="after")
    def _validate_target_fields(self) -> PageChildBindingTarget:
        if self.kind == "self":
            if (
                self.action_id is not None
                or self.provider_instance_id is not None
                or self.provider_labels
                or self.instance_key is not None
            ):
                raise ValueError(
                    "self page child target must not include action selector fields"
                )
            return self

        if self.kind == "action":
            if self.action_id is None:
                raise ValueError("action page child target requires actionId")
            return self

        raise ValueError("page child target kind must be 'self' or 'action'")


class PageChildBindingDescriptor(DeckrModel):
    """One concrete child binding requested for a dynamic page session."""

    control_id: str = Field(alias="controlId")
    target: PageChildBindingTarget
    item_key: str | None = Field(default=None, alias="itemKey")
    handler: str | None = None
    settings: JsonObject = Field(default_factory=dict)
    internal: JsonObject = Field(default_factory=dict)

    @field_validator("control_id")
    @classmethod
    def _validate_control_id(cls, value: str) -> str:
        return _require_text(value, field_name="page child control id")

    @field_validator("item_key", "handler")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_text(value, field_name="page child metadata")

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


class DynamicPageCommand(DeckrModel):
    """Concrete page-session command resolved by the controller."""

    page_id: str = Field(alias="pageId")
    bindings: tuple[PageChildBindingDescriptor, ...]

    @field_validator("page_id")
    @classmethod
    def _validate_page_id(cls, value: str) -> str:
        return _require_text(value, field_name="dynamic page id")

    @field_validator("bindings", mode="after")
    @classmethod
    def _validate_bindings(
        cls, value: tuple[PageChildBindingDescriptor, ...]
    ) -> tuple[PageChildBindingDescriptor, ...]:
        if not value:
            raise ValueError("dynamic page command must include child bindings")
        control_ids = [binding.control_id for binding in value]
        duplicates = {
            control_id
            for control_id in control_ids
            if control_ids.count(control_id) > 1
        }
        if duplicates:
            raise ValueError(
                "dynamic page child control ids must be unique: "
                + ", ".join(sorted(duplicates))
            )
        return value

    def to_dict(self) -> dict[str, Any]:
        """Serialize for action command payloads."""
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


class OpenPageBody(ActionMessageBody):
    descriptor: DynamicPageCommand


class ReplacePageBody(ActionMessageBody):
    descriptor: DynamicPageCommand


def make_dynamic_page_id() -> str:
    """Generate a unique page ID for dynamic pages."""
    return str(uuid.uuid4())
