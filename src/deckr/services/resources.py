"""Typed resource descriptions and observations over managed service views."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from deckr.contracts.models import thaw_json
from deckr.core.json_schema import local_validator

ResourceText = Annotated[str, Field(min_length=1, pattern=r"^\S(?:[\s\S]*\S)?$")]


class ResourceModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)

    @model_validator(mode="before")
    @classmethod
    def json_only(cls, value):
        if isinstance(value, dict):

            def encode(model):
                if isinstance(model, ResourceModel):
                    return model.to_dict()
                raise ValueError("Resource values must be JSON values")

            json.dumps(value, allow_nan=False, default=encode)
        return value

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


class ValueSchema(ResourceModel):
    """A bounded, local JSON Schema subset; no remote references or coercion."""

    type: Literal["boolean", "number", "integer", "string", "object", "array"]
    minimum: float | None = None
    maximum: float | None = None
    multipleOf: Annotated[float, Field(gt=0)] | None = None
    minLength: Annotated[int, Field(ge=0)] | None = None
    maxLength: Annotated[int, Field(ge=0)] | None = None
    enum: Annotated[list[Any], Field(min_length=1)] | None = None
    properties: dict[str, ValueSchema] | None = None
    required: list[ResourceText] | None = None
    items: ValueSchema | None = None
    maxItems: Annotated[int, Field(ge=0)] | None = None

    @model_validator(mode="after")
    def coherent(self):
        if self.minimum is not None and self.maximum is not None:
            if self.minimum > self.maximum:
                raise ValueError("minimum exceeds maximum")
        if self.required and not set(self.required) <= set(self.properties or {}):
            raise ValueError("Required fields must have declared schemas")
        if self.type == "array" and self.items is None:
            raise ValueError("Array values require an item schema")
        if self.enum is not None:
            unique = {json.dumps(v, sort_keys=True, allow_nan=False) for v in self.enum}
            if len(unique) != len(self.enum):
                raise ValueError("Duplicate enum value")
        return self

    def json_schema(self) -> dict[str, Any]:
        result = self.to_dict()
        if self.type == "object":
            result["additionalProperties"] = False
            result["properties"] = {
                key: schema.json_schema()
                for key, schema in (self.properties or {}).items()
            }
        if self.items is not None:
            result["items"] = self.items.json_schema()
        return result

    def validate_value(self, value: Any) -> None:
        json.dumps(value, allow_nan=False)
        local_validator(self.json_schema()).validate(value)


class PropertyDescriptor(ResourceModel):
    valueSchema: ValueSchema
    writable: bool
    unit: ResourceText | None = None
    semantic: ResourceText | None = None


class ActionDescriptor(ResourceModel):
    paramsSchema: ValueSchema
    confirmation: Literal["acceptance", "observation"]

    @model_validator(mode="after")
    def object_params(self):
        if self.paramsSchema.type != "object":
            raise ValueError("Action parameters must be an object")
        return self


class ResourceDescriptor(ResourceModel):
    resourceId: ResourceText
    title: ResourceText
    profile: ResourceText
    room: ResourceText | None = None
    properties: dict[ResourceText, PropertyDescriptor]
    actions: dict[ResourceText, ActionDescriptor]


class PropertyObservation(ResourceModel):
    quality: Literal["known", "unknown", "unavailable", "invalid"]
    value: Any = None
    reason: ResourceText | None = None
    assumed: bool | None = None

    @model_validator(mode="after")
    def value_quality(self):
        present = "value" in self.model_fields_set
        if (self.quality == "known") != present:
            raise ValueError("Only known observations must contain a value")
        return self


class ResourceSnapshot(ResourceModel):
    resourceId: ResourceText
    revision: ResourceText
    observedAt: ResourceText
    descriptor: ResourceDescriptor
    properties: dict[ResourceText, PropertyObservation]

    @model_validator(mode="after")
    def coherent(self):
        stamp = datetime.fromisoformat(self.observedAt.replace("Z", "+00:00"))
        if stamp.tzinfo is None:
            raise ValueError("observedAt requires a timezone")
        if self.resourceId != self.descriptor.resourceId:
            raise ValueError("Resource descriptor identity mismatch")
        if set(self.properties) != set(self.descriptor.properties):
            raise ValueError("Every described property requires an observation")
        for name, observation in self.properties.items():
            if observation.quality == "known":
                self.descriptor.properties[name].valueSchema.validate_value(
                    observation.value
                )
        return self

    @classmethod
    def from_view(cls, payload):
        """Read the domain payload after the managed client checked view fences."""
        return cls.model_validate(
            thaw_json({k: payload[k] for k in cls.model_fields if k in payload})
        )
