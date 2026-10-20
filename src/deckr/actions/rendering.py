"""Action Runtime rendering wire values; rendering engines belong to controllers."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Annotated, Any, Literal

from pydantic import Field, field_serializer, field_validator, model_validator

from deckr.actions._body import ActionMessageBody
from deckr.actions.pages import BindingCommandTarget
from deckr.contracts.models import DeckrModel, JsonObject, freeze_json, thaw_json

RENDER_TEXT_FORMAT = "dev.deckr.render.text.v1"
RENDER_IMAGE_FORMAT = "dev.deckr.render.image.v1"
_EXTENSION = re.compile(
    r"^(?!dev\.deckr\.)[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)+\.v[1-9][0-9]*$"
)
_Text = Annotated[str, Field(min_length=1)]
_Positive = Annotated[int, Field(gt=0, strict=True)]
_Color = Annotated[str, Field(pattern=r"^#[0-9a-fA-F]{6}$")]


class _RenderModel(DeckrModel):
    @field_validator("targets", "formats", mode="before", check_fields=False)
    @classmethod
    def _wire_arrays(cls, value: Any) -> Any:
        return tuple(value) if isinstance(value, list) else value

    @model_validator(mode="before")
    @classmethod
    def _no_explicit_null(cls, value: Any) -> Any:
        if isinstance(value, Mapping) and any(v is None for v in value.values()):
            raise ValueError("rendering fields cannot be null")
        return value


class RenderLimits(_RenderModel):
    max_message_bytes: _Positive
    max_image_bytes: _Positive
    max_image_pixels: _Positive
    max_text_code_points: _Positive
    max_render_time_ms: _Positive


class RenderText(_RenderModel):
    text: str
    foreground: _Color = "#FFFFFF"
    background: _Color = "#000000"


class RenderImage(_RenderModel):
    image: _Text
    encoding: Literal["png", "jpeg"]
    fit: Literal["contain", "cover", "stretch"] = "contain"
    background: _Color = "#000000"


class RenderFormat(_RenderModel):
    id: _Text
    options: JsonObject | None = None

    @model_validator(mode="after")
    def _format(self) -> RenderFormat:
        if self.id == RENDER_TEXT_FORMAT:
            if self.options is not None:
                raise ValueError("text format has no options")
        elif self.id == RENDER_IMAGE_FORMAT:
            if self.options is None or set(self.options) != {"encodings"}:
                raise ValueError("image format requires encodings")
            encodings = self.options["encodings"]
            if (
                not isinstance(encodings, list | tuple)
                or any(e not in ("png", "jpeg") for e in encodings)
                or "png" not in encodings
                or len(encodings) != len(set(encodings))
            ):
                raise ValueError(
                    "image encodings require PNG and unique supported names"
                )
        elif not _EXTENSION.fullmatch(self.id):
            raise ValueError("unsupported core format or invalid extension ID")
        return self

    @field_validator("options", mode="before")
    @classmethod
    def _thaw_options(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("options", mode="after")
    @classmethod
    def _freeze_options(cls, value: Any) -> Any:
        return freeze_json(value) if value is not None else None

    @field_serializer("options")
    def _serialize_options(self, value: Any) -> Any:
        return thaw_json(value)


class RenderTargetSupport(_RenderModel):
    requirement_name: _Text
    width: _Positive
    height: _Positive
    formats: tuple[RenderFormat, ...]
    limits: RenderLimits

    @model_validator(mode="after")
    def _formats(self) -> RenderTargetSupport:
        ids = [f.id for f in self.formats]
        if len(ids) != len(set(ids)) or not {
            RENDER_TEXT_FORMAT,
            RENDER_IMAGE_FORMAT,
        } <= set(ids):
            raise ValueError(
                "render targets require unique formats and both baseline formats"
            )
        return self


class RenderingSupport(_RenderModel):
    version: Annotated[int, Field(strict=True, ge=1, le=1)]
    targets: tuple[RenderTargetSupport, ...]

    @model_validator(mode="after")
    def _targets(self) -> RenderingSupport:
        names = [t.requirement_name for t in self.targets]
        if len(names) != len(set(names)):
            raise ValueError("duplicate render target requirement")
        return self


class RenderContent(_RenderModel):
    format: _Text
    data: JsonObject

    @field_validator("data", mode="before")
    @classmethod
    def _thaw_data(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("data", mode="after")
    @classmethod
    def _freeze_data(cls, value: Any) -> Any:
        return freeze_json(value)

    @field_serializer("data")
    def _serialize_data(self, value: Any) -> Any:
        return thaw_json(value)

    @model_validator(mode="after")
    def _content(self) -> RenderContent:
        if self.format == RENDER_TEXT_FORMAT:
            RenderText.from_wire(thaw_json(self.data))
        elif self.format == RENDER_IMAGE_FORMAT:
            RenderImage.from_wire(thaw_json(self.data))
        elif not _EXTENSION.fullmatch(self.format):
            raise ValueError("unsupported core format or invalid extension ID")
        return self


class BindingRenderBody(_RenderModel, ActionMessageBody):
    target: BindingCommandTarget
    requirement_name: _Text
    operation: Literal["set", "clear"]
    content: RenderContent | None = None

    @model_validator(mode="after")
    def _operation(self) -> BindingRenderBody:
        if (self.operation == "set") != (self.content is not None):
            raise ValueError("set requires content; clear prohibits content")
        return self


RENDER_RESULT_CODES = {
    ("controller", "dispatched"): frozenset(),
    ("controller", "rejected"): frozenset(
        {
            "invalid_content",
            "unsupported_format",
            "unsupported_target",
            "target_not_current",
            "limit_exceeded",
            "busy",
        }
    ),
    ("controller", "failed"): frozenset(
        {
            "invalid_content",
            "limit_exceeded",
            "render_failed",
            "deadline_exceeded",
            "dispatch_failed",
        }
    ),
    ("controller", "superseded"): frozenset({"newer_output", "target_not_current"}),
    ("hardware", "rejected"): frozenset({"hardware_rejected"}),
}


class BindingRenderResultBody(_RenderModel, ActionMessageBody):
    request_message_id: _Text
    target: BindingCommandTarget
    requirement_name: _Text
    stage: Literal["controller", "hardware"]
    status: Literal["dispatched", "rejected", "failed", "superseded"]
    command_message_id: _Text | None = None
    code: _Text | None = None
    reason: Annotated[str, Field(max_length=512)] | None = None

    @model_validator(mode="after")
    def _result(self) -> BindingRenderResultBody:
        codes = RENDER_RESULT_CODES.get((self.stage, self.status))
        if codes is None or (
            self.code is not None if not codes else self.code not in codes
        ):
            raise ValueError("invalid render result stage, status, or code")
        needs_command = self.status == "dispatched" or self.stage == "hardware"
        if needs_command != (self.command_message_id is not None):
            raise ValueError("invalid hardware command correlation")
        return self
