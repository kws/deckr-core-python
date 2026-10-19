"""Message bodies."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import Field, JsonValue, field_serializer, field_validator

from deckr.contracts.models import DeckrModel, JsonObject, freeze_json, thaw_json
from deckr.hardware.descriptors import DeviceRef, DeviceSourceReference

CONTROL_INPUT = "controlInput"


CONTROL_COMMAND = "controlCommand"


CAPABILITY_STATE_CHANGED = "capabilityStateChanged"


CAPABILITY_STATE_REQUEST = "capabilityStateRequest"


CAPABILITY_STATE_REPLY = "capabilityStateReply"


COMMAND_ACCEPTED = "commandAccepted"


COMMAND_REJECTED = "commandRejected"


COMMAND_REPLY = "commandReply"


CommandRejectionReason = Literal[
    "malformed",
    "unsupported",
    "expired",
    "unauthorized",
    "stale",
    "rejected",
]


CapabilityStateStatus = Literal["ok", "unavailable", "unsupported", "rejected"]


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _require_non_empty(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _require_optional_non_empty(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_non_empty(value, field_name=field_name)


def _require_non_negative(value: int | None, *, field_name: str) -> int | None:
    if value is not None and value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


class ControlInputMessage(DeckrModel):
    device_ref: DeviceRef = Field(alias="deviceRef")
    control_id: str = Field(alias="controlId")
    capability_id: str = Field(alias="capabilityId")
    event_type: str = Field(alias="eventType")
    value: JsonValue | None = None
    sequence: int | None = None
    occurred_at: datetime = Field(default_factory=_now_utc, alias="occurredAt")
    sources: tuple[DeviceSourceReference, ...] = Field(default_factory=tuple)

    @field_validator("control_id", "capability_id", "event_type")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _require_non_empty(value, field_name="control input target")

    @field_validator("sequence")
    @classmethod
    def _validate_sequence(cls, value: int | None) -> int | None:
        return _require_non_negative(value, field_name="sequence")

    @field_validator("value", mode="after")
    @classmethod
    def _freeze_value(cls, value: JsonValue | None) -> Any:
        if value is None:
            return None
        return freeze_json(value)

    @field_serializer("value")
    def _serialize_value(self, value: Any) -> Any:
        return thaw_json(value)

    @field_serializer("occurred_at")
    def _serialize_occurred_at(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


class ControlCommandMessage(DeckrModel):
    device_ref: DeviceRef = Field(alias="deviceRef")
    control_id: str | None = Field(default=None, alias="controlId")
    capability_id: str = Field(alias="capabilityId")
    command_type: str = Field(alias="commandType")
    params: JsonObject = Field(default_factory=dict)

    @field_validator("capability_id", "command_type")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _require_non_empty(value, field_name="control command target")

    @field_validator("control_id")
    @classmethod
    def _validate_control_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_non_empty(value, field_name="control command target")

    @field_validator("params", mode="after")
    @classmethod
    def _freeze_params(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return freeze_json(value)

    @field_serializer("params")
    def _serialize_params(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)


class CapabilityStateChangedMessage(DeckrModel):
    device_ref: DeviceRef = Field(alias="deviceRef")
    capability_id: str = Field(alias="capabilityId")
    value: JsonValue | None = None
    control_id: str | None = Field(default=None, alias="controlId")
    state_type: str | None = Field(default=None, alias="stateType")
    sequence: int | None = None
    occurred_at: datetime = Field(default_factory=_now_utc, alias="occurredAt")

    @field_validator("capability_id")
    @classmethod
    def _validate_capability_id(cls, value: str) -> str:
        return _require_non_empty(value, field_name="capability state target")

    @field_validator("control_id", "state_type")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        return _require_optional_non_empty(value, field_name="capability state target")

    @field_validator("sequence")
    @classmethod
    def _validate_sequence(cls, value: int | None) -> int | None:
        return _require_non_negative(value, field_name="sequence")

    @field_validator("value", mode="after")
    @classmethod
    def _freeze_value(cls, value: JsonValue | None) -> Any:
        if value is None:
            return None
        return freeze_json(value)

    @field_serializer("value")
    def _serialize_value(self, value: Any) -> Any:
        return thaw_json(value)

    @field_serializer("occurred_at")
    def _serialize_occurred_at(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


class CapabilityStateRequestMessage(DeckrModel):
    device_ref: DeviceRef = Field(alias="deviceRef")
    capability_id: str = Field(alias="capabilityId")
    control_id: str | None = Field(default=None, alias="controlId")
    state_type: str | None = Field(default=None, alias="stateType")
    params: JsonObject = Field(default_factory=dict)

    @field_validator("capability_id")
    @classmethod
    def _validate_capability_id(cls, value: str) -> str:
        return _require_non_empty(value, field_name="capability state target")

    @field_validator("control_id", "state_type")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        return _require_optional_non_empty(value, field_name="capability state target")

    @field_validator("params", mode="after")
    @classmethod
    def _freeze_params(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        return freeze_json(value)

    @field_serializer("params")
    def _serialize_params(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)


class CapabilityStateReplyMessage(DeckrModel):
    device_ref: DeviceRef = Field(alias="deviceRef")
    capability_id: str = Field(alias="capabilityId")
    status: CapabilityStateStatus = "ok"
    value: JsonValue | None = None
    control_id: str | None = Field(default=None, alias="controlId")
    state_type: str | None = Field(default=None, alias="stateType")
    error: str | None = None

    @field_validator("capability_id")
    @classmethod
    def _validate_capability_id(cls, value: str) -> str:
        return _require_non_empty(value, field_name="capability state target")

    @field_validator("control_id", "state_type", "error")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        return _require_optional_non_empty(value, field_name="capability state reply")

    @field_validator("value", mode="after")
    @classmethod
    def _freeze_value(cls, value: JsonValue | None) -> Any:
        if value is None:
            return None
        return freeze_json(value)

    @field_serializer("value")
    def _serialize_value(self, value: Any) -> Any:
        return thaw_json(value)


class CommandAcceptedMessage(DeckrModel):
    device_ref: DeviceRef = Field(alias="deviceRef")
    control_id: str | None = Field(default=None, alias="controlId")
    capability_id: str = Field(alias="capabilityId")
    command_type: str = Field(alias="commandType")
    accepted_at: datetime = Field(default_factory=_now_utc, alias="acceptedAt")

    @field_validator("capability_id", "command_type")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _require_non_empty(value, field_name="command acknowledgement")

    @field_validator("control_id")
    @classmethod
    def _validate_control_id(cls, value: str | None) -> str | None:
        return _require_optional_non_empty(value, field_name="command acknowledgement")

    @field_serializer("accepted_at")
    def _serialize_accepted_at(self, value: datetime) -> str:
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


class CommandRejectedMessage(DeckrModel):
    device_ref: DeviceRef = Field(alias="deviceRef")
    control_id: str | None = Field(default=None, alias="controlId")
    capability_id: str = Field(alias="capabilityId")
    command_type: str = Field(alias="commandType")
    reason: CommandRejectionReason
    message: str | None = None

    @field_validator("capability_id", "command_type")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _require_non_empty(value, field_name="command rejection")

    @field_validator("control_id", "message")
    @classmethod
    def _validate_optional_text(cls, value: str | None) -> str | None:
        return _require_optional_non_empty(value, field_name="command rejection")


class CommandReplyMessage(DeckrModel):
    device_ref: DeviceRef = Field(alias="deviceRef")
    control_id: str | None = Field(default=None, alias="controlId")
    capability_id: str = Field(alias="capabilityId")
    command_type: str = Field(alias="commandType")
    result: JsonValue | None = None

    @field_validator("capability_id", "command_type")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        return _require_non_empty(value, field_name="command reply")

    @field_validator("control_id")
    @classmethod
    def _validate_control_id(cls, value: str | None) -> str | None:
        return _require_optional_non_empty(value, field_name="command reply")

    @field_validator("result", mode="after")
    @classmethod
    def _freeze_result(cls, value: JsonValue | None) -> Any:
        if value is None:
            return None
        return freeze_json(value)

    @field_serializer("result")
    def _serialize_result(self, value: Any) -> Any:
        return thaw_json(value)


HardwareMessageBody = (
    ControlInputMessage
    | ControlCommandMessage
    | CapabilityStateChangedMessage
    | CapabilityStateRequestMessage
    | CapabilityStateReplyMessage
    | CommandAcceptedMessage
    | CommandRejectedMessage
    | CommandReplyMessage
)


HARDWARE_BODY_BY_MESSAGE_TYPE: dict[str, type[HardwareMessageBody]] = {
    CONTROL_INPUT: ControlInputMessage,
    CONTROL_COMMAND: ControlCommandMessage,
    CAPABILITY_STATE_CHANGED: CapabilityStateChangedMessage,
    CAPABILITY_STATE_REQUEST: CapabilityStateRequestMessage,
    CAPABILITY_STATE_REPLY: CapabilityStateReplyMessage,
    COMMAND_ACCEPTED: CommandAcceptedMessage,
    COMMAND_REJECTED: CommandRejectedMessage,
    COMMAND_REPLY: CommandReplyMessage,
}


HARDWARE_MESSAGE_TYPE_BY_BODY = {
    body_type: message_type
    for message_type, body_type in HARDWARE_BODY_BY_MESSAGE_TYPE.items()
}


_HARDWARE_BODY_TYPES = tuple(HARDWARE_BODY_BY_MESSAGE_TYPE.values())
