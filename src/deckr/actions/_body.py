"""Body."""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from typing import Any, Literal

from pydantic import Field, field_serializer, field_validator

from deckr.contracts.models import DeckrModel, JsonObject, freeze_json, thaw_json
from deckr.hardware.descriptors import CORE_CAPABILITY_FAMILIES, CapabilityRef

_RESERVED_EXTENSION_DATA_FIELDS = frozenset(
    {
        "actionId",
        "actionInstanceId",
        "actionUuid",
        "action_uuid",
        "bindingId",
        "capabilityId",
        "configId",
        "controlId",
        "controlRef",
        "contextId",
        "deviceRef",
        "providerInstanceId",
        "pageSessionId",
        "providerId",
        "uuid",
    }
)


_CONTRACT_NAME_PATTERN = r"^[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)*$"


_GLOBALLY_QUALIFIED_NAME_PATTERN = r"^[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)+$"


_EXTENSION_CAPABILITY_FAMILY_PATTERN = (
    r"^(?!dev\.deckr\.)[a-z][a-z0-9_-]*(?:\.[a-z0-9][a-z0-9_-]*)+$"
)


_CONTRACT_NAME_RE = re.compile(_CONTRACT_NAME_PATTERN)


_GLOBALLY_QUALIFIED_NAME_RE = re.compile(_GLOBALLY_QUALIFIED_NAME_PATTERN)


_EXTENSION_CAPABILITY_FAMILY_RE = re.compile(_EXTENSION_CAPABILITY_FAMILY_PATTERN)


def _reserved_extension_data_paths(
    value: Any,
    *,
    path: str = "data",
) -> tuple[str, ...]:
    if isinstance(value, Mapping):
        paths: list[str] = []
        for key, item in value.items():
            child_path = f"{path}.{key}"
            if key in _RESERVED_EXTENSION_DATA_FIELDS:
                paths.append(child_path)
            paths.extend(_reserved_extension_data_paths(item, path=child_path))
        return tuple(paths)
    if isinstance(value, list | tuple):
        paths: list[str] = []
        for index, item in enumerate(value):
            paths.extend(_reserved_extension_data_paths(item, path=f"{path}[{index}]"))
        return tuple(paths)
    return ()


def make_context_id() -> str:
    """Generate an opaque controller-owned action context handle."""
    return str(uuid.uuid4())


def make_binding_id() -> str:
    """Generate an opaque controller-owned control binding id."""
    return str(uuid.uuid4())


def make_page_session_id() -> str:
    """Generate an opaque controller-owned dynamic page session id."""
    return str(uuid.uuid4())


class ActionMessageBody(DeckrModel):
    """Base class for typed ``actions`` lane bodies."""

    def to_dict(self) -> dict[str, Any]:
        """Serialize for JSON."""
        return self.model_dump(by_alias=True, exclude_none=True, mode="json")


class EmptyActionBody(ActionMessageBody):
    """Body for messages whose meaning lives entirely in the envelope/subject."""


class ActionExtensionBody(ActionMessageBody):
    """Explicit extension body for action lane extension traffic."""

    extension_type: str
    extension_schema_id: str
    data: JsonObject = Field(default_factory=dict)

    @field_validator("extension_type", "extension_schema_id")
    @classmethod
    def _validate_extension_identity(cls, value: str) -> str:
        return _require_globally_qualified_name(value, field_name="extension identity")

    @field_validator("data", mode="before")
    @classmethod
    def _thaw_data(cls, value: Any) -> Any:
        return thaw_json(value)

    @field_validator("data", mode="after")
    @classmethod
    def _freeze_data(cls, value: Mapping[str, Any]) -> Mapping[str, Any]:
        reserved = _reserved_extension_data_paths(value)
        if reserved:
            fields = ", ".join(sorted(reserved))
            raise ValueError(
                f"extension data must not contain routing fields: {fields}"
            )
        return freeze_json(value)

    @field_serializer("data")
    def _serialize_data(self, value: Mapping[str, Any]) -> dict[str, Any]:
        return thaw_json(value)


CapabilityViewKind = Literal["raw", "native", "projected", "derived", "extension"]


CapabilityProvenance = Literal["native", "projection", "derivation", "extension"]


SettingsScope = Literal["action_provider_instance", "action_instance"]


PageChildBindingTargetKind = Literal["self", "action"]


ActionLifecycleRejectionTargetKind = Literal[
    "action_instance",
    "binding",
    "page_session",
]


ActionLifecycleRejectionReason = Literal[
    "action_not_available",
    "provider_not_ready",
    "invalid_settings",
    "unsupported_capability",
    "resource_unavailable",
    "permission_denied",
    "stale_lifecycle",
    "internal_error",
]


SettingsProvenance = Literal[
    "config_default",
    "user_override",
    "template_override",
    "runtime",
    "stale_schema",
]


ActionAvailabilityStatus = Literal["available", "unavailable", "probing"]


def _require_text(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _require_optional_text(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_non_negative(value: int | None, *, field_name: str) -> int | None:
    if value is not None and value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


def _require_contract_name(value: str, *, field_name: str) -> str:
    normalized = _require_text(value, field_name=field_name)
    if normalized != value:
        raise ValueError(f"{field_name} must not contain surrounding whitespace")
    if not _CONTRACT_NAME_RE.fullmatch(normalized):
        raise ValueError(f"{field_name} must be a lowercase contract identifier")
    return normalized


def _require_globally_qualified_name(value: str, *, field_name: str) -> str:
    normalized = _require_contract_name(value, field_name=field_name)
    if not _GLOBALLY_QUALIFIED_NAME_RE.fullmatch(normalized):
        raise ValueError(f"{field_name} must be globally namespaced")
    return normalized


def _require_capability_family(value: str, *, field_name: str) -> str:
    normalized = _require_contract_name(value, field_name=field_name)
    if normalized in CORE_CAPABILITY_FAMILIES:
        return normalized
    if _EXTENSION_CAPABILITY_FAMILY_RE.fullmatch(normalized):
        return normalized
    raise ValueError(
        f"{field_name} must be a Deckr core capability family or globally namespaced extension family"
    )


def _validate_contract_names(
    values: tuple[str, ...],
    *,
    field_name: str,
) -> tuple[str, ...]:
    if len(set(values)) != len(values):
        raise ValueError(f"{field_name} names must be unique")
    return tuple(
        _require_contract_name(value, field_name=field_name) for value in values
    )


def _require_bound_capability_ref(
    ref: CapabilityRef,
    *,
    field_name: str,
) -> CapabilityRef:
    if ref.device_ref is None or ref.control_id is None:
        raise ValueError(f"{field_name} requires deviceRef and controlId")
    return ref
