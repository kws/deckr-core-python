"""Device descriptor."""

from __future__ import annotations

from collections.abc import Mapping

from pydantic import Field, field_validator, model_validator

from deckr.contracts.models import DeckrModel
from deckr.hardware._descriptor_rules import (
    CAPABILITY_DESCRIPTOR_SCHEMA_ID,
    CONTROL_DESCRIPTOR_SCHEMA_ID,
    DEVICE_DESCRIPTOR_SCHEMA_ID,
    _require_contract_token,
    _require_non_empty,
    _require_not_endpoint_address,
    _require_unique,
    _validate_unique_tuple,
)
from deckr.hardware.capability_descriptor import CapabilityDescriptor
from deckr.hardware.references import (
    ControlGeometry,
    DescriptorCapabilityRef,
    DeviceConnection,
    DeviceIdentifier,
    DeviceRef,
    DeviceSourceReference,
)


class ControlDescriptor(DeckrModel):
    """Descriptor for one addressable user-facing control or region."""

    control_id: str = Field(alias="controlId")
    kind: str
    label: str | None = None
    group_id: str | None = Field(default=None, alias="groupId")
    parent_control_id: str | None = Field(default=None, alias="parentControlId")
    related_control_ids: tuple[str, ...] = Field(
        default_factory=tuple,
        alias="relatedControlIds",
    )
    surface_id: str | None = Field(default=None, alias="surfaceId")
    geometry: ControlGeometry | None = None
    input_capabilities: tuple[CapabilityDescriptor, ...] = Field(
        default_factory=tuple,
        alias="inputCapabilities",
    )
    output_capabilities: tuple[CapabilityDescriptor, ...] = Field(
        default_factory=tuple,
        alias="outputCapabilities",
    )
    state_capabilities: tuple[CapabilityDescriptor, ...] = Field(
        default_factory=tuple,
        alias="stateCapabilities",
    )
    config_capabilities: tuple[CapabilityDescriptor, ...] = Field(
        default_factory=tuple,
        alias="configCapabilities",
    )
    diagnostic_capabilities: tuple[CapabilityDescriptor, ...] = Field(
        default_factory=tuple,
        alias="diagnosticCapabilities",
    )
    sources: tuple[DeviceSourceReference, ...] = Field(default_factory=tuple)

    @field_validator("control_id")
    @classmethod
    def _validate_control_id(cls, value: str) -> str:
        return _require_non_empty(value, field_name="control_id")

    @field_validator("kind")
    @classmethod
    def _validate_kind(cls, value: str) -> str:
        return _require_contract_token(value, field_name="control kind")

    @field_validator(
        "label",
        "group_id",
        "parent_control_id",
        "surface_id",
    )
    @classmethod
    def _validate_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_non_empty(value, field_name="control text")

    @field_validator("related_control_ids", mode="after")
    @classmethod
    def _validate_related_control_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        ids = tuple(
            _require_non_empty(item, field_name="related_control_id") for item in value
        )
        return _validate_unique_tuple(ids, field_name="related_control_ids")

    @property
    def capabilities(self) -> tuple[CapabilityDescriptor, ...]:
        """Return all capabilities owned by this control."""

        return (
            *self.input_capabilities,
            *self.output_capabilities,
            *self.state_capabilities,
            *self.config_capabilities,
            *self.diagnostic_capabilities,
        )

    @model_validator(mode="after")
    def _validate_control(self) -> ControlDescriptor:
        _require_unique(
            (capability.capability_id for capability in self.capabilities),
            field_name=f"capability ids on control {self.control_id}",
        )
        for capability in self.input_capabilities:
            if capability.direction != "input":
                raise ValueError("input_capabilities must have input direction")
        for capability in self.output_capabilities:
            if capability.direction != "output":
                raise ValueError("output_capabilities must have output direction")
        for capability in self.state_capabilities:
            if capability.direction != "state":
                raise ValueError("state_capabilities must have state direction")
        for capability in self.config_capabilities:
            if capability.direction not in {"state", "command"}:
                raise ValueError(
                    "config_capabilities must have state or command direction"
                )
        for capability in self.diagnostic_capabilities:
            if capability.direction != "state":
                raise ValueError("diagnostic_capabilities must have state direction")
        return self


class DeviceDescriptor(DeckrModel):
    """Descriptor for one controller-visible device or sub-surface."""

    device_id: str = Field(alias="deviceId")
    fingerprint: str
    display_name: str = Field(alias="displayName")
    manufacturer: str | None = None
    model: str | None = None
    model_id: str | None = Field(default=None, alias="modelId")
    serial_number: str | None = Field(default=None, alias="serialNumber")
    hardware_version: str | None = Field(default=None, alias="hardwareVersion")
    firmware_version: str | None = Field(default=None, alias="firmwareVersion")
    identifiers: tuple[DeviceIdentifier, ...] = Field(default_factory=tuple)
    connections: tuple[DeviceConnection, ...] = Field(default_factory=tuple)
    parent: DeviceRef | None = None
    default_status_indicator: DescriptorCapabilityRef | None = Field(
        default=None,
        alias="defaultStatusIndicator",
    )
    controls: tuple[ControlDescriptor, ...] = Field(default_factory=tuple)
    capabilities: tuple[CapabilityDescriptor, ...] = Field(default_factory=tuple)
    sources: tuple[DeviceSourceReference, ...] = Field(default_factory=tuple)

    @field_validator("device_id")
    @classmethod
    def _validate_device_id(cls, value: str) -> str:
        return _require_not_endpoint_address(value, field_name="device_id")

    @field_validator(
        "fingerprint",
        "display_name",
        "manufacturer",
        "model",
        "model_id",
        "serial_number",
        "hardware_version",
        "firmware_version",
    )
    @classmethod
    def _validate_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_non_empty(value, field_name="device descriptor text")

    @model_validator(mode="after")
    def _validate_device_descriptor(self) -> DeviceDescriptor:
        _require_unique(
            (connection.connection_id for connection in self.connections),
            field_name="connection ids",
        )
        _require_unique(
            (control.control_id for control in self.controls),
            field_name="control ids",
        )
        _require_unique(
            (capability.capability_id for capability in self.capabilities),
            field_name="device-level capability ids",
        )
        self._validate_source_connections()
        self._validate_control_relations()
        self._validate_device_capability_directions()
        self._validate_default_status_indicator()
        self._validate_projections()
        return self

    def _validate_source_connections(self) -> None:
        connection_ids = {connection.connection_id for connection in self.connections}
        for source in (*self.sources, *self._all_sources_from_controls()):
            if (
                source.connection_id is not None
                and source.connection_id not in connection_ids
            ):
                raise ValueError(
                    f"source {source.source_id} references unknown connection "
                    f"{source.connection_id}"
                )

    def _all_sources_from_controls(self) -> tuple[DeviceSourceReference, ...]:
        sources: list[DeviceSourceReference] = []
        for control in self.controls:
            sources.extend(control.sources)
            for capability in control.capabilities:
                sources.extend(capability.sources)
        for capability in self.capabilities:
            sources.extend(capability.sources)
        return tuple(sources)

    def _validate_control_relations(self) -> None:
        control_ids = {control.control_id for control in self.controls}
        for control in self.controls:
            if control.parent_control_id is not None:
                if control.parent_control_id == control.control_id:
                    raise ValueError("control parent must not reference itself")
                if control.parent_control_id not in control_ids:
                    raise ValueError(
                        f"control {control.control_id} references unknown parent "
                        f"{control.parent_control_id}"
                    )
            for related_id in control.related_control_ids:
                if related_id == control.control_id:
                    raise ValueError("related control must not reference itself")
                if related_id not in control_ids:
                    raise ValueError(
                        f"control {control.control_id} references unknown related "
                        f"control {related_id}"
                    )

    def _validate_device_capability_directions(self) -> None:
        for capability in self.capabilities:
            if capability.direction not in {"state", "command", "output"}:
                raise ValueError(
                    "device-level capabilities must have state, command, or output "
                    "direction"
                )

    def _validate_default_status_indicator(self) -> None:
        if self.default_status_indicator is None:
            return
        capability = self._resolve_capability(self.default_status_indicator)
        if capability is None:
            raise ValueError(
                "default status indicator must reference a real capability"
            )
        if capability.direction != "output":
            raise ValueError(
                "default status indicator must reference an output capability"
            )

    def _validate_projections(self) -> None:
        for control_id, capability in self._all_capabilities():
            projection = capability.projection
            if projection is None:
                continue
            source = projection.source
            source_capability = self._resolve_capability(source)
            if source_capability is None:
                raise ValueError(
                    f"capability {capability.capability_id} projects from unknown "
                    f"capability {source.capability_id}"
                )
            if (
                source.control_id == control_id
                and source.capability_id == capability.capability_id
            ):
                raise ValueError("capability projection must not reference itself")

    def _all_capabilities(self) -> tuple[tuple[str | None, CapabilityDescriptor], ...]:
        result: list[tuple[str | None, CapabilityDescriptor]] = [
            (None, capability) for capability in self.capabilities
        ]
        for control in self.controls:
            result.extend(
                (control.control_id, capability) for capability in control.capabilities
            )
        return tuple(result)

    def _resolve_capability(
        self, ref: DescriptorCapabilityRef
    ) -> CapabilityDescriptor | None:
        if ref.control_id is None:
            for capability in self.capabilities:
                if capability.capability_id == ref.capability_id:
                    return capability
            return None
        for control in self.controls:
            if control.control_id != ref.control_id:
                continue
            for capability in control.capabilities:
                if capability.capability_id == ref.capability_id:
                    return capability
        return None


_SCHEMA_MODELS: Mapping[str, tuple[str, type[DeckrModel]]] = {
    DEVICE_DESCRIPTOR_SCHEMA_ID: ("Deckr DeviceDescriptor", DeviceDescriptor),
    CONTROL_DESCRIPTOR_SCHEMA_ID: ("Deckr ControlDescriptor", ControlDescriptor),
    CAPABILITY_DESCRIPTOR_SCHEMA_ID: (
        "Deckr CapabilityDescriptor",
        CapabilityDescriptor,
    ),
}
