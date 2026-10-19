"""Manifest."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from deckr.components._defs import Component
from deckr.contracts.lanes import MessageContract, MessageContractRegistry

if TYPE_CHECKING:
    pass

if TYPE_CHECKING:
    from deckr.components.context import ComponentContext


COMPONENT_ENTRYPOINT_GROUP = "deckr.components"


COMPONENT_INSTANCE_SOURCE_ENTRYPOINT_GROUP = "deckr.component_instance_sources"


class ComponentCardinality(StrEnum):
    SINGLETON = "singleton"
    MULTI_INSTANCE = "multi_instance"


@dataclass(frozen=True, slots=True)
class ComponentManifest:
    component_id: str
    consumes: tuple[str, ...] = ()
    publishes: tuple[str, ...] = ()
    cardinality: ComponentCardinality = ComponentCardinality.SINGLETON
    lane_contracts: tuple[MessageContract, ...] = ()
    endpoint_slots: tuple[str, ...] = ()
    role: str | None = None


@dataclass(frozen=True, slots=True)
class ResolvedLaneSet:
    consumes: tuple[str, ...] = ()
    publishes: tuple[str, ...] = ()


class ComponentFactory(Protocol):
    def __call__(self, context: ComponentContext) -> Component: ...


class LaneResolver(Protocol):
    def __call__(
        self,
        *,
        manifest: ComponentManifest,
        config: Mapping[str, Any],
        endpoints: Mapping[str, str],
        instance_id: str,
    ) -> ResolvedLaneSet: ...


class LaneBindingValidator(Protocol):
    def __call__(
        self,
        *,
        config: Mapping[str, Any],
        endpoints: Mapping[str, str],
        instance_id: str,
        lane_contracts: MessageContractRegistry,
    ) -> None: ...


class ComponentConfigValidator(Protocol):
    def __call__(
        self,
        *,
        config: Mapping[str, Any],
        endpoints: Mapping[str, str],
        instance_id: str,
    ) -> None: ...


class ComponentInstanceSourceReporter(Protocol):
    def __call__(
        self,
        message: str,
        *,
        component_id: str | None = None,
        instance_id: str | None = None,
    ) -> None: ...


def _noop_component_instance_source_reporter(
    message: str,
    *,
    component_id: str | None = None,
    instance_id: str | None = None,
) -> None:
    del message, component_id, instance_id


@dataclass(frozen=True, slots=True)
class ComponentInstanceDefinition:
    component_id: str
    instance_id: str
    config: Mapping[str, Any] = field(default_factory=dict)
    endpoints: Mapping[str, str] = field(default_factory=dict)
    runtime_name: str | None = None
    config_address: str | None = None
    generated_by: str | None = None


@dataclass(frozen=True, slots=True)
class ComponentInstanceSourceContext:
    source_id: str
    source_config: Mapping[str, Any]
    resolved_config: Mapping[str, Any]
    base_dir: Path
    report: ComponentInstanceSourceReporter = field(
        default=_noop_component_instance_source_reporter,
        repr=False,
        compare=False,
    )


class ComponentInstanceSourceLoader(Protocol):
    def __call__(
        self,
        context: ComponentInstanceSourceContext,
    ) -> Sequence[ComponentInstanceDefinition]: ...


@dataclass(frozen=True, slots=True)
class ComponentInstanceSourceDefinition:
    source_id: str
    load: ComponentInstanceSourceLoader


@dataclass(frozen=True, slots=True)
class PlanningEvent:
    message: str
    source_id: str | None = None
    component_id: str | None = None
    instance_id: str | None = None


@dataclass(frozen=True, slots=True)
class ComponentPlanningReport:
    events: tuple[PlanningEvent, ...] = ()


@dataclass(frozen=True, slots=True)
class ComponentDefinition:
    manifest: ComponentManifest
    factory: ComponentFactory
    resolve_lanes: LaneResolver | None = None
    validate_lane_bindings: LaneBindingValidator | None = None
    validate_config: ComponentConfigValidator | None = None

    def lanes_for(
        self,
        *,
        config: Mapping[str, Any],
        endpoints: Mapping[str, str],
        instance_id: str,
    ) -> ResolvedLaneSet:
        if self.resolve_lanes is not None:
            return self.resolve_lanes(
                manifest=self.manifest,
                config=config,
                endpoints=endpoints,
                instance_id=instance_id,
            )
        return ResolvedLaneSet(
            consumes=self.manifest.consumes,
            publishes=self.manifest.publishes,
        )

    def validate_resolved_lane_bindings(
        self,
        *,
        config: Mapping[str, Any],
        endpoints: Mapping[str, str],
        instance_id: str,
        lane_contracts: MessageContractRegistry,
    ) -> None:
        if self.validate_lane_bindings is None:
            return
        self.validate_lane_bindings(
            config=config,
            endpoints=endpoints,
            instance_id=instance_id,
            lane_contracts=lane_contracts,
        )

    def validate_instance_config(
        self,
        *,
        config: Mapping[str, Any],
        endpoints: Mapping[str, str],
        instance_id: str,
    ) -> None:
        if self.validate_config is None:
            return
        self.validate_config(
            config=config,
            endpoints=endpoints,
            instance_id=instance_id,
        )


ComponentDefinitions = Mapping[str, ComponentDefinition] | Sequence[ComponentDefinition]


ComponentInstanceSourceDefinitions = (
    Mapping[str, ComponentInstanceSourceDefinition]
    | Sequence[ComponentInstanceSourceDefinition]
)


@dataclass(frozen=True, slots=True)
class ComponentInstanceSpec:
    component_id: str
    instance_id: str
    runtime_name: str
    config: Mapping[str, Any]
    endpoints: Mapping[str, str]
    definition: ComponentDefinition
    lanes: ResolvedLaneSet
    config_address: str | None = None
    generated_by: str | None = None


@dataclass(frozen=True, slots=True)
class ComponentHostPlan:
    specs: tuple[ComponentInstanceSpec, ...]
    lane_contracts: MessageContractRegistry
    lane_names: tuple[str, ...]
    base_dir: Path
    report: ComponentPlanningReport = field(default_factory=ComponentPlanningReport)

    @classmethod
    def from_specs(
        cls,
        specs: Sequence[ComponentInstanceSpec],
        *,
        base_dir: Path | None = None,
        lane_contracts: MessageContractRegistry
        | Sequence[MessageContract]
        | None = None,
    ) -> ComponentHostPlan:
        normalized_specs = tuple(specs)
        from deckr.components.lane_policy import (
            _build_lane_contract_registry,
            _lane_names_for_specs,
            _validate_component_lane_bindings,
        )

        registry = _build_lane_contract_registry(
            normalized_specs,
            extra_contracts=lane_contracts,
        )
        _validate_component_lane_bindings(normalized_specs, registry)
        return cls(
            specs=normalized_specs,
            lane_contracts=registry,
            lane_names=_lane_names_for_specs(
                normalized_specs,
                lane_contracts=registry,
            ),
            base_dir=base_dir or Path.cwd(),
        )

    @property
    def components(self) -> tuple[ComponentInstanceSpec, ...]:
        return self.specs
