"""Planning."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from deckr.components.discovery import (
    load_component_definition,
    load_component_instance_source_definition,
)
from deckr.components.lane_policy import (
    _build_lane_contract_registry,
    _lane_names_for_specs,
    _validate_component_lane_bindings,
)
from deckr.components.manifest import (
    ComponentCardinality,
    ComponentDefinition,
    ComponentDefinitions,
    ComponentHostPlan,
    ComponentInstanceDefinition,
    ComponentInstanceSourceContext,
    ComponentInstanceSourceDefinition,
    ComponentInstanceSourceDefinitions,
    ComponentInstanceSourceReporter,
    ComponentInstanceSpec,
    ComponentPlanningReport,
    PlanningEvent,
)
from deckr.core.config import ConfigDocument
from deckr.core.util.runtime_id import require_runtime_id


def runtime_name_for(component_id: str, instance_id: str) -> str:
    if instance_id == "default":
        return component_id
    return f"{component_id}:{instance_id}"


def resolve_component_host_plan(
    document: ConfigDocument,
    *,
    definitions: ComponentDefinitions | None = None,
    instance_source_definitions: ComponentInstanceSourceDefinitions | None = None,
) -> ComponentHostPlan:
    report_events: list[PlanningEvent] = []
    instance_defs = list(
        _configured_component_instance_definitions(
            document, report_events=report_events
        )
    )
    instance_defs.extend(
        _generated_component_instance_definitions(
            document,
            instance_source_definitions=instance_source_definitions,
            report_events=report_events,
        )
    )
    specs = tuple(
        _specs_from_instance_definitions(
            instance_defs,
            document=document,
            definitions=definitions,
            report_events=report_events,
        )
    )
    lane_contracts = _build_lane_contract_registry(specs, document=document)
    _validate_component_lane_bindings(specs, lane_contracts)
    return ComponentHostPlan(
        specs=specs,
        lane_contracts=lane_contracts,
        lane_names=_lane_names_for_specs(specs, lane_contracts=lane_contracts),
        base_dir=document.base_dir,
        report=ComponentPlanningReport(events=tuple(report_events)),
    )


def resolve_component_instance_specs(
    document: ConfigDocument,
    *,
    definitions: ComponentDefinitions | None = None,
    instance_source_definitions: ComponentInstanceSourceDefinitions | None = None,
) -> list[ComponentInstanceSpec]:
    return list(
        resolve_component_host_plan(
            document,
            definitions=definitions,
            instance_source_definitions=instance_source_definitions,
        ).specs
    )


def configured_component_instance_specs(
    document: ConfigDocument,
    *,
    definitions: ComponentDefinitions | None = None,
    instance_source_definitions: ComponentInstanceSourceDefinitions | None = None,
) -> list[ComponentInstanceSpec]:
    return resolve_component_instance_specs(
        document,
        definitions=definitions,
        instance_source_definitions=instance_source_definitions,
    )


GENERIC_INSTANCE_FIELDS = frozenset(
    {"component", "instance_id", "runtime_name", "endpoints", "config"}
)


def _configured_component_instance_definitions(
    document: ConfigDocument,
    *,
    report_events: list[PlanningEvent],
) -> list[ComponentInstanceDefinition]:
    instances = document.children("deckr.components.instances")
    definitions: list[ComponentInstanceDefinition] = []
    for config_name, source in instances.items():
        definitions.append(
            _component_instance_definition_from_mapping(
                source,
                config_address=f"deckr.components.instances.{config_name}",
                generated_by=None,
            )
        )
        report_events.append(
            PlanningEvent(
                message=f"configured component instance {config_name}",
                instance_id=definitions[-1].instance_id,
                component_id=definitions[-1].component_id,
            )
        )
    return definitions


def _component_instance_definition_from_mapping(
    source: Mapping[str, Any],
    *,
    config_address: str | None,
    generated_by: str | None,
) -> ComponentInstanceDefinition:
    unknown = sorted(set(source) - GENERIC_INSTANCE_FIELDS)
    if unknown:
        names = ", ".join(unknown)
        location = f" in {config_address}" if config_address else ""
        raise ValueError(f"Unknown component instance field(s){location}: {names}")

    component_id = source.get("component")
    if not isinstance(component_id, str) or not component_id.strip():
        raise ValueError(f"{config_address or 'Component instance'}.component required")
    instance_id = require_runtime_id(
        str(source.get("instance_id", "")).strip(),
        label="Component instance ID",
        source_hint=f"Set `{config_address}.instance_id`.",
    )
    runtime_name_source = source.get("runtime_name")
    runtime_name = None
    if runtime_name_source is not None:
        runtime_name = require_runtime_id(
            str(runtime_name_source).strip(),
            label="Component runtime name",
            source_hint=f"Set `{config_address}.runtime_name`.",
        )

    endpoints_source = source.get("endpoints")
    endpoints: dict[str, str] = {}
    if endpoints_source is not None:
        if not isinstance(endpoints_source, Mapping):
            raise ValueError(f"{config_address}.endpoints must be a table")
        for slot, endpoint_id in endpoints_source.items():
            if not isinstance(slot, str) or not slot.strip():
                raise ValueError(f"{config_address}.endpoints keys must be strings")
            if not isinstance(endpoint_id, str) or not endpoint_id.strip():
                raise ValueError(
                    f"{config_address}.endpoints.{slot} must be a non-empty string"
                )
            endpoints[slot.strip()] = require_runtime_id(
                endpoint_id.strip(),
                label=f"Endpoint ID for {slot}",
                source_hint=f"Set `{config_address}.endpoints.{slot}`.",
            )

    config = source.get("config", {})
    if not isinstance(config, Mapping):
        raise ValueError(f"{config_address}.config must be a table")

    return ComponentInstanceDefinition(
        component_id=component_id.strip(),
        instance_id=instance_id,
        runtime_name=runtime_name,
        config=dict(config),
        endpoints=endpoints,
        config_address=config_address,
        generated_by=generated_by,
    )


def _source_declarations(document: ConfigDocument) -> tuple[Mapping[str, Any], ...]:
    components = document.namespace("deckr.components")
    if components is None:
        return ()
    source = components.get("instance_sources")
    if source is None:
        return ()
    if isinstance(source, str) or not isinstance(source, Sequence):
        raise ValueError("deckr.components.instance_sources must be an array of tables")
    declarations: list[Mapping[str, Any]] = []
    for index, item in enumerate(source):
        if not isinstance(item, Mapping):
            raise ValueError(
                f"deckr.components.instance_sources[{index}] must be a table"
            )
        declarations.append(item)
    return tuple(declarations)


def _instance_source_ids(source: Mapping[str, Any], *, index: int) -> tuple[str, str]:
    declaration_id = source.get("id")
    source_id = source.get("source")
    if not isinstance(declaration_id, str) or not declaration_id.strip():
        raise ValueError(
            f"deckr.components.instance_sources[{index}].id must be a non-empty string"
        )
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValueError(
            "deckr.components.instance_sources"
            f"[{index}].source must be a non-empty string"
        )
    return declaration_id.strip(), source_id.strip()


def _component_instance_source_reporter(
    source_id: str,
    report_events: list[PlanningEvent],
) -> ComponentInstanceSourceReporter:
    def report(
        message: str,
        *,
        component_id: str | None = None,
        instance_id: str | None = None,
    ) -> None:
        report_events.append(
            PlanningEvent(
                source_id=source_id,
                component_id=component_id,
                instance_id=instance_id,
                message=message,
            )
        )

    return report


def _generated_component_instance_definitions(
    document: ConfigDocument,
    *,
    instance_source_definitions: ComponentInstanceSourceDefinitions | None,
    report_events: list[PlanningEvent],
) -> list[ComponentInstanceDefinition]:
    source_definitions = (
        _instance_source_definition_mapping(instance_source_definitions)
        if instance_source_definitions is not None
        else None
    )
    generated: list[ComponentInstanceDefinition] = []
    seen_declaration_ids: set[str] = set()
    for index, source in enumerate(_source_declarations(document)):
        declaration_id, source_id = _instance_source_ids(source, index=index)
        if declaration_id in seen_declaration_ids:
            raise ValueError(
                "Duplicate Deckr component instance source declaration id: "
                f"{declaration_id}"
            )
        seen_declaration_ids.add(declaration_id)
        definition = (
            source_definitions.get(source_id)
            if source_definitions is not None
            else load_component_instance_source_definition(source_id)
        )
        if definition is None:
            raise ValueError(f"Unknown Deckr component instance source: {source_id}")

        context = ComponentInstanceSourceContext(
            source_id=source_id,
            source_config=source,
            resolved_config=document.raw,
            base_dir=document.base_dir,
            report=_component_instance_source_reporter(source_id, report_events),
        )
        output = tuple(definition.load(context))
        report_events.append(
            PlanningEvent(
                source_id=source_id, message=f"ran instance source {declaration_id}"
            )
        )
        for item in output:
            if not isinstance(item, ComponentInstanceDefinition):
                raise TypeError(
                    f"Component instance source {source_id!r} returned "
                    "a non-ComponentInstanceDefinition"
                )
            generated.append(
                ComponentInstanceDefinition(
                    component_id=item.component_id,
                    instance_id=item.instance_id,
                    config=dict(item.config),
                    endpoints=dict(item.endpoints),
                    runtime_name=item.runtime_name,
                    config_address=item.config_address,
                    generated_by=source_id,
                )
            )
            report_events.append(
                PlanningEvent(
                    source_id=source_id,
                    component_id=item.component_id,
                    instance_id=item.instance_id,
                    message="generated component instance",
                )
            )
    return generated


def _definition_mapping(
    definitions: ComponentDefinitions,
) -> dict[str, ComponentDefinition]:
    if isinstance(definitions, Mapping):
        return dict(definitions)
    return {definition.manifest.component_id: definition for definition in definitions}


def _instance_source_definition_mapping(
    definitions: ComponentInstanceSourceDefinitions,
) -> dict[str, ComponentInstanceSourceDefinition]:
    if isinstance(definitions, Mapping):
        return dict(definitions)
    return {definition.source_id: definition for definition in definitions}


def _component_definition_for(
    component_id: str,
    *,
    definitions: Mapping[str, ComponentDefinition] | None,
) -> ComponentDefinition:
    definition = (
        definitions.get(component_id)
        if definitions is not None
        else load_component_definition(component_id)
    )
    if definition is None:
        raise ValueError(f"Unknown Deckr component id: {component_id}")
    if definition.manifest.component_id != component_id:
        raise ValueError(
            f"Component definition for {component_id!r} declares "
            f"{definition.manifest.component_id!r}"
        )
    return definition


def _validate_instance_endpoints(
    instance: ComponentInstanceDefinition,
    definition: ComponentDefinition,
) -> None:
    allowed = set(definition.manifest.endpoint_slots)
    unknown = sorted(set(instance.endpoints) - allowed)
    if unknown:
        names = ", ".join(unknown)
        raise ValueError(
            f"Component instance {instance.instance_id!r} for "
            f"{instance.component_id!r} uses unknown endpoint slot(s): {names}"
        )
    missing = sorted(allowed - set(instance.endpoints))
    if missing:
        names = ", ".join(missing)
        raise ValueError(
            f"Component instance {instance.instance_id!r} for "
            f"{instance.component_id!r} is missing endpoint slot(s): {names}"
        )


def _specs_from_instance_definitions(
    instances: Sequence[ComponentInstanceDefinition],
    *,
    document: ConfigDocument,
    definitions: ComponentDefinitions | None,
    report_events: list[PlanningEvent],
) -> list[ComponentInstanceSpec]:
    definition_map = (
        _definition_mapping(definitions) if definitions is not None else None
    )
    seen_instance_ids: set[str] = set()
    seen_runtime_names: set[str] = set()
    seen_endpoints: dict[tuple[str, str], str] = {}
    component_counts: dict[str, int] = {}
    specs: list[ComponentInstanceSpec] = []

    for instance in instances:
        if instance.instance_id in seen_instance_ids:
            raise ValueError(
                f"Duplicate Deckr component instance id: {instance.instance_id}"
            )
        seen_instance_ids.add(instance.instance_id)
        definition = _component_definition_for(
            instance.component_id,
            definitions=definition_map,
        )
        runtime_name = instance.runtime_name or runtime_name_for(
            instance.component_id,
            instance.instance_id,
        )
        if runtime_name in seen_runtime_names:
            raise ValueError(f"Duplicate Deckr component runtime name: {runtime_name}")
        seen_runtime_names.add(runtime_name)
        component_counts[instance.component_id] = (
            component_counts.get(instance.component_id, 0) + 1
        )
        _validate_instance_endpoints(instance, definition)
        for family, endpoint_id in instance.endpoints.items():
            key = (family, endpoint_id)
            existing = seen_endpoints.get(key)
            if existing is not None:
                raise ValueError(
                    f"Duplicate Deckr endpoint id {endpoint_id!r} for family "
                    f"{family!r}: {existing!r} and {instance.instance_id!r}"
                )
            seen_endpoints[key] = instance.instance_id

        definition.validate_instance_config(
            config=instance.config,
            endpoints=instance.endpoints,
            instance_id=instance.instance_id,
        )
        specs.append(
            ComponentInstanceSpec(
                component_id=instance.component_id,
                instance_id=instance.instance_id,
                runtime_name=runtime_name,
                config=instance.config,
                endpoints=instance.endpoints,
                definition=definition,
                lanes=definition.lanes_for(
                    config=instance.config,
                    endpoints=instance.endpoints,
                    instance_id=instance.instance_id,
                ),
                config_address=instance.config_address,
                generated_by=instance.generated_by,
            )
        )
        report_events.append(
            PlanningEvent(
                component_id=instance.component_id,
                instance_id=instance.instance_id,
                message="planned component instance",
            )
        )

    for component_id, count in sorted(component_counts.items()):
        if count <= 1:
            continue
        definition = _component_definition_for(component_id, definitions=definition_map)
        if definition.manifest.cardinality == ComponentCardinality.SINGLETON:
            raise ValueError(
                f"Component {component_id!r} has singleton cardinality but "
                f"{count} instances were planned"
            )
    return specs
