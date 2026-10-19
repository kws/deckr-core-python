"""Discovery."""

from __future__ import annotations

from importlib.metadata import entry_points

from deckr.components.manifest import (
    COMPONENT_ENTRYPOINT_GROUP,
    COMPONENT_INSTANCE_SOURCE_ENTRYPOINT_GROUP,
    ComponentDefinition,
    ComponentInstanceSourceDefinition,
)


def available_component_ids() -> list[str]:
    return sorted(
        entry_point.name
        for entry_point in entry_points().select(group=COMPONENT_ENTRYPOINT_GROUP)
    )


def load_component_definition(component_id: str) -> ComponentDefinition | None:
    for entry_point in entry_points().select(group=COMPONENT_ENTRYPOINT_GROUP):
        if entry_point.name != component_id:
            continue
        definition = entry_point.load()
        if not isinstance(definition, ComponentDefinition):
            raise TypeError(
                f"Entry point {component_id!r} did not load a ComponentDefinition"
            )
        return definition
    return None


def available_component_instance_source_ids() -> list[str]:
    return sorted(
        entry_point.name
        for entry_point in entry_points().select(
            group=COMPONENT_INSTANCE_SOURCE_ENTRYPOINT_GROUP
        )
    )


def load_component_instance_source_definition(
    source_id: str,
) -> ComponentInstanceSourceDefinition | None:
    for entry_point in entry_points().select(
        group=COMPONENT_INSTANCE_SOURCE_ENTRYPOINT_GROUP
    ):
        if entry_point.name != source_id:
            continue
        definition = entry_point.load()
        if not isinstance(definition, ComponentInstanceSourceDefinition):
            raise TypeError(
                f"Entry point {source_id!r} did not load a "
                "ComponentInstanceSourceDefinition"
            )
        if definition.source_id != source_id:
            raise ValueError(
                f"Component instance source entry point {source_id!r} loaded "
                f"source {definition.source_id!r}"
            )
        return definition
    return None
