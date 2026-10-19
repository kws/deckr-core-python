"""Activation."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from deckr.runtime import Deckr

from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass

import anyio

from deckr.beacon import Beacon
from deckr.bindings.authority_stores import CONCORD_REAPER_COMPONENT_ID
from deckr.components._defs import Component
from deckr.components._runner import ComponentManager
from deckr.components.context import ComponentContext, ComponentEndpointOpener
from deckr.components.manifest import ComponentHostPlan, ComponentInstanceSpec
from deckr.concord import Concord
from deckr.contracts.messages import endpoint_address
from deckr.lanes import EndpointSession, Lane, LaneRegistry


@dataclass(frozen=True, slots=True)
class ComponentHost:
    component_manager: ComponentManager
    components: tuple[Component, ...]
    lane_names: tuple[str, ...]
    lanes: LaneRegistry

    def get_lane(self, name: str) -> Lane | None:
        return self.lanes.get(name)

    async def stop(self) -> None:
        await self.component_manager.stop()


@asynccontextmanager
async def start_components(
    deckr: Deckr,
    plan: ComponentHostPlan,
) -> AsyncIterator[ComponentHost]:
    _validate_runtime_for_plan(deckr, plan)
    component_manager = ComponentManager()
    async with anyio.create_task_group() as tg:
        await tg.start(component_manager.run)
        host = await _activate_component_plan(deckr, plan, component_manager)
        try:
            yield host
        finally:
            with anyio.CancelScope(shield=True):
                await host.stop()
                tg.cancel_scope.cancel()


def _validate_runtime_for_plan(deckr: Deckr, plan: ComponentHostPlan) -> None:
    if not deckr.is_running:
        raise RuntimeError("Deckr runtime must be entered before starting components")

    missing_lanes = sorted(set(plan.lane_names) - set(deckr.lanes.names))
    if missing_lanes:
        lanes = ", ".join(repr(lane) for lane in missing_lanes)
        raise LookupError(f"Deckr runtime is missing required lane(s): {lanes}")

    runtime_contracts = deckr.lane_contracts.contracts
    for lane, contract in sorted(plan.lane_contracts.contracts.items()):
        if runtime_contracts.get(lane) != contract:
            raise ValueError(
                f"Deckr runtime lane contract for {lane!r} does not match "
                "the component host plan"
            )


def _optional_beacon(deckr: Deckr) -> Beacon | None:
    try:
        return deckr.beacon
    except RuntimeError:
        return None


def _optional_concord(deckr: Deckr) -> Concord | None:
    try:
        return deckr.concord
    except RuntimeError:
        return None


def _endpoint_opener_for_spec(
    deckr: Deckr,
    spec: ComponentInstanceSpec,
) -> ComponentEndpointOpener:
    def open_endpoint(
        slot: str,
        *,
        session_id: str | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> AbstractAsyncContextManager[EndpointSession]:
        endpoint_id = spec.endpoints.get(slot)
        if endpoint_id is None:
            raise KeyError(
                f"Component {spec.runtime_name!r} has no endpoint slot {slot!r}"
            )
        merged_metadata = {
            "componentId": spec.component_id,
            "instanceId": spec.instance_id,
            "runtimeName": spec.runtime_name,
            "endpointSlot": slot,
            **dict(metadata or {}),
        }
        return deckr.endpoint(
            endpoint_address(slot, endpoint_id),
            session_id=session_id,
            metadata=merged_metadata,
        )

    return open_endpoint


async def _activate_component_plan(
    deckr: Deckr,
    plan: ComponentHostPlan,
    component_manager: ComponentManager,
) -> ComponentHost:
    created: list[Component] = []
    beacon = _optional_beacon(deckr)
    concord = _optional_concord(deckr)
    for spec in plan.specs:
        context = ComponentContext(
            component_id=spec.component_id,
            instance_id=spec.instance_id,
            runtime_name=spec.runtime_name,
            manifest=spec.definition.manifest,
            config=spec.config,
            endpoints=spec.endpoints,
            base_dir=plan.base_dir,
            lanes=deckr.lanes,
            beacon=beacon,
            concord=concord,
            kv_bucket_for=deckr.kv_bucket,
            _concord_maintenance_stores_for=(
                deckr._concord_maintenance_stores  # noqa: SLF001
                if spec.component_id == CONCORD_REAPER_COMPONENT_ID
                else None
            ),
            endpoint_for=_endpoint_opener_for_spec(deckr, spec),
        )
        component = spec.definition.factory(context)
        if not isinstance(component, Component):
            raise TypeError(
                f"Component {spec.component_id!r} did not return a Component"
            )
        created.append(component)

    for component in created:
        await component_manager.add_component(component)

    return ComponentHost(
        component_manager=component_manager,
        components=tuple(created),
        lane_names=plan.lane_names,
        lanes=deckr.lanes,
    )
