"""Component worker."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import TYPE_CHECKING

import anyio

from deckr.components._defs import (
    Component,
    ComponentState,
    ComponentStatus,
    ReadinessState,
    RunContext,
    RunningComponent,
)

if TYPE_CHECKING:
    from deckr.components.manager import ComponentManager

_STATE_ORDER: dict[ComponentState, int] = {
    ComponentState.IDLE: 0,
    ComponentState.STARTING: 1,
    ComponentState.RUNNING: 2,
    ComponentState.STOPPING: 3,
    ComponentState.STOPPED: 4,
    ComponentState.FAILED: -1,
}


logger = logging.getLogger(__name__)


SHUTDOWN_TIMEOUT_PER_COMPONENT = 2.0


async def component_runner(
    component: Component,
    stopping: anyio.Event,
    on_started: Callable[[], Awaitable[None]] | None = None,
    status_reporter: _ManagerStatusReporter | None = None,
    stop_timeout_s: Callable[[], float | None] | None = None,
    record_stop_error: Callable[[Exception], None] | None = None,
) -> None:
    """Run a component with its own task group.

    The component controls its own concurrency through the task group.
    This function keeps running until stopped, cancelled, or the component crashes.

    Args:
        component: The component to run
        stopping: Event to signal component should stop
        on_started: Optional callback to execute after component.start() succeeds
    """
    started = False
    # One TaskGroup per component: the component "controls its own concurrency".
    async with anyio.create_task_group() as tg:
        ctx = RunContext(tg=tg, stopping=stopping, status=status_reporter)
        await component.start(ctx)
        started = True

        try:
            # Component started successfully - execute callback if provided
            if on_started:
                await on_started()

            # Keep the runner alive until the manager asks it to stop. If the
            # surrounding scope is cancelled instead, this task still owns the
            # component cleanup before the component TaskGroup unwinds.
            await stopping.wait()
        finally:
            if started:
                stopping.set()
                timeout = (
                    stop_timeout_s() if stop_timeout_s is not None else None
                ) or SHUTDOWN_TIMEOUT_PER_COMPONENT
                with anyio.move_on_after(timeout, shield=True) as scope:
                    try:
                        await component.stop()
                    except Exception as e:
                        if record_stop_error is not None:
                            record_stop_error(e)
                        logger.warning(
                            f"Error calling stop() on component '{component.name}': {e}",
                            exc_info=True,
                        )
                if scope.cancel_called:
                    logger.warning(
                        f"Component '{component.name}' stop() timed out after "
                        f"{timeout}s"
                    )
            tg.cancel_scope.cancel()


class _ManagerStatusReporter:
    def __init__(self, manager: ComponentManager, component_name: str) -> None:
        self._manager = manager
        self._component_name = component_name

    async def report(
        self,
        readiness_state: ReadinessState,
        *,
        reasons: Sequence[str] = (),
        diagnostics: Mapping[str, object] | None = None,
    ) -> None:
        await self._manager._report_component_readiness(
            self._component_name,
            readiness_state,
            reasons=reasons,
            diagnostics=diagnostics,
        )


def _normalize_status_reason(reason: str) -> str:
    if not reason:
        raise ValueError("component readiness reason must not be empty")
    return reason


def _refresh_effective_readiness(rc: RunningComponent) -> None:
    rc.readiness_state = rc.local_readiness_state
    rc.readiness_reasons = rc.local_readiness_reasons
    rc.diagnostics = dict(rc.local_diagnostics)


def _status_from_running(rc: RunningComponent) -> ComponentStatus:
    return ComponentStatus(
        runtime_name=rc.component.name,
        lifecycle_state=rc.state,
        readiness_state=rc.readiness_state,
        readiness_reasons=rc.readiness_reasons,
        diagnostics=dict(rc.diagnostics),
    )
