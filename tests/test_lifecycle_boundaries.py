from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

_WatchBoundary = tuple[Path, str, str, str]


# These are the authority-state subscriptions intentionally owned by the
# materialized views and Phase 4 lifecycle contexts. In particular, source aliases
# are explicit: renaming ``source`` must update this reviewed inventory rather
# than silently evading a receiver-name pattern.
_APPROVED_AUTHORITY_STATE_BOUNDARIES: dict[_WatchBoundary, str] = {
    (
        Path("src/deckr/hardware/_state.py"),
        "HardwareStateChannel.watch",
        "self._bus",
        "subscribe",
    ): "Bounded coalesced hardware state with initial/final snapshots",
    (
        Path("src/deckr/hardware/managed_claim.py"),
        "ManagedHardwareClaim._watch_agreement",
        "self._agreement",
        "watch_state",
    ): "Claim-owned public Concord lifetime; no raw authority store watch",
    (
        Path("src/deckr/_beacon/_view.py"),
        "BeaconView.run",
        "self.source",
        "subscribe",
    ): "Phase 3 Beacon materialized-view source",
    (
        Path("src/deckr/_beacon/_view.py"),
        "BeaconView.subscribe",
        "self._broadcaster",
        "subscribe",
    ): "Phase 3 Beacon coalesced semantic view",
    (
        Path("src/deckr/_concord/_adapters.py"),
        "_MaterializedSourceAdapter.subscribe",
        "self._bucket",
        "subscribe",
    ): "Phase 3 Concord materialized-store adapter",
    (
        Path("src/deckr/_concord/_view.py"),
        "ConcordView._contract_loop",
        "self.contract_source",
        "subscribe",
    ): "Phase 3 Concord contract source",
    (
        Path("src/deckr/_concord/_view.py"),
        "ConcordView._token_loop",
        "self.token_source",
        "subscribe",
    ): "Phase 3 Concord participant-token source",
    (
        Path("src/deckr/_concord/_view.py"),
        "ConcordView.subscribe",
        "self._broadcaster",
        "subscribe",
    ): "Phase 3 Concord coalesced semantic view",
    (
        Path("src/deckr/_beacon/_api.py"),
        "Beacon.watch",
        "self._view",
        "subscribe",
    ): "Phase 3 Beacon public watch facade",
    (
        Path("src/deckr/_concord/_api.py"),
        "Concord.watch",
        "self._view",
        "subscribe",
    ): "Phase 3 Concord public watch facade",
    (
        Path("src/deckr/_concord/_agreement_context.py"),
        "ConcordAgreementLease._watch_loop",
        "self._view",
        "subscribe",
    ): "Phase 4 agreement filtered view source",
    (
        Path("src/deckr/_concord/_agreement_context.py"),
        "ConcordAgreementLease.wait_for_state.wait",
        "self._state_bus",
        "subscribe",
    ): "Phase 4 agreement atomic state wait",
    (
        Path("src/deckr/_concord/_agreement_context.py"),
        "ConcordAgreementLease.watch_state",
        "self._state_bus",
        "subscribe",
    ): "Phase 4 agreement coalesced state",
    (
        Path("src/deckr/_concord/_lease.py"),
        "ConcordParticipantLease.attach",
        "self._view",
        "subscribe",
    ): "Phase 4 partial-attach view-version retry source",
    (
        Path("src/deckr/_concord/_participant_context.py"),
        "ConcordParticipant._watch_loop",
        "self._view",
        "subscribe",
    ): "Phase 4 participant filtered view source",
    (
        Path("src/deckr/_concord/_participant_context.py"),
        "ConcordParticipant.wait_for_state.wait",
        "self._resources.state_bus",
        "subscribe",
    ): "Phase 4 participant atomic state wait",
    (
        Path("src/deckr/_concord/_participant_context.py"),
        "ConcordParticipant.watch_state",
        "self._resources.state_bus",
        "subscribe",
    ): "Phase 4 participant coalesced state",
    (
        Path("src/deckr/services/managed.py"),
        "ManagedService.wait_for_state.wait",
        "self._state_bus",
        "subscribe",
    ): "Phase 4 managed-service atomic state wait",
    (
        Path("src/deckr/services/managed.py"),
        "ManagedService.watch_state",
        "self._state_bus",
        "subscribe",
    ): "Phase 4 managed-service coalesced state",
    (
        Path("src/deckr/services/provider.py"),
        "ServiceProvider.watch_state",
        "self._participant",
        "watch_state",
    ): "Phase 4 provider-owned participant state",
}


# Every other production ``watch``/``watch_state``/``subscribe`` call is still
# exact-inventoried.
# This deliberately includes transports and package-owned state: a new raw
# authority alias cannot bypass the guard merely by choosing an unfamiliar name.
_APPROVED_OTHER_WATCH_BOUNDARIES = Counter[_WatchBoundary](
    {
        # One directory watch per runtime, one message subscription per endpoint,
        # and context-owned observers over the shared materialized/state views.
        (
            Path("src/deckr/hardware/_state.py"),
            "HardwareStateChannel.wait_for",
            "self",
            "watch",
        ): 1,
        (
            Path("src/deckr/hardware/client_runtime.py"),
            "HardwareMessageRouter._run",
            "self.endpoint",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/hardware/discovery.py"),
            "HardwareDirectory._run",
            "self._beacon",
            "watch",
        ): 1,
        (
            Path("src/deckr/hardware/discovery.py"),
            "HardwareDirectory.wait_for_device",
            "self",
            "watch_state",
        ): 1,
        (
            Path("src/deckr/hardware/discovery.py"),
            "HardwareDirectory.watch_state",
            "self._state",
            "watch",
        ): 1,
        (
            Path("src/deckr/hardware/managed_claim.py"),
            "ManagedHardwareClaim.watch_state",
            "self._state",
            "watch",
        ): 1,
        (
            Path("src/deckr/hardware/manager.py"),
            "HardwareManagerRuntime.watch_state",
            "self._observer.channel",
            "watch",
        ): 1,
        (
            Path("src/deckr/hardware/manager_state.py"),
            "HardwareManagerObserver.discovery_loop",
            "self.runtime.beacon",
            "watch",
        ): 1,
        (
            Path("src/deckr/hardware/recovery.py"),
            "ManagedHardwareUse._next_spec",
            "self._client.directory",
            "watch_state",
        ): 1,
        (
            Path("src/deckr/hardware/recovery.py"),
            "ManagedHardwareUse._run",
            "claim",
            "watch_state",
        ): 1,
        (
            Path("src/deckr/hardware/recovery.py"),
            "ManagedHardwareUse.watch_state",
            "self._state",
            "watch",
        ): 1,
        (
            Path("src/deckr/_beacon/_directory.py"),
            "BeaconDirectory._event_loop",
            "self._beacon",
            "watch",
        ): 1,
        (
            Path("src/deckr/components/manager.py"),
            "ComponentManager.wait_for_state",
            "self",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/components/manager.py"),
            "ComponentManager.subscribe",
            "self._lifecycle_state",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/components/manager.py"),
            "ComponentManager.subscribe_status",
            "self._status_state",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/hardware/manager_commands.py"),
            "HardwareManagerCommands._command_subscription_loop",
            "self.endpoint",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/hardware/manager.py"),
            "HardwareManagerRuntime._contract_event_loop",
            "self._claim_manager",
            "watch_state",
        ): 1,
        (
            Path("src/deckr/lanes.py"),
            "EndpointSession.subscribe",
            "self._message_bus",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/services/consumer_views.py"),
            "ServiceConsumerViews._watch_view",
            "store",
            "watch",
        ): 1,
        (
            Path("src/deckr/services/provider.py"),
            "ServiceProvider._enter",
            "self.endpoint",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/services/managed_views.py"),
            "_ServiceManagedServiceViewAccess.watch",
            "self._store",
            "watch",
        ): 1,
        (
            Path("src/deckr/services/managed_views.py"),
            "_LeaseManagedServiceViewAccess.watch",
            "super()",
            "watch",
        ): 1,
        (
            Path("src/deckr/services/subscription_manager.py"),
            "SharedResourceSubscriptionManager.open_session",
            "self._state",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/services/view_store.py"),
            "ServiceViewStore.watch",
            "self._state",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/services/view_store.py"),
            "ServiceViewStore._event_loop",
            "self._bucket",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/services/view_access.py"),
            "ManagedServiceViewAccess.watch",
            "self._store",
            "watch",
        ): 1,
        (
            Path("src/deckr/bindings/nats.py"),
            "DeckrNatsSubstrate.request",
            "self._nc",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/bindings/nats.py"),
            "DeckrNatsSubstrate.subscribe",
            "self._nc",
            "subscribe",
        ): 2,
        (
            Path("src/deckr/substrates/_nats_watch.py"),
            "NatsKvWatch.start",
            "self._connection",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/core/kv_source.py"),
            "KvMaterializedSource.subscribe",
            "self._broadcaster",
            "subscribe",
        ): 1,
        (
            Path("src/deckr/core/kv_source.py"),
            "KvMaterializedSource._consume_one_watch",
            "self._bucket",
            "watch",
        ): 1,
        (
            Path("src/deckr/substrates/_nats_kv_records.py"),
            "kv_keys_by_watch",
            "kv",
            "watch",
        ): 1,
        # Exact maintenance snapshots own and delete their temporary consumer.
        (
            Path("src/deckr/substrates/_nats_kv_records.py"),
            "kv_snapshot_entries",
            "kv",
            "watch",
        ): 1,
        (
            Path("src/deckr/substrates/_supervised_substrate.py"),
            "SupervisedNatsSubstrate.subscribe",
            "self._connected_nats()",
            "subscribe",
        ): 1,
    }
)


def test_production_watch_and_subscription_boundaries_are_exactly_inventoried() -> None:
    repository = Path(__file__).resolve().parents[1]
    production_files = _production_python_files(repository)
    observed: Counter[_WatchBoundary] = Counter()
    for path in production_files:
        relative = path.relative_to(repository)
        observed.update(_watch_boundaries(relative, path.read_text()))

    available_paths = {path.relative_to(repository) for path in production_files}
    expected = Counter(
        {
            boundary: 1
            for boundary in _APPROVED_AUTHORITY_STATE_BOUNDARIES
            if boundary[0] in available_paths
        }
    )
    expected.update(
        {
            boundary: count
            for boundary, count in _APPROVED_OTHER_WATCH_BOUNDARIES.items()
            if boundary[0] in available_paths
        }
    )
    unexpected = observed - expected
    stale_allowlist = expected - observed
    assert not unexpected and not stale_allowlist, _format_boundary_diff(
        unexpected=unexpected,
        stale_allowlist=stale_allowlist,
    )


def test_watch_inventory_does_not_depend_on_authority_receiver_names() -> None:
    source = """
class Example:
    async def run(self):
        await arbitrary_alias.subscribe()
        await renamed_source.watch()
        await self.contract_source.subscribe()
"""
    path = Path("deckr-example/src/example.py")
    assert _watch_boundaries(path, source) == Counter(
        {
            (path, "Example.run", "arbitrary_alias", "subscribe"): 1,
            (path, "Example.run", "renamed_source", "watch"): 1,
            (path, "Example.run", "self.contract_source", "subscribe"): 1,
        }
    )


class _WatchBoundaryVisitor(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self._path = path
        self._scope: list[str] = []
        self.boundaries: Counter[_WatchBoundary] = Counter()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._scope.append(node.name)
        self.generic_visit(node)
        self._scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_callable(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_callable(node)

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute) and node.func.attr in {
            "subscribe",
            "watch",
            "watch_state",
        }:
            scope = ".".join(self._scope) if self._scope else "<module>"
            self.boundaries[
                (
                    self._path,
                    scope,
                    ast.unparse(node.func.value),
                    node.func.attr,
                )
            ] += 1
        self.generic_visit(node)

    def _visit_callable(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        self._scope.append(node.name)
        self.generic_visit(node)
        self._scope.pop()


def _watch_boundaries(path: Path, source: str) -> Counter[_WatchBoundary]:
    visitor = _WatchBoundaryVisitor(path)
    visitor.visit(ast.parse(source, filename=str(path)))
    return visitor.boundaries


def _production_python_files(repository: Path) -> tuple[Path, ...]:
    return tuple(
        sorted(
            path
            for path in (repository / "src").rglob("*.py")
            if "tests" not in path.parts
            and "__pycache__" not in path.parts
            and "testing" not in path.parts
        )
    )


def _format_boundary_diff(
    *,
    unexpected: Counter[_WatchBoundary],
    stale_allowlist: Counter[_WatchBoundary],
) -> str:
    lines: list[str] = []
    if unexpected:
        lines.append("unexpected watch/subscription boundaries:")
        lines.extend(_format_boundaries(unexpected))
    if stale_allowlist:
        lines.append("stale watch/subscription allowlist entries:")
        lines.extend(_format_boundaries(stale_allowlist))
    return "\n".join(lines)


def _format_boundaries(boundaries: Counter[_WatchBoundary]) -> list[str]:
    return [
        f"  {path}:{scope}: {receiver}.{operation}(...) x{count}"
        for (path, scope, receiver, operation), count in sorted(boundaries.items())
    ]
