from __future__ import annotations

import ast
import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

_Boundary = tuple[Path, str, str, str]

_STATE_STREAM_OWNERS = {
    (
        Path("src/deckr/core/util/state.py"),
        "CoalescedStateBroadcaster",
    ),
    (Path("src/deckr/core/kv_source.py"), "KvMaterializedSource"),
    (Path("src/deckr/_beacon/_api.py"), "Beacon"),
    (Path("src/deckr/beacon.py"), "_BeaconSubscriber"),
    (Path("src/deckr/_concord/_api.py"), "Concord"),
    (Path("src/deckr/concord.py"), "_ConcordSubscriber"),
    (Path("src/deckr/_concord/_participant_context.py"), "ConcordParticipant"),
    (Path("src/deckr/services/view_store.py"), "ServiceViewStore"),
    (
        Path("src/deckr/services/subscription_manager.py"),
        "SharedResourceSubscriptionManager",
    ),
}


@dataclass(frozen=True, slots=True)
class _TemporaryBoundary:
    phase: str
    reason: str
    count: int = 1


# These are the deliberately bounded state-fanout primitives left after Phase 3.
# Keeping exact semantic call sites here makes accidental replacement with an
# unbounded registry or queue visible in the same audit as temporary debt.
_PERMANENT_BOUNDED_BOUNDARIES: dict[_Boundary, int] = {
    # Released ownership is pruned against the open-contract snapshot at bootstrap.
    (
        Path("src/deckr/_concord/_owner.py"),
        "ConcordOwnerBootstrap.__init__",
        "self._released",
        "task_or_reply_route_map",
    ): 1,
    # Raw ingress is bounded and overflow permanently fails this consumer.
    (
        Path("src/deckr/substrates/_nats_watch.py"),
        "NatsKvWatch.__init__",
        "self._send, self._receive",
        "bounded_memory_stream",
    ): 1,
    (
        Path("src/deckr/_concord/_participant_resources.py"),
        "ParticipantResources.__init__",
        "self.managed",
        "task_or_reply_route_map",
    ): 1,
    (
        Path("src/deckr/_concord/_participant_resources.py"),
        "ParticipantResources.__init__",
        "self.leases",
        "task_or_reply_route_map",
    ): 1,
    (
        Path("src/deckr/_concord/_participant_resources.py"),
        "ParticipantResources.__init__",
        "self.cleanup_leases",
        "task_or_reply_route_map",
    ): 1,
    (
        Path("src/deckr/_concord/_participant_resources.py"),
        "ParticipantResources.__init__",
        "self.cleanup_wakeups",
        "task_or_reply_route_map",
    ): 1,
    (
        Path("src/deckr/core/util/state.py"),
        "_CancellationSafeMutex.__init__",
        "self._scopes",
        "task_or_reply_route_map",
    ): 1,
    (
        Path("src/deckr/core/util/state.py"),
        "CoalescedStateBroadcaster.__init__",
        "self._registrations",
        "subscriber_registry",
    ): 1,
    (
        Path("src/deckr/core/util/state.py"),
        "CoalescedStateBroadcaster.subscribe",
        "send, receive",
        "bounded_state_stream",
    ): 1,
    (
        Path("src/deckr/bindings/nats.py"),
        "DeckrNatsSubstrate.request",
        "send, receive",
        "bounded_memory_stream",
    ): 1,
    (
        Path("src/deckr/services/_client.py"),
        "DeckrServices._race_service_candidates",
        "send, receive",
        "bounded_memory_stream",
    ): 1,
    (
        Path("src/deckr/substrates/nats_server.py"),
        "NatsServerSupervisor.__init__",
        "self._logs",
        "bounded_queue",
    ): 1,
}


# Phase 0 freezes each unsafe creator by semantic call site. These are not
# approved APIs: every entry names the implementation phase that removes or
# bounds it, and the test fails when an entry disappears without its metadata
# being removed too.
_TEMPORARY_BOUNDARIES: dict[_Boundary, _TemporaryBoundary] = {
    (
        Path("src/deckr/components/manager.py"),
        "ComponentManager.__init__",
        "self._running",
        "task_or_reply_route_map",
    ): _TemporaryBoundary(
        phase="Phase 5",
        reason="cap live components and their owned work",
    ),
    (
        Path("src/deckr/components/manager.py"),
        "ComponentManager.__init__",
        "self._lifecycle_by_name",
        "task_or_reply_route_map",
    ): _TemporaryBoundary(
        phase="Phase 5",
        reason="bound component lifecycle identities with component admission",
    ),
    (
        Path("src/deckr/services/_client.py"),
        "DeckrServices.__init__",
        "self._active_service_use_leases",
        "task_or_reply_route_map",
    ): _TemporaryBoundary(
        phase="Phase 5B",
        reason="cap and close active service-use lease owners",
    ),
    (
        Path("src/deckr/components/manager.py"),
        "ComponentManager.__init__",
        "self._event_send, self._event_receive",
        "bounded_memory_stream",
    ): _TemporaryBoundary(
        phase="Phase 5",
        reason="replace oversized component ingress with explicit admission",
    ),
    (
        Path("src/deckr/lanes.py"),
        "EndpointSession.__init__",
        "self._subscriptions",
        "listener_or_sender_registry",
    ): _TemporaryBoundary(
        phase="Phase 5",
        reason="cap endpoint subscription registrations and own their shutdown",
    ),
    (
        Path("src/deckr/bindings/nats.py"),
        "DeckrNatsSubstrate.subscribe",
        "send, receive",
        "bounded_memory_stream",
    ): _TemporaryBoundary(
        phase="Phase 5",
        reason="enforce the lane capacity invariant and typed overload closure",
    ),
    (
        Path("src/deckr/services/subscription_manager.py"),
        "SharedResourceSubscriptionManager.__init__",
        "self._subscribers",
        "subscriber_registry",
    ): _TemporaryBoundary(
        phase="Phase 5B",
        reason="cap logical sessions under provider-lease-scoped ownership",
    ),
    (
        Path("src/deckr/bindings/nats.py"),
        "DeckrNatsSubstrate.__init__",
        "self._reply_subjects",
        "task_or_reply_route_map",
    ): _TemporaryBoundary(
        phase="Phase 5",
        reason="bound in-flight request routes and clean every terminal path",
    ),
}


def test_production_queue_and_task_boundaries_are_inventoried() -> None:
    repository = Path(__file__).resolve().parents[1]
    production_files = _production_python_files(repository)
    observed: Counter[_Boundary] = Counter()
    for path in production_files:
        relative = path.relative_to(repository)
        tree = ast.parse(path.read_text(), filename=str(path))
        visitor = _QueueTaskBoundaryVisitor(relative)
        visitor.visit(tree)
        observed.update(visitor.boundaries)

    available_paths = {path.relative_to(repository) for path in production_files}
    allowed = Counter(
        {
            boundary: metadata.count
            for boundary, metadata in _TEMPORARY_BOUNDARIES.items()
            if boundary[0] in available_paths
        }
    )
    allowed.update(
        {
            boundary: count
            for boundary, count in _PERMANENT_BOUNDED_BOUNDARIES.items()
            if boundary[0] in available_paths
        }
    )
    unexpected = observed - allowed
    stale_allowlist = allowed - observed
    assert not unexpected and not stale_allowlist, _format_boundary_diff(
        unexpected=unexpected,
        stale_allowlist=stale_allowlist,
    )


def test_queue_task_audit_detects_each_forbidden_boundary_shape() -> None:
    tree = ast.parse(
        """
class Example:
    def __init__(self):
        self.omitted = anyio.create_memory_object_stream()
        self.zero = anyio.create_memory_object_stream(max_buffer_size=0)
        self.infinite = anyio.create_memory_object_stream(inf)
        self.queue = Queue()
        self.history = deque()
        self.subscribers = SubscribableQueue(maxsize=10)
        self.scheduled = ScheduledQueue()
        self.reply_routes = {}
        self.tasks = {}
        self.bounded = anyio.create_memory_object_stream(capacity)
        self.window = deque(maxlen=capacity)
        self.commands = Queue(maxsize=10)
"""
    )
    visitor = _QueueTaskBoundaryVisitor(Path("deckr-example/src/example.py"))
    visitor.visit(tree)

    assert visitor.boundaries == Counter(
        {
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.omitted",
                "memory_stream_without_positive_capacity",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.zero",
                "memory_stream_without_positive_capacity",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.infinite",
                "memory_stream_without_positive_capacity",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.queue",
                "queue_without_positive_capacity",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.history",
                "queue_without_positive_capacity",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.subscribers",
                "SubscribableQueue",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.scheduled",
                "ScheduledQueue",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.reply_routes",
                "task_or_reply_route_map",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.tasks",
                "task_or_reply_route_map",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.bounded",
                "bounded_memory_stream",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.window",
                "bounded_queue",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.commands",
                "bounded_queue",
            ): 1,
        }
    )


def test_queue_task_audit_detects_bounded_state_streams_and_registries() -> None:
    tree = ast.parse(
        """
class KvMaterializedSource:
    def __init__(self):
        self._subscribers: set[object] = set()

    async def subscribe(self):
        send, receive = anyio.create_memory_object_stream(max_buffer_size=capacity)
"""
    )
    visitor = _QueueTaskBoundaryVisitor(Path("src/deckr/core/kv_source.py"))
    visitor.visit(tree)

    assert visitor.boundaries == Counter(
        {
            (
                Path("src/deckr/core/kv_source.py"),
                "KvMaterializedSource.__init__",
                "self._subscribers",
                "subscriber_registry",
            ): 1,
            (
                Path("src/deckr/core/kv_source.py"),
                "KvMaterializedSource.subscribe",
                "send, receive",
                "bounded_state_stream",
            ): 1,
        }
    )


def test_queue_task_audit_detects_typed_delivery_registries_after_rename() -> None:
    tree = ast.parse(
        """
class Example:
    def __init__(self):
        self.targets: set[anyio.abc.ObjectSendStream[bytes]] = set()
        self.observers: set[StateListener] = set()
        self.opaque: dict[str, anyio.CancelScope] = {}
"""
    )
    visitor = _QueueTaskBoundaryVisitor(Path("deckr-example/src/example.py"))
    visitor.visit(tree)

    assert visitor.boundaries == Counter(
        {
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.targets",
                "listener_or_sender_registry",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.observers",
                "listener_or_sender_registry",
            ): 1,
            (
                Path("deckr-example/src/example.py"),
                "Example.__init__",
                "self.opaque",
                "task_or_reply_route_map",
            ): 1,
        }
    )


class _QueueTaskBoundaryVisitor(ast.NodeVisitor):
    def __init__(self, path: Path) -> None:
        self._path = path
        self._scope: list[str] = []
        self._assigned_calls: set[int] = set()
        self.boundaries: Counter[_Boundary] = Counter()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._scope.append(node.name)
        self.generic_visit(node)
        self._scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_callable(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_callable(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        owner = ", ".join(_target_name(target) for target in node.targets)
        self._inspect_assigned_value(node.value, owner=owner, annotation="")
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        owner = _target_name(node.target)
        annotation = ast.unparse(node.annotation)
        if node.value is not None:
            self._inspect_assigned_value(
                node.value,
                owner=owner,
                annotation=annotation,
            )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if id(node) not in self._assigned_calls:
            self._inspect_queue_call(node, owner="<expression>")
        self.generic_visit(node)

    def _visit_callable(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
    ) -> None:
        self._scope.append(node.name)
        self.generic_visit(node)
        self._scope.pop()

    def _inspect_assigned_value(
        self,
        value: ast.AST,
        *,
        owner: str,
        annotation: str,
    ) -> None:
        if isinstance(value, ast.Call):
            self._assigned_calls.add(id(value))
            self._inspect_queue_call(value, owner=owner)
        if _is_mapping_initializer(value) and _is_task_or_reply_route_map(
            owner,
            annotation=annotation,
        ):
            self._record(owner, "task_or_reply_route_map")
        if _is_collection_initializer(value):
            if _is_subscriber_registry(owner) or (
                self._is_state_owner_scope() and _is_registration_registry(owner)
            ):
                self._record(owner, "subscriber_registry")
            elif _is_listener_or_sender_registry(owner, annotation=annotation):
                self._record(owner, "listener_or_sender_registry")
        if (
            self._is_state_owner_scope()
            and owner.rsplit(".", 1)[-1].lower().startswith("pending_")
            and _is_list_initializer(value)
        ):
            self._record(owner, "state_queue_without_capacity")

    def _inspect_queue_call(self, node: ast.Call, *, owner: str) -> None:
        name = _callable_leaf_name(node.func)
        if name == "create_memory_object_stream":
            capacity = _call_capacity(node, keyword="max_buffer_size")
            if capacity is None or not _is_explicit_positive_capacity(capacity):
                self._record(owner, "memory_stream_without_positive_capacity")
            elif self._is_state_owner_scope():
                self._record(owner, "bounded_state_stream")
            else:
                self._record(owner, "bounded_memory_stream")
            return
        if name in {"Queue", "PriorityQueue", "LifoQueue"}:
            capacity = _call_capacity(node, keyword="maxsize")
            if capacity is None or not _is_explicit_positive_capacity(capacity):
                self._record(owner, "queue_without_positive_capacity")
            else:
                self._record(owner, "bounded_queue")
            return
        if name == "deque":
            capacity = _call_capacity(node, keyword="maxlen", position=1)
            if capacity is None or not _is_explicit_positive_capacity(capacity):
                self._record(owner, "queue_without_positive_capacity")
            else:
                self._record(owner, "bounded_queue")
            return
        if name in {"SubscribableQueue", "ScheduledQueue"}:
            self._record(owner, name)

    def _record(self, owner: str, kind: str) -> None:
        scope = ".".join(self._scope) if self._scope else "<module>"
        self.boundaries[(self._path, scope, owner, kind)] += 1

    def _is_state_owner_scope(self) -> bool:
        return any(
            self._path == path and owner in self._scope
            for path, owner in _STATE_STREAM_OWNERS
        )


def _callable_leaf_name(node: ast.AST) -> str:
    while isinstance(node, ast.Subscript):
        node = node.value
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _call_capacity(
    node: ast.Call,
    *,
    keyword: str,
    position: int = 0,
) -> ast.AST | None:
    for item in node.keywords:
        if item.arg == keyword:
            return item.value
    return node.args[position] if len(node.args) > position else None


def _is_explicit_positive_capacity(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant):
        return (
            isinstance(node.value, int | float)
            and not isinstance(node.value, bool)
            and math.isfinite(node.value)
            and node.value > 0
        )
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        if isinstance(node.operand, ast.Constant) and isinstance(
            node.operand.value, int | float
        ):
            value = node.operand.value
            if isinstance(node.op, ast.USub):
                value = -value
            return math.isfinite(value) and value > 0
    if isinstance(node, ast.Name) and node.id.lower() in {"inf", "infinity"}:
        return False
    if isinstance(node, ast.Attribute) and node.attr.lower() in {"inf", "infinity"}:
        return False
    # A named capacity is explicit; its owning constructor must enforce its
    # positive runtime invariant. This guard catches omitted, zero, negative,
    # and infinite production capacity at the creator boundary.
    return not (
        isinstance(node, ast.Call)
        and _callable_leaf_name(node.func) == "float"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and str(node.args[0].value).lower()
        in {
            "inf",
            "+inf",
            "-inf",
            "infinity",
            "+infinity",
            "-infinity",
            "nan",
        }
    )


def _is_mapping_initializer(node: ast.AST) -> bool:
    if isinstance(node, (ast.Dict, ast.DictComp)):
        return True
    if not isinstance(node, ast.Call):
        return False
    if _callable_leaf_name(node.func) in {"dict", "defaultdict"}:
        return True
    return _callable_leaf_name(node.func) == "field" and any(
        item.arg == "default_factory"
        and _callable_leaf_name(item.value) in {"dict", "defaultdict"}
        for item in node.keywords
    )


def _is_collection_initializer(node: ast.AST) -> bool:
    if isinstance(node, (ast.Dict, ast.DictComp, ast.Set, ast.SetComp)):
        return True
    if not isinstance(node, ast.Call):
        return False
    name = _callable_leaf_name(node.func)
    if name in {"dict", "defaultdict", "set"}:
        return True
    return name == "field" and any(
        item.arg == "default_factory"
        and _callable_leaf_name(item.value) in {"dict", "defaultdict", "set"}
        for item in node.keywords
    )


def _is_list_initializer(node: ast.AST) -> bool:
    if isinstance(node, (ast.List, ast.ListComp)):
        return True
    if not isinstance(node, ast.Call):
        return False
    name = _callable_leaf_name(node.func)
    if name == "list":
        return True
    return name == "field" and any(
        item.arg == "default_factory" and _callable_leaf_name(item.value) == "list"
        for item in node.keywords
    )


def _is_subscriber_registry(owner: str) -> bool:
    return owner.rsplit(".", 1)[-1].lower() in {"subscribers", "_subscribers"}


def _is_registration_registry(owner: str) -> bool:
    return owner.rsplit(".", 1)[-1].lower() in {"registrations", "_registrations"}


def _is_listener_or_sender_registry(owner: str, *, annotation: str) -> bool:
    name = owner.rsplit(".", 1)[-1].lower()
    if name in {
        "listeners",
        "_listeners",
        "senders",
        "_senders",
        "subscriptions",
        "_subscriptions",
    }:
        return True
    # Type-aware matching keeps renamed delivery registries visible. These are
    # capabilities/registrations rather than ordinary data collections.
    type_names = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", annotation)
    return any(
        type_name.lower().endswith(("listener", "sendstream", "subscription"))
        for type_name in type_names
    )


def _is_task_or_reply_route_map(owner: str, *, annotation: str) -> bool:
    name = owner.rsplit(".", 1)[-1].lower()
    retained_owner_type_suffixes = (
        "binding",
        "cancelscope",
        "context",
        "event",
        "inflight",
        "instance",
        "lease",
        "route",
        "runningcomponent",
        "runtime",
        "scope",
        "session",
        "task",
        "watcher",
        "worker",
    )
    is_instance_member = owner.startswith("self.")
    annotation_type_names = tuple(
        type_name.lower()
        for type_name in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", annotation)
    )
    return (
        (
            is_instance_member
            and any(
                type_name.startswith("managed")
                or type_name.endswith(retained_owner_type_suffixes)
                for type_name in annotation_type_names
            )
        )
        or "dispatch_lane" in name
        or "command_stream" in name
        or "reply" in name
        or "scope" in name
        or "task" in name
        or "worker" in name
        or (
            is_instance_member
            and any(
                owner_token in name
                for owner_token in (
                    "context",
                    "inflight",
                    "instance",
                    "lease",
                    "managed",
                    "route",
                    "runtime",
                    "session",
                )
            )
        )
    )


def _target_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _target_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    if isinstance(node, (ast.Tuple, ast.List)):
        return ", ".join(_target_name(item) for item in node.elts)
    return ast.unparse(node)


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
    unexpected: Counter[_Boundary],
    stale_allowlist: Counter[_Boundary],
) -> str:
    lines: list[str] = []
    if unexpected:
        lines.append("unexpected queue/task boundaries:")
        lines.extend(_format_boundaries(unexpected))
    if stale_allowlist:
        lines.append("stale queue/task allowlist entries:")
        lines.extend(_format_boundaries(stale_allowlist))
    return "\n".join(lines)


def _format_boundaries(boundaries: Counter[_Boundary]) -> list[str]:
    return [
        f"  {path}:{scope}: {owner}: {kind} x{count}"
        for (path, scope, owner, kind), count in sorted(boundaries.items())
    ]
