"""Mutable authority resources shared by participant scheduling and attachment workers."""

from __future__ import annotations

import anyio

from deckr._concord._lease import ConcordParticipantLease
from deckr._concord._models import ConcordLifecyclePhase, ContractHandle
from deckr._concord._participant_models import ConcordManagedContract
from deckr.core.identity import ContractPointer
from deckr.core.util.anyio import CoalescedStateBroadcaster


class ParticipantResources:
    def __init__(self) -> None:
        self.pointer_locks: dict[ContractPointer, anyio.Lock] = {}
        self.managed: dict[str, ConcordManagedContract] = {}
        self.leases: dict[str, ConcordParticipantLease] = {}
        self.selected: frozenset[ContractPointer] = frozenset()
        self.retired_pointers: set[ContractPointer] = set()
        self.cleanup_contracts: dict[ContractPointer, ContractHandle] = {}
        self.cleanup_leases: dict[ContractPointer, ConcordParticipantLease] = {}
        self.cleanup_running: set[ContractPointer] = set()
        self.cleanup_wakeups: dict[ContractPointer, anyio.Event] = {}
        self.failures: dict[ContractPointer, tuple[ConcordLifecyclePhase, str]] = {}
        self.state_bus = CoalescedStateBroadcaster[ContractPointer](current=False)
        self.task_group: anyio.abc.TaskGroup | None = None
        self.closed = False
