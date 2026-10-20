"""State."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from deckr.contracts.authority import ContractPointer
from deckr.services.runtime import ServiceDescriptor


class ManagedServicePhase(StrEnum):
    CONNECTING = "connecting"
    READY = "ready"
    RECONNECTING = "reconnecting"
    UNAVAILABLE = "unavailable"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class ManagedServiceFailure:
    code: str
    message: str
    diagnostics: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "diagnostics",
            MappingProxyType(dict(self.diagnostics)),
        )


@dataclass(frozen=True, slots=True)
class ManagedServiceState:
    version: int
    phase: ManagedServicePhase
    descriptor: ServiceDescriptor | None
    local_authority: bool
    failure: ManagedServiceFailure | None = None
    cleanup_failures: int = 0
    last_cleanup_failure: ManagedServiceFailure | None = None
    # Observation only; managed operations still check the current lease.
    contract: ContractPointer | None = None


ManagedServiceStatePredicate = Callable[[ManagedServiceState], bool]
