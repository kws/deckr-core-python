"""Immutable local hardware state; discovery is separate from authority."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType

from deckr.contracts.authority import ContractPointer
from deckr.contracts.models import freeze_json
from deckr.hardware.claims import LiveHardwareClaim
from deckr.hardware.descriptors import DeviceDescriptor


class HardwarePhase(StrEnum):
    STARTING = "starting"
    READY = "ready"
    RECONNECTING = "reconnecting"
    UNAVAILABLE = "unavailable"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class HardwareFailure:
    code: str
    message: str
    diagnostics: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "diagnostics", freeze_json(dict(self.diagnostics)))


class HardwareUnavailable(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class HardwareManagerState:
    version: int
    phase: HardwarePhase
    inventory: Mapping[str, DeviceDescriptor]
    claims: tuple[LiveHardwareClaim, ...]
    discovery_current: bool
    advertisement_published: bool
    authority_current: bool
    failure: HardwareFailure | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "inventory", MappingProxyType(dict(self.inventory)))


@dataclass(frozen=True, slots=True)
class HardwareClaimState:
    version: int
    phase: HardwarePhase
    contract: ContractPointer | None
    local_authority: bool
    descriptors: Mapping[str, DeviceDescriptor]
    failure: HardwareFailure | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "descriptors", MappingProxyType(dict(self.descriptors))
        )
