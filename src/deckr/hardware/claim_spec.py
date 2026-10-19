"""Local claim intent; the wire terms remain the existing hardware profile."""

from dataclasses import dataclass
from math import isfinite

from deckr.hardware.discovery import HardwareCandidate


@dataclass(frozen=True, slots=True)
class HardwareClaimSpec:
    candidates: tuple[HardwareCandidate, ...]
    timeout_seconds: float = 10.0
    event_buffer_size: int = 64

    def __post_init__(self) -> None:
        candidates = tuple(self.candidates)
        if not candidates:
            raise ValueError("A hardware claim requires at least one device")
        if (
            len({(str(c.manager_endpoint), c.manager_session_id) for c in candidates})
            != 1
        ):
            raise ValueError("All claimed devices must belong to one manager session")
        if len({c.device_ref.device_id for c in candidates}) != len(candidates):
            raise ValueError("Claim device ids must be unique")
        if (
            not isfinite(self.timeout_seconds)
            or self.timeout_seconds <= 0
            or type(self.event_buffer_size) is not int
            or not 1 <= self.event_buffer_size <= 4096
        ):
            raise ValueError(
                "Claim timeout must be positive and event buffer must be between 1 and 4096"
            )
        object.__setattr__(self, "candidates", candidates)
