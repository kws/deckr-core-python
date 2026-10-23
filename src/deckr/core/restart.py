"""Trusted supervisor evidence; parsing does not itself verify process death."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from deckr.core.identity import parse_endpoint_address

RESTART_HANDOFF_SCHEMA = "dev.deckr.runtime.restart_handoff.v1"


def _text(value: Any) -> str:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError("restart identity must be nonempty unpadded text")
    return value


@dataclass(frozen=True)
class VerifiedRestart:
    """Authenticated supervisor assertion of exact exited process lifetimes.

    The caller authenticates the channel and the process/session association;
    structural validation alone does not establish that a process exited.
    """

    instance_id: str
    participants: frozenset[tuple[str, str]]

    def contains(self, endpoint: object, session_id: str) -> bool:
        return (str(endpoint), session_id) in self.participants

    @classmethod
    def from_handoff(cls, value: Any, *, instance_id: str) -> VerifiedRestart:
        if not isinstance(value, dict) or set(value) != {
            "schema",
            "instanceId",
            "predecessors",
        }:
            raise ValueError("invalid restart handoff fields")
        if value["schema"] != RESTART_HANDOFF_SCHEMA:
            raise ValueError("invalid restart handoff schema")
        if value["instanceId"] != _text(instance_id):
            raise ValueError("restart handoff belongs to a different instance")
        predecessors = value["predecessors"]
        if not isinstance(predecessors, list) or not 1 <= len(predecessors) <= 256:
            raise ValueError("restart predecessors must contain 1 to 256 lifetimes")
        participants, lifetimes = set(), set()
        for predecessor in predecessors:
            if not isinstance(predecessor, dict) or set(predecessor) != {
                "predecessorId",
                "exitCode",
                "participants",
            }:
                raise ValueError("invalid restart predecessor fields")
            lifetime = _text(predecessor["predecessorId"])
            if lifetime in lifetimes:
                raise ValueError("duplicate restart predecessor")
            lifetimes.add(lifetime)
            exit_code = predecessor["exitCode"]
            if type(exit_code) is not int or not -(2**31) <= exit_code < 2**31:
                raise ValueError("restart exitCode must be a signed 32-bit integer")
            rows = predecessor["participants"]
            if not isinstance(rows, list) or not 1 <= len(rows) <= 256:
                raise ValueError("invalid restart participants")
            for row in rows:
                if not isinstance(row, dict) or set(row) != {"endpoint", "sessionId"}:
                    raise ValueError("invalid restart participant fields")
                identity = (_text(row["endpoint"]), _text(row["sessionId"]))
                parse_endpoint_address(identity[0])
                if identity in participants:
                    raise ValueError("duplicate restart participant")
                participants.add(identity)
        return cls(instance_id, frozenset(participants))


def load_restart_handoff(
    path: str | Path, *, instance_id: str
) -> VerifiedRestart | None:
    """Read an explicitly trusted supervisor file; absence permits first boot."""
    path = Path(path)
    if not path.is_absolute():
        raise ValueError("restart handoff path must be absolute")

    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate restart handoff field")
            result[key] = value
        return result

    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    if len(raw) > 262144:
        raise ValueError("restart handoff is too large")
    return VerifiedRestart.from_handoff(
        json.loads(raw, object_pairs_hook=unique), instance_id=instance_id
    )


def restart_handoff_from_env() -> VerifiedRestart | None:
    path = os.environ.get("DECKR_RESTART_HANDOFF")
    if path is None:
        return None
    instance = os.environ.get("DECKR_INSTANCE_ID")
    if not instance:
        raise ValueError("DECKR_INSTANCE_ID is required with DECKR_RESTART_HANDOFF")
    return load_restart_handoff(path, instance_id=instance)
