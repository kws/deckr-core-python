"""Acknowledged local session reporting to a trusted host supervisor.

The host authenticates the caller's process lifetime and owns the durable journal.
This channel reports identity, never protocol authority or failure inferred from
discovery. A report must be acknowledged before an endpoint can be used.
"""

from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass
from pathlib import Path

import anyio

from deckr.core.restart import VerifiedRestart
from deckr.lanes import EndpointSession


@dataclass(frozen=True)
class SupervisorChannel:
    path: str
    instance_id: str

    @classmethod
    def from_env(cls) -> SupervisorChannel | None:
        path = os.environ.get("DECKR_SUPERVISOR_SOCKET")
        if path is None:
            return None
        instance = os.environ.get("DECKR_INSTANCE_ID")
        handoff = os.environ.get("DECKR_RESTART_HANDOFF")
        if (
            not Path(path).is_absolute()
            or not instance
            or instance.strip() != instance
            or not handoff
            or not Path(handoff).is_absolute()
        ):
            raise ValueError(
                "supervisor socket requires absolute paths and instance ID"
            )
        return cls(path, instance)

    async def register(self, session: EndpointSession) -> None:
        await self._send(
            dict(
                op="register",
                endpoint=str(session.address),
                sessionId=session.session_id,
            )
        )

    async def cleaned(self, restart: VerifiedRestart | None) -> None:
        await self._send(
            dict(
                op="cleaned",
                participants=sorted(restart.participants) if restart else [],
            )
        )

    async def _send(self, message: dict) -> None:
        await anyio.to_thread.run_sync(
            self._request,
            dict(message, instanceId=self.instance_id),
            abandon_on_cancel=True,
        )

    def _request(self, message: dict) -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(5)
            connection.connect(self.path)
            connection.sendall(json.dumps(message).encode() + b"\n")
            with connection.makefile("rb") as stream:
                response = stream.readline(4097)
            if len(response) > 4096 or not response.endswith(b"\n"):
                raise RuntimeError("invalid supervisor acknowledgement")
            if json.loads(response) != {"ok": True}:
                raise RuntimeError(
                    "supervisor did not acknowledge durable session state"
                )
