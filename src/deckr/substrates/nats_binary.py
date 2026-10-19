"""Nats binary."""

from __future__ import annotations

import logging
import os
import re
import shutil
import signal
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


_DEFAULT_HOST = "127.0.0.1"


_DEFAULT_PORT = -1


_DEFAULT_MINIMUM_VERSION = "2.14.0"


_SIGKILL = getattr(signal, "SIGKILL", signal.SIGTERM)


_VERSION_RE = re.compile(r"\bv(?P<version>\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?)\b")


class NatsServerSupervisorError(RuntimeError):
    """Base class for supervised nats-server failures."""


class NatsServerBinaryResolutionError(NatsServerSupervisorError):
    """Raised when no usable nats-server binary can be resolved."""


class NatsServerVersionError(NatsServerSupervisorError):
    """Raised when a resolved nats-server binary is too old or invalid."""


class NatsServerStartupError(NatsServerSupervisorError):
    """Raised when the supervised nats-server does not become ready."""


class NatsServerProcessExited(NatsServerSupervisorError):
    """Raised when nats-server exits while the supervised runtime is active."""


@dataclass(frozen=True, slots=True)
class ResolvedNatsServerBinary:
    path: Path
    version: str
    source: str


class NatsServerBinaryResolver:
    def __init__(
        self,
        *,
        server_path: Path | str | None = None,
        minimum_version: str = _DEFAULT_MINIMUM_VERSION,
    ) -> None:
        self.server_path = Path(server_path).expanduser() if server_path else None
        self.minimum_version = minimum_version

    def resolve(self) -> ResolvedNatsServerBinary:
        candidates = list(self._candidates())
        errors: list[str] = []
        for path, source in candidates:
            try:
                return self._resolve_candidate(path, source=source)
            except NatsServerSupervisorError as exc:
                errors.append(str(exc))
        if not errors:
            errors.append(
                "No configured, bundled, or system nats-server binary was found"
            )
        raise NatsServerBinaryResolutionError("; ".join(errors))

    def _candidates(self) -> Iterator[tuple[Path, str]]:
        if self.server_path is not None:
            if not self.server_path.is_absolute():
                raise NatsServerBinaryResolutionError(
                    "Configured nats-server path must be absolute"
                )
            yield self.server_path, "configured"
            return

        bundled = _bundled_nats_server_path()
        if bundled is not None:
            yield bundled, "bundled"

        system = shutil.which("nats-server")
        if system:
            yield Path(system), "path"

    def _resolve_candidate(
        self,
        path: Path,
        *,
        source: str,
    ) -> ResolvedNatsServerBinary:
        if not path.exists():
            raise NatsServerBinaryResolutionError(
                f"nats-server binary does not exist: {path}"
            )
        if not os.access(path, os.X_OK):
            raise NatsServerBinaryResolutionError(
                f"nats-server binary is not executable: {path}"
            )
        version = _read_nats_server_version(path)
        _assert_minimum_version(version, self.minimum_version, path=path)
        return ResolvedNatsServerBinary(path=path, version=version, source=source)


def _bundled_nats_server_path() -> Path | None:
    try:
        from deckr_nats_server_bin import (  # type: ignore[import-not-found]
            NatsServerBinaryNotFound,
            nats_server_path,
        )
    except ModuleNotFoundError:
        return None
    try:
        return nats_server_path()
    except NatsServerBinaryNotFound:
        return None


def _read_nats_server_version(path: Path) -> str:
    try:
        result = subprocess.run(
            [str(path), "-v"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise NatsServerVersionError(
            f"Could not execute nats-server binary {path}: {exc}"
        ) from exc
    output = "\n".join(part for part in (result.stdout, result.stderr) if part)
    match = _VERSION_RE.search(output)
    if result.returncode != 0 or match is None:
        raise NatsServerVersionError(
            f"Could not determine nats-server version from {path}"
        )
    return match.group("version")


def _assert_minimum_version(version: str, minimum: str, *, path: Path) -> None:
    if _version_key(version) < _version_key(minimum):
        raise NatsServerVersionError(
            f"nats-server {version} at {path} is older than required {minimum}"
        )


def _version_key(version: str) -> tuple[int, int, int]:
    core = version.split("-", 1)[0].split("+", 1)[0]
    major, minor, patch = core.split(".")
    return int(major), int(minor), int(patch)
