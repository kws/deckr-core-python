"""Nats server."""

from __future__ import annotations

import json
import os
import secrets
import signal
import subprocess
import tempfile
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anyio

from deckr.substrates.nats_binary import (
    _DEFAULT_HOST,
    _DEFAULT_MINIMUM_VERSION,
    _DEFAULT_PORT,
    _SIGKILL,
    NatsServerBinaryResolver,
    NatsServerProcessExited,
    NatsServerStartupError,
    ResolvedNatsServerBinary,
    logger,
)


@dataclass(frozen=True, slots=True)
class NatsServerHandle:
    url: str
    auth_token: str | None
    runtime_dir: Path
    config_path: Path
    store_dir: Path
    binary: ResolvedNatsServerBinary


class NatsServerSupervisor:
    def __init__(
        self,
        *,
        server_path: Path | str | None = None,
        minimum_version: str = _DEFAULT_MINIMUM_VERSION,
        host: str = _DEFAULT_HOST,
        port: int = _DEFAULT_PORT,
        runtime_dir: Path | str | None = None,
        store_dir: Path | str | None = None,
        server_name: str = "deckr-local-nats",
        auth_enabled: bool = True,
        auth_token: str | None = None,
        startup_timeout: float = 10.0,
        shutdown_timeout: float = 5.0,
        log_buffer_lines: int = 200,
    ) -> None:
        if not host:
            raise ValueError("host must not be empty")
        if startup_timeout <= 0:
            raise ValueError("startup_timeout must be greater than zero")
        if shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be greater than zero")
        if log_buffer_lines <= 0:
            raise ValueError("log_buffer_lines must be greater than zero")
        self._resolver = NatsServerBinaryResolver(
            server_path=server_path,
            minimum_version=minimum_version,
        )
        self.host = host
        self.port = int(port)
        self.runtime_dir = Path(runtime_dir).expanduser() if runtime_dir else None
        self.store_dir = Path(store_dir).expanduser() if store_dir else None
        self.server_name = server_name
        self.auth_enabled = auth_enabled
        self.configured_auth_token = auth_token
        self.startup_timeout = float(startup_timeout)
        self.shutdown_timeout = float(shutdown_timeout)
        self._logs: deque[str] = deque(maxlen=log_buffer_lines)
        self._temporary_dir: tempfile.TemporaryDirectory[str] | None = None
        self._process = None
        self._handle: NatsServerHandle | None = None
        self._auth_token: str | None = None
        self._ports_dir: Path | None = None
        self._stopping = False

    @property
    def handle(self) -> NatsServerHandle | None:
        return self._handle

    @property
    def url(self) -> str | None:
        return self._handle.url if self._handle is not None else None

    @property
    def auth_token(self) -> str | None:
        return self._auth_token

    def recent_logs(self) -> tuple[str, ...]:
        return tuple(self._logs)

    async def start(self) -> NatsServerHandle:
        if self._process is not None:
            raise RuntimeError("nats-server supervisor is already running")

        binary = self._resolver.resolve()
        runtime_dir = self._prepare_runtime_dir()
        store_dir = self._prepare_store_dir(runtime_dir)
        ports_dir = runtime_dir / "ports"
        ports_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        _chmod_private_directory(ports_dir)
        _remove_stale_ports_files(ports_dir)

        token = self._resolve_auth_token()
        config_path = runtime_dir / "nats-server.conf"
        _write_private_text(
            config_path,
            _config_text(
                server_name=self.server_name,
                host=self.host,
                port=self.port,
                ports_dir=ports_dir,
                store_dir=store_dir,
                auth_token=token,
            ),
        )

        self._auth_token = token
        self._ports_dir = ports_dir
        self._stopping = False
        try:
            self._process = await anyio.open_process(
                [str(binary.path), "-c", str(config_path)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=str(runtime_dir),
                start_new_session=True,
            )
            async with anyio.create_task_group() as startup_tg:
                self._start_output_drains(startup_tg)
                try:
                    url = await self._wait_until_ready()
                finally:
                    startup_tg.cancel_scope.cancel()
        except BaseException:
            await self.stop()
            raise

        self._handle = NatsServerHandle(
            url=url,
            auth_token=token,
            runtime_dir=runtime_dir,
            config_path=config_path,
            store_dir=store_dir,
            binary=binary,
        )
        logger.info(
            "Started supervised nats-server %s from %s on %s",
            binary.version,
            binary.source,
            url,
        )
        return self._handle

    def _resolve_auth_token(self) -> str | None:
        if not self.auth_enabled:
            return None
        if self.configured_auth_token is not None:
            return self.configured_auth_token
        return secrets.token_urlsafe(32)

    def start_monitor(self, tg: anyio.abc.TaskGroup) -> None:
        self._start_output_drains(tg)
        tg.start_soon(self._raise_on_unexpected_exit)

    async def stop(self) -> None:
        cleanup_errors: list[BaseException] = []

        with anyio.CancelScope(shield=True):
            self._stopping = True
            process = self._process
            if process is not None:
                logger.info("Stopping supervised nats-server")
                try:
                    await self._stop_process(process)
                except BaseException as err:
                    cleanup_errors.append(err)
                try:
                    with anyio.move_on_after(1.0, shield=True):
                        await process.aclose()
                except BaseException as err:
                    cleanup_errors.append(err)
            self._process = None

            if self._ports_dir is not None:
                try:
                    _remove_stale_ports_files(self._ports_dir)
                except BaseException as err:
                    cleanup_errors.append(err)
            self._ports_dir = None
            self._handle = None
            self._auth_token = None

            if self._temporary_dir is not None:
                try:
                    self._temporary_dir.cleanup()
                except BaseException as err:
                    cleanup_errors.append(err)
                self._temporary_dir = None

        if len(cleanup_errors) == 1:
            raise cleanup_errors[0]
        if cleanup_errors:
            raise BaseExceptionGroup(
                "supervised nats-server cleanup failed",
                cleanup_errors,
            )

    async def _stop_process(self, process: Any) -> None:
        if process.returncode is not None:
            return
        self._signal_process(process, signal.SIGTERM)
        with anyio.move_on_after(self.shutdown_timeout) as scope:
            await process.wait()
        if scope.cancel_called and process.returncode is None:
            logger.warning("Supervised nats-server did not stop gracefully; killing it")
            self._signal_process(process, _SIGKILL)
            await process.wait()
            return
        logger.info("Stopped supervised nats-server gracefully")

    def _signal_process(self, process: Any, sig: int) -> None:
        if _signal_process_group(process, sig):
            return
        if sig == signal.SIGTERM:
            process.terminate()
            return
        process.kill()

    def _start_output_drains(self, tg: anyio.abc.TaskGroup) -> None:
        process = self._process
        if process is None:
            return
        if process.stdout is not None:
            tg.start_soon(self._drain_output, process.stdout, "stdout")
        if process.stderr is not None:
            tg.start_soon(self._drain_output, process.stderr, "stderr")

    async def _drain_output(self, stream, source: str) -> None:
        pending = b""
        try:
            while True:
                chunk = await stream.receive()
                if not chunk:
                    break
                pending += chunk
                while b"\n" in pending:
                    line, pending = pending.split(b"\n", 1)
                    self._append_log(source, line)
        except (
            anyio.BrokenResourceError,
            anyio.ClosedResourceError,
            anyio.EndOfStream,
        ):
            pass
        finally:
            if pending:
                self._append_log(source, pending)

    def _append_log(self, source: str, line: bytes) -> None:
        rendered = line.decode("utf-8", errors="replace").rstrip("\r")
        if rendered:
            self._logs.append(f"{source}: {rendered}")

    async def _wait_until_ready(self) -> str:
        deadline = anyio.current_time() + self.startup_timeout
        last_error: BaseException | None = None
        while anyio.current_time() < deadline:
            process = self._process
            if process is None:
                raise NatsServerStartupError("nats-server process was not started")
            if process.returncode is not None:
                raise NatsServerStartupError(
                    _failure_message(
                        f"nats-server exited with code {process.returncode}",
                        logs=self.recent_logs(),
                    )
                )
            try:
                url = self._read_ports_file_url()
                if url is not None:
                    await _verify_jetstream(url, auth_token=self._auth_token)
                    return url
            except Exception as exc:
                last_error = exc
            await anyio.sleep(0.05)
        detail = "nats-server did not become ready"
        if last_error is not None:
            detail = f"{detail}: {last_error}"
        raise NatsServerStartupError(_failure_message(detail, logs=self.recent_logs()))

    def _read_ports_file_url(self) -> str | None:
        if self._ports_dir is None:
            return None
        for path in sorted(self._ports_dir.glob("*.ports")):
            try:
                return _nats_url_from_ports_file(path)
            except (json.JSONDecodeError, OSError, ValueError):
                continue
        return None

    async def _raise_on_unexpected_exit(self) -> None:
        process = self._process
        if process is None:
            return
        returncode = await process.wait()
        if self._stopping:
            return
        raise NatsServerProcessExited(
            _failure_message(
                f"supervised nats-server exited with code {returncode}",
                logs=self.recent_logs(),
            )
        )

    def _prepare_runtime_dir(self) -> Path:
        if self.runtime_dir is not None:
            self.runtime_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            _chmod_private_directory(self.runtime_dir)
            return self.runtime_dir
        self._temporary_dir = tempfile.TemporaryDirectory(prefix="deckr-nats-")
        path = Path(self._temporary_dir.name)
        _chmod_private_directory(path)
        return path

    def _prepare_store_dir(self, runtime_dir: Path) -> Path:
        store_dir = self.store_dir or runtime_dir / "jetstream"
        store_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        _chmod_private_directory(store_dir)
        return store_dir


def _write_private_text(path: Path, text: str) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    fd = os.open(path, flags, 0o600)
    try:
        os.write(fd, text.encode("utf-8"))
    finally:
        os.close(fd)


def _chmod_private_directory(path: Path) -> None:
    try:
        path.chmod(0o700)
    except OSError:
        logger.debug("Could not set private permissions on %s", path, exc_info=True)


def _remove_stale_ports_files(path: Path) -> None:
    for ports_file in path.glob("*.ports"):
        try:
            ports_file.unlink()
        except FileNotFoundError:
            pass


def _signal_process_group(process: Any, sig: int) -> bool:
    killpg = getattr(os, "killpg", None)
    pid = getattr(process, "pid", None)
    if killpg is None or pid is None:
        return False
    try:
        killpg(pid, sig)
    except ProcessLookupError:
        return True
    except OSError:
        return False
    return True


def _config_text(
    *,
    server_name: str,
    host: str,
    port: int,
    ports_dir: Path,
    store_dir: Path,
    auth_token: str | None,
) -> str:
    lines = [
        f"server_name: {json.dumps(server_name)}",
        f"host: {json.dumps(host)}",
        f"port: {port}",
        f"ports_file_dir: {json.dumps(str(ports_dir))}",
        "jetstream {",
        f"  store_dir: {json.dumps(str(store_dir))}",
        "}",
    ]
    if auth_token is not None:
        lines.extend(
            (
                "authorization {",
                f"  token: {json.dumps(auth_token)}",
                "}",
            )
        )
    lines.append("")
    return "\n".join(lines)


def _nats_url_from_ports_file(path: Path) -> str:
    payload = json.loads(path.read_text())
    urls = payload.get("nats")
    if not isinstance(urls, list) or not urls:
        raise ValueError(f"NATS ports file {path} does not contain a nats URL")
    url = urls[0]
    if not isinstance(url, str) or not url.startswith(("nats://", "tls://")):
        raise ValueError(f"NATS ports file {path} contains an invalid nats URL")
    return url


async def _verify_jetstream(url: str, *, auth_token: str | None) -> None:
    try:
        import nats
    except ModuleNotFoundError as exc:
        raise RuntimeError("Supervised NATS requires deckr[nats].") from exc

    connect_options: dict[str, Any] = {
        "allow_reconnect": False,
        "connect_timeout": 1,
        "max_reconnect_attempts": 0,
    }
    if auth_token is not None:
        connect_options["token"] = auth_token
    nc = await nats.connect(url, **connect_options)
    try:
        await nc.jetstream().account_info()
    finally:
        await nc.close()


def _failure_message(message: str, *, logs: tuple[str, ...]) -> str:
    if not logs:
        return message
    recent = "\n".join(f"  {line}" for line in logs[-20:])
    return f"{message}\nRecent nats-server output:\n{recent}"
