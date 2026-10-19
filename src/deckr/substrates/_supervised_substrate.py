"""Supervised substrate."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from pathlib import Path

import anyio

from deckr.bindings.nats import (
    DEFAULT_NATS_RECONNECT_MAX_INTERVAL_SECONDS,
    DeckrNatsSubstrate as NatsSubstrate,
    _reconnect_interval,
)
from deckr.contracts.lanes import MessageContract, MessageContractRegistry
from deckr.contracts.messages import DeckrMessage, EndpointAddress
from deckr.core.kv import KvBucketPolicy
from deckr.lanes import ReplyPredicate
from deckr.substrates.nats_binary import (
    _DEFAULT_HOST,
    _DEFAULT_MINIMUM_VERSION,
    _DEFAULT_PORT,
)
from deckr.substrates.nats_kv import NatsJsonKvBucket
from deckr.substrates.nats_server import NatsServerSupervisor


class SupervisedNatsSubstrate:
    def __init__(
        self,
        *,
        lane_contracts: MessageContractRegistry,
        supervisor: NatsServerSupervisor | None = None,
        server_path: Path | str | None = None,
        minimum_server_version: str = _DEFAULT_MINIMUM_VERSION,
        host: str = _DEFAULT_HOST,
        port: int = _DEFAULT_PORT,
        runtime_dir: Path | str | None = None,
        store_dir: Path | str | None = None,
        auth_enabled: bool = True,
        auth_token: str | None = None,
        startup_timeout: float = 10.0,
        shutdown_timeout: float = 5.0,
        log_buffer_lines: int = 200,
        buffer_size: int = 100,
        reconnect_max_interval_seconds: float = DEFAULT_NATS_RECONNECT_MAX_INTERVAL_SECONDS,
    ) -> None:
        self.reconnect_max_interval_seconds = _reconnect_interval(
            reconnect_max_interval_seconds
        )
        self.supervisor = supervisor or NatsServerSupervisor(
            server_path=server_path,
            minimum_version=minimum_server_version,
            host=host,
            port=port,
            runtime_dir=runtime_dir,
            store_dir=store_dir,
            auth_enabled=auth_enabled,
            auth_token=auth_token,
            startup_timeout=startup_timeout,
            shutdown_timeout=shutdown_timeout,
            log_buffer_lines=log_buffer_lines,
        )
        self._lane_contracts = lane_contracts
        self._buffer_size = buffer_size
        self._nats: NatsSubstrate | None = None

    @property
    def url(self) -> str | None:
        if self._nats is not None:
            return self._nats.url
        return self.supervisor.url

    async def connect(self) -> None:
        if self._nats is not None:
            raise RuntimeError("Supervised NATS substrate is already connected")
        handle = await self.supervisor.start()
        self._nats = NatsSubstrate(
            url=handle.url,
            auth_token=handle.auth_token,
            lane_contracts=self._lane_contracts,
            buffer_size=self._buffer_size,
            reconnect_max_interval_seconds=self.reconnect_max_interval_seconds,
        )
        try:
            await self._nats.connect()
        except BaseException:
            await self.supervisor.stop()
            self._nats = None
            raise

    def start(self, tg: anyio.abc.TaskGroup) -> None:
        self.supervisor.start_monitor(tg)

    async def aclose(self) -> None:
        with anyio.CancelScope(shield=True):
            try:
                if self._nats is not None:
                    await self._nats.aclose()
            finally:
                self._nats = None
                await self.supervisor.stop()

    def contract_for(self, lane: str) -> MessageContract:
        return self._lane_contracts.contract_for(lane)

    async def publish(self, message: DeckrMessage) -> None:
        await self._connected_nats().publish(message)

    async def publish_reply(
        self,
        message: DeckrMessage,
        *,
        request: DeckrMessage,
    ) -> None:
        await self._connected_nats().publish_reply(message, request=request)

    async def request(
        self,
        message: DeckrMessage,
        *,
        timeout: float = 2.0,
        accept: ReplyPredicate | None = None,
    ) -> DeckrMessage:
        return await self._connected_nats().request(
            message,
            timeout=timeout,
            accept=accept,
        )

    def subscribe(
        self,
        lane: str,
        endpoint: EndpointAddress,
        *,
        endpoint_session_id: str,
    ) -> AbstractAsyncContextManager[anyio.abc.ObjectReceiveStream[DeckrMessage]]:
        return self._connected_nats().subscribe(
            lane,
            endpoint,
            endpoint_session_id=endpoint_session_id,
        )

    def kv_bucket(self, policy: KvBucketPolicy) -> NatsJsonKvBucket:
        return self._connected_nats().kv_bucket(policy)

    def _connected_nats(self) -> NatsSubstrate:
        if self._nats is None:
            raise RuntimeError("Supervised NATS substrate is not connected")
        return self._nats
