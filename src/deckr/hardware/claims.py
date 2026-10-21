"""Claims."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, Protocol

import anyio

from deckr.concord import ContractHandle
from deckr.contracts.messages import DeckrMessage, EndpointAddress, EntitySubject
from deckr.hardware.command_context import HardwareCommandContext
from deckr.hardware.descriptors import DeviceRef
from deckr.hardware.profiles import HardwareClaimTerms

logger = logging.getLogger(__name__)


_HardwareCommandHandler = Callable[[HardwareCommandContext], Awaitable[bool | None]]


_HardwareResetHandler = Callable[[str], Awaitable[None]]


class _HardwareEndpoint(Protocol):
    address: EndpointAddress
    session_id: str

    async def send(
        self,
        *,
        lane: str,
        recipient: str | EndpointAddress,
        recipient_session_id: str | None = None,
        subject: EntitySubject,
        message_type: str,
        body: Mapping[str, Any],
        causation_id: str | None = None,
        contract: Mapping[str, Any] | None = None,
    ) -> DeckrMessage: ...

    async def reply_to(
        self,
        request: DeckrMessage,
        *,
        message_type: str,
        body: Mapping[str, Any],
        subject: EntitySubject | None = None,
        causation_id: str | None = None,
    ) -> DeckrMessage: ...

    def subscribe(
        self,
        lane: str,
    ) -> AbstractAsyncContextManager[anyio.abc.ObjectReceiveStream[DeckrMessage]]: ...


@dataclass(frozen=True, slots=True)
class LiveHardwareClaim:
    contract: ContractHandle
    terms: HardwareClaimTerms
    controller_endpoint: EndpointAddress
    controller_session_id: str
    device_refs: tuple[DeviceRef, ...]

    @property
    def device_ids(self) -> tuple[str, ...]:
        return tuple(ref.device_id for ref in self.device_refs)


@dataclass(slots=True)
class _ClaimCandidate:
    contract: ContractHandle
    terms: HardwareClaimTerms
    local_authority: bool
    valid: bool = False
    controller_session_id: str | None = None

    @property
    def device_ids(self) -> tuple[str, ...]:
        return tuple(device.device_ref.device_id for device in self.terms.devices)
