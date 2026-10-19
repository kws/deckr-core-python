"""Subscription state."""

from __future__ import annotations

import logging
from collections.abc import Hashable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Generic, TypeVar

from deckr.services.messages import ServiceError, ServiceMessageBody
from deckr.services.runtime import ServiceUnavailable

logger = logging.getLogger(__name__)


ResourceT = TypeVar("ResourceT", bound=Hashable)


_RECONNECT_DELAY_SECONDS = 0.05


class ServiceSubscriptionState(StrEnum):
    """Small state vocabulary for managed service subscriptions."""

    PENDING = "pending"
    READY = "ready"
    UNAVAILABLE = "unavailable"
    RECONNECTING = "reconnecting"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class ServiceSubscriptionMessage(Generic[ResourceT]):
    """One resource-state update from a managed service subscription."""

    resource: ResourceT
    state: ServiceSubscriptionState
    payload: Mapping[str, Any] | None = None
    error: ServiceError | None = None
    diagnostics: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.payload is not None:
            object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))
        object.__setattr__(
            self,
            "diagnostics",
            MappingProxyType(dict(self.diagnostics)),
        )


def _state_message(
    resource: ResourceT,
    state: ServiceSubscriptionState,
    *,
    error: ServiceError | None = None,
    diagnostics: Mapping[str, Any] | None = None,
) -> ServiceSubscriptionMessage[ResourceT]:
    return ServiceSubscriptionMessage(
        resource=resource,
        state=state,
        error=error,
        diagnostics=dict(diagnostics or {}),
    )


def _service_error_from_unavailable(exc: ServiceUnavailable) -> ServiceError:
    return ServiceError(
        code=exc.code,
        message=exc.message,
        diagnostics=dict(exc.diagnostics),
    )


def _service_unavailable_from_reply(reply: ServiceMessageBody) -> ServiceUnavailable:
    error = reply.error
    if error is None:
        return ServiceUnavailable(
            "service_use_contract_invalid",
            "Service-use contract ended",
        )
    return ServiceUnavailable(error.code, error.message, dict(error.diagnostics))
