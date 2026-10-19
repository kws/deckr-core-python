"""Errors."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from deckr.concord import ConcordConflict, ConcordConflictCode, ContractValidityStatus
from deckr.services.messages import ServiceMessageBody

_TERMINAL_DURING_NEGOTIATION = frozenset(
    {
        ContractValidityStatus.CANCELLED,
        ContractValidityStatus.MISSING_CONTRACT,
        ContractValidityStatus.INVALID_CONTRACT,
        ContractValidityStatus.INVALID_TOKEN,
        ContractValidityStatus.MISSING_TOKEN,
        ContractValidityStatus.GENERATION_MISMATCH,
        ContractValidityStatus.SESSION_MISMATCH,
        ContractValidityStatus.TERMS_HASH_MISMATCH,
    }
)


_SERVICE_USE_LOSS_STATUSES = frozenset(
    {
        *{status.value for status in _TERMINAL_DURING_NEGOTIATION},
    }
)


_SERVICE_USE_LOSS_CODES = frozenset(
    {
        "contract_not_managed",
        "contract_local_authority_lost",
        "contract_unavailable",
        "service_use_closed",
        "service_use_conflict",
        *{f"contract_{status}" for status in _SERVICE_USE_LOSS_STATUSES},
    }
)


_SERVICE_USE_REPLY_ERROR_CODES = frozenset(
    {
        "service_use_contract_invalid",
    }
)


class ServiceUnavailable(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.diagnostics = dict(diagnostics or {})


class UnsupportedServiceScope(ValueError):
    pass


class ServiceUseAuthorizationError(ValueError):
    def __init__(
        self,
        code: str,
        message: str,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.diagnostics = dict(diagnostics or {})


def terminal_concord_conflict_status(
    exc: ConcordConflict,
) -> ContractValidityStatus | None:
    """Classify terminal Concord conflicts for service infrastructure."""

    return {
        ConcordConflictCode.CONTRACT_CANCELLED: ContractValidityStatus.CANCELLED,
        ConcordConflictCode.CONTRACT_MISSING: ContractValidityStatus.MISSING_CONTRACT,
        ConcordConflictCode.CONTRACT_INVALID: ContractValidityStatus.INVALID_CONTRACT,
        ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH: (
            ContractValidityStatus.INVALID_CONTRACT
        ),
        ConcordConflictCode.TOKEN_MISSING: ContractValidityStatus.MISSING_TOKEN,
        ConcordConflictCode.TOKEN_INVALID: ContractValidityStatus.INVALID_TOKEN,
        ConcordConflictCode.TOKEN_IDENTITY_MISMATCH: (
            ContractValidityStatus.INVALID_TOKEN
        ),
    }.get(exc.code)


def service_unavailable_ends_service_use(exc: ServiceUnavailable) -> bool:
    """Return whether a service-domain exception means the current lease is lost."""

    if exc.code in _SERVICE_USE_LOSS_CODES:
        return True
    return _service_use_diagnostics_end_service_use(dict(exc.diagnostics))


def service_message_ends_service_use(message: ServiceMessageBody) -> bool:
    """Return whether a service response reports ended service-use authority."""

    error = getattr(message, "error", None)
    if error is None:
        return False
    if error.code in _SERVICE_USE_LOSS_CODES:
        return True
    if error.code not in _SERVICE_USE_REPLY_ERROR_CODES:
        return False
    return _service_use_diagnostics_end_service_use(
        dict(getattr(error, "diagnostics", None) or {})
    )


def service_use_negotiation_terminal_status(
    status: ContractValidityStatus,
) -> bool:
    return status in _TERMINAL_DURING_NEGOTIATION


def _service_use_diagnostics_end_service_use(diagnostics: Mapping[str, Any]) -> bool:
    status = diagnostics.get("status")
    reason = diagnostics.get("reason")
    return status in _SERVICE_USE_LOSS_STATUSES or reason in (
        _SERVICE_USE_LOSS_CODES | _SERVICE_USE_LOSS_STATUSES
    )
