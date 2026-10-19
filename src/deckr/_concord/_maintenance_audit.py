"""Maintenance audit."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from deckr._concord._maintenance_models import logger
from deckr._concord._maintenance_validation import ConcordTokenMaintenanceEntry
from deckr._concord._models import ContractRecord, ContractValidityStatus
from deckr.core.identity import EndpointAddress


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _datetime_log_value(value: datetime | None) -> str | None:
    if value is None:
        return None
    return _ensure_utc(value).isoformat().replace("+00:00", "Z")


def _datetime_from_raw_value(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if not isinstance(value, str):
        return None
    source = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(source)
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _actor_log_value(value: EndpointAddress | str | None) -> str | None:
    if value is None:
        return None
    return str(value)


def _token_log_summary(
    token_entries: tuple[ConcordTokenMaintenanceEntry, ...],
) -> tuple[list[str], dict[str, str], dict[str, int]]:
    participants: list[str] = []
    sessions: dict[str, str] = {}
    refresh_sequences: dict[str, int] = {}
    for _entry, token in token_entries:
        participant = str(token.participant)
        participants.append(participant)
        sessions[participant] = token.session_id
        refresh_sequences[participant] = token.refresh_seq
    return participants, sessions, refresh_sequences


def _log_concord_contract_deletion_audit(
    *,
    log_label: str,
    contract_key: str,
    record: ContractRecord,
    state_revision: int,
    validation_status: ContractValidityStatus,
    token_entries: tuple[ConcordTokenMaintenanceEntry, ...],
    listed_token_key_count: int,
) -> None:
    token_participants, token_sessions, token_refresh_sequences = _token_log_summary(
        token_entries
    )
    logger.info(
        "%s Concord maintenance deleting cancelled contract contract_key=%s "
        "contract=%s generation=%s profile=%s state=%s participants=%s "
        "attached_participants=%s created_by=%s created_at=%s "
        "cancelled_by=%s cancelled_at=%s cancel_reason=%s cancel_revision=%s "
        "supersedes=%s terms_hash=%s validation_status=%s "
        "state_revision=%s token_participants=%s token_sessions=%s "
        "token_refresh_sequences=%s listed_token_key_count=%s",
        log_label,
        contract_key,
        record.contract_id,
        record.generation,
        record.profile,
        record.state.value,
        [str(item) for item in record.participants],
        [str(item) for item in record.attached_participants],
        _actor_log_value(record.created_by),
        _datetime_log_value(record.created_at),
        _actor_log_value(record.cancelled_by),
        _datetime_log_value(record.cancelled_at),
        record.cancel_reason,
        record.cancel_revision,
        (
            record.supersedes.model_dump(by_alias=True, mode="json")
            if record.supersedes is not None
            else None
        ),
        record.terms_hash,
        validation_status.value,
        state_revision,
        token_participants,
        token_sessions,
        token_refresh_sequences,
        listed_token_key_count,
    )
