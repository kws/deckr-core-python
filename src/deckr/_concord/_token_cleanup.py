"""Token cleanup."""

from __future__ import annotations

from deckr._concord._keys import (
    concord_contract_prefix,
    concord_participant_token_key,
    parse_concord_participant_token_key,
)
from deckr._concord._maintenance_models import (
    _MAX_TOKEN_CLEANUP_SCAN_ATTEMPTS,
    _ConcordTokenCleanupAttempt,
    logger,
)
from deckr._concord._maintenance_validation import ConcordTokenMaintenanceEntry
from deckr._concord._models import (
    ConcordConflict,
    ConcordConflictCode,
    ConcordUnavailable,
    ParticipantTokenRecord,
)
from deckr._concord._ports import ConcordMaintenanceScanPort
from deckr.core.identity import ContractPointer


async def _concord_participant_token_entries(
    token_store: ConcordMaintenanceScanPort,
    *,
    contract_id: str,
    generation: int,
    terms_hash: str | None,
) -> tuple[ConcordTokenMaintenanceEntry, ...]:
    token_entries: list[ConcordTokenMaintenanceEntry] = []
    pointer = ContractPointer(contractId=contract_id, generation=generation)
    requested_prefix = concord_contract_prefix(
        contract_id=contract_id,
        generation=generation,
    )
    for entry in await token_store.items_exact(requested_prefix):
        parsed = parse_concord_participant_token_key(entry.key)
        if parsed is None or not entry.key.startswith(requested_prefix):
            raise ConcordConflict(
                ConcordConflictCode.TOKEN_IDENTITY_MISMATCH,
                "Concord participant-token scan returned a key outside the "
                "requested contract generation",
                key=entry.key,
                expected_pointer=pointer,
            )
        parsed_contract_id, parsed_generation, participant = parsed
        canonical_key = concord_participant_token_key(
            contract_id=parsed_contract_id,
            generation=parsed_generation,
            participant=participant,
        )
        if (
            parsed_contract_id != contract_id
            or parsed_generation != generation
            or entry.key != canonical_key
        ):
            raise ConcordConflict(
                ConcordConflictCode.TOKEN_IDENTITY_MISMATCH,
                "Concord participant-token scan key differs from the requested "
                "contract generation",
                key=entry.key,
                expected_pointer=pointer,
            )
        try:
            token = ParticipantTokenRecord.from_wire(entry.value)
        except (TypeError, ValueError) as exc:
            raise ConcordConflict(
                ConcordConflictCode.TOKEN_INVALID,
                f"Concord participant token {entry.key!r} is malformed: {exc}",
                key=entry.key,
                expected_pointer=pointer,
            ) from exc
        if (
            token.contract_id != contract_id
            or token.generation != generation
            or token.participant != participant
            or token.terms_hash != terms_hash
        ):
            raise ConcordConflict(
                ConcordConflictCode.TOKEN_IDENTITY_MISMATCH,
                "Concord participant token key and record identity differ",
                key=entry.key,
                expected_pointer=pointer,
            )
        token_entries.append((entry, token))
    return tuple(sorted(token_entries, key=lambda item: item[0].key))


async def _delete_concord_participant_tokens_until_empty(
    token_store: ConcordMaintenanceScanPort,
    *,
    contract_id: str,
    generation: int,
    terms_hash: str | None,
    initial_entries: tuple[ConcordTokenMaintenanceEntry, ...] | None,
    log_label: str,
) -> _ConcordTokenCleanupAttempt:
    entries = initial_entries
    deleted = 0
    if not entries:
        try:
            entries = await _concord_participant_token_entries(
                token_store,
                contract_id=contract_id,
                generation=generation,
                terms_hash=terms_hash,
            )
        except (ConcordConflict, ConcordUnavailable) as exc:
            return _ConcordTokenCleanupAttempt(
                completed=False,
                failure=exc,
            )
        if not entries:
            return _ConcordTokenCleanupAttempt(completed=True)
    for _attempt in range(_MAX_TOKEN_CLEANUP_SCAN_ATTEMPTS):
        for entry, _token in entries:
            try:
                marker_revision = await token_store.delete(
                    entry.key,
                    revision=entry.revision,
                )
            except ConcordConflict:
                continue
            except ConcordUnavailable as exc:
                return _ConcordTokenCleanupAttempt(
                    completed=False,
                    deleted_token_key_count=deleted,
                    failure=exc,
                )
            if marker_revision is not None:
                deleted += 1
        try:
            entries = await _concord_participant_token_entries(
                token_store,
                contract_id=contract_id,
                generation=generation,
                terms_hash=terms_hash,
            )
        except (ConcordConflict, ConcordUnavailable) as exc:
            return _ConcordTokenCleanupAttempt(
                completed=False,
                deleted_token_key_count=deleted,
                failure=exc,
            )
        if not entries:
            return _ConcordTokenCleanupAttempt(
                completed=True,
                deleted_token_key_count=deleted,
            )
    logger.warning(
        "%s Concord maintenance token cleanup did not converge contract=%s "
        "generation=%s remaining_keys=%s attempts=%s",
        log_label,
        contract_id,
        generation,
        [entry.key for entry, _token in entries],
        _MAX_TOKEN_CLEANUP_SCAN_ATTEMPTS,
    )
    return _ConcordTokenCleanupAttempt(
        completed=False,
        deleted_token_key_count=deleted,
    )
