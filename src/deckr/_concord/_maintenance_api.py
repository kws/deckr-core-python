"""Exact Concord validation, terminal cleanup, and persistent stale observations."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime

from deckr._concord._adapters import concord_maintenance_scan_store
from deckr._concord._keys import (
    concord_contract_key,
    concord_contracts_prefix,
    concord_participant_token_key,
    concord_stale_observation_key,
    concord_token_cleanup_key,
    parse_concord_stale_observation_key,
    parse_concord_token_cleanup_key,
)
from deckr._concord._maintenance_audit import (
    _ensure_utc,
    _log_concord_contract_deletion_audit,
)
from deckr._concord._maintenance_models import (
    _MAX_OBSERVATION_CAS_ATTEMPTS,
    CONCORD_MAINTENANCE_ACTOR,
    CONCORD_REAPER_STALE_CONTRACT_REASON,
    DEFAULT_CONCORD_REAPER_CANCELLED_RETENTION_SECONDS,
    ConcordMaintenanceDeletionResult,
    ConcordStaleObservationRecord,
    ConcordTokenCleanupRecord,
    _ConcordTokenCleanupAttempt,
    _now_utc,
    logger,
)
from deckr._concord._maintenance_validation import (
    ConcordTokenMaintenanceEntry,
    _assert_contract_handle,
    _assert_contract_replacement_identity,
    _parse_contract_entry,
    _parse_stale_observation_entry,
    _parse_token_cleanup_entry,
)
from deckr._concord._models import (
    ConcordConflict,
    ConcordConflictCode,
    ConcordUnavailable,
    ContractHandle,
    ContractState,
    ContractValidity,
    ContractValidityStatus,
)
from deckr._concord._ports import (
    ConcordMaintenanceScanPort,
    ConcordRawMaintenanceStorePort,
)
from deckr._concord._token_cleanup import (
    _concord_participant_token_entries,
    _delete_concord_participant_tokens_until_empty,
)
from deckr._concord._validation import (
    ConcordObservationState,
    ConcordSessionAssertions,
    ContractObservation,
    ParticipantTokenObservation,
    contract_observation_from_entry,
    evaluate_contract_validity,
    token_observation_from_entry,
)
from deckr.core.identity import ContractPointer, EndpointAddress
from deckr.core.kv import KvEntry


class ConcordMaintenance:
    """Exact/scanning Concord maintenance authority with no runtime watch."""

    def __init__(
        self,
        contract_store: ConcordRawMaintenanceStorePort | ConcordMaintenanceScanPort,
        token_store: ConcordRawMaintenanceStorePort | ConcordMaintenanceScanPort,
        maintenance_store: ConcordRawMaintenanceStorePort | ConcordMaintenanceScanPort,
    ) -> None:
        self._contract_store = concord_maintenance_scan_store(contract_store)
        self._token_store = concord_maintenance_scan_store(token_store)
        self._maintenance_store = concord_maintenance_scan_store(maintenance_store)

    @property
    def contract_bucket(self) -> str:
        return self._contract_store.bucket

    @property
    def token_bucket(self) -> str:
        return self._token_store.bucket

    @property
    def maintenance_bucket(self) -> str:
        return self._maintenance_store.bucket

    async def validate_exact(
        self,
        contract: ContractHandle,
        *,
        current_sessions: (
            Mapping[str | EndpointAddress, str] | ConcordSessionAssertions | None
        ) = None,
    ) -> ContractValidity:
        """Validate one known contract pointer using exact store reads only."""

        assertions = (
            current_sessions
            if isinstance(current_sessions, ConcordSessionAssertions)
            else ConcordSessionAssertions(current_sessions)
        )
        pointer = ContractPointer(
            contractId=contract.contract_id,
            generation=contract.generation,
        )
        key = concord_contract_key(
            contract_id=pointer.contract_id,
            generation=pointer.generation,
        )
        try:
            entry = await self._contract_store.get_exact(key)
        except ConcordUnavailable as exc:
            contract_observation = ContractObservation(
                key,
                None,
                ConcordObservationState.UNAVAILABLE,
                diagnostic=str(exc),
            )
            token_observations: tuple[ParticipantTokenObservation, ...] = ()
        else:
            contract_observation = contract_observation_from_entry(key, entry)
            participants = (
                contract_observation.record.participants
                if contract_observation.record is not None
                else contract.participants
            )
            token_observations = await self._exact_token_observations(
                pointer,
                participants,
            )
        return evaluate_contract_validity(
            expected_key=contract.key,
            expected_pointer=pointer,
            contract=contract_observation,
            tokens=token_observations,
            session_assertions=assertions,
        )

    async def cancel_contract(
        self,
        contract: ContractHandle,
        *,
        reason: str = CONCORD_REAPER_STALE_CONTRACT_REASON,
        log_label: str = "Concord",
        now: datetime | None = None,
    ) -> bool:
        """Terminally cancel an open contract as the maintenance actor."""

        pointer = _assert_contract_handle(contract)
        current = await self._contract_store.get_exact(contract.key)
        if current is None:
            return False
        record = _parse_contract_entry(current, contract.key, pointer)
        if record.state == ContractState.CANCELLED:
            return False
        cancelled = record.model_copy(
            update={
                "state": ContractState.CANCELLED,
                "cancelled_by": CONCORD_MAINTENANCE_ACTOR,
                "cancelled_at": _ensure_utc(now or _now_utc()),
                "cancel_revision": current.revision,
                "cancel_reason": reason,
            }
        )
        _assert_contract_replacement_identity(record, cancelled, pointer, contract.key)
        await self._contract_store.update(
            contract.key,
            cancelled,
            revision=current.revision,
        )
        logger.info(
            "%s Concord maintenance cancelled contract profile=%s contract=%s "
            "generation=%s cancelled_by=%s reason=%s revision=%s",
            log_label,
            record.profile,
            record.contract_id,
            record.generation,
            CONCORD_MAINTENANCE_ACTOR,
            reason,
            current.revision,
        )
        return True

    async def delete_cancelled_contract(
        self,
        contract: ContractHandle,
        *,
        retention_seconds: float = float(
            DEFAULT_CONCORD_REAPER_CANCELLED_RETENTION_SECONDS
        ),
        log_label: str = "Concord",
        now: datetime | None = None,
    ) -> ConcordMaintenanceDeletionResult:
        """Delete an eligible retained contract and durably finish token cleanup."""

        pointer = _assert_contract_handle(contract)
        observed = await self._contract_store.get_exact(contract.key)
        if observed is None:
            return ConcordMaintenanceDeletionResult()
        record = _parse_contract_entry(observed, contract.key, pointer)
        if record.state != ContractState.CANCELLED:
            return ConcordMaintenanceDeletionResult()
        current_time = _ensure_utc(now or _now_utc())
        if (
            record.cancelled_at is None
            or (current_time - record.cancelled_at.astimezone(UTC)).total_seconds()
            < retention_seconds
        ):
            return ConcordMaintenanceDeletionResult()

        validity = await self.validate_exact(contract)
        token_entries = await _concord_participant_token_entries(
            self._token_store,
            contract_id=contract.contract_id,
            generation=contract.generation,
            terms_hash=record.terms_hash,
        )
        _log_concord_contract_deletion_audit(
            log_label=log_label,
            contract_key=contract.key,
            record=record,
            state_revision=observed.revision,
            validation_status=validity.status,
            token_entries=token_entries,
            listed_token_key_count=len(token_entries),
        )
        cleanup_marker = await self._ensure_token_cleanup_marker(
            pointer=pointer,
            contract_revision=observed.revision,
            terms_hash=record.terms_hash,
            now=current_time,
        )
        try:
            await self._contract_store.delete(
                contract.key,
                revision=observed.revision,
            )
        except ConcordConflict:
            logger.info(
                "%s Concord maintenance delete conflict; leaving contract "
                "for next scan contract_key=%s contract=%s generation=%s "
                "revision=%s",
                log_label,
                contract.key,
                contract.contract_id,
                contract.generation,
                observed.revision,
                exc_info=True,
            )
            return ConcordMaintenanceDeletionResult()

        try:
            cleanup = await self._complete_token_cleanup(
                cleanup_marker,
                initial_entries=token_entries,
                log_label=log_label,
            )
        except (ConcordConflict, ConcordUnavailable):
            logger.warning(
                "%s Concord maintenance token cleanup remains pending "
                "contract=%s generation=%s marker=%s",
                log_label,
                contract.contract_id,
                contract.generation,
                cleanup_marker.key,
                exc_info=True,
            )
            return ConcordMaintenanceDeletionResult(
                contract_deleted=True,
                cleanup_pending=True,
            )
        return ConcordMaintenanceDeletionResult(
            contract_deleted=True,
            cleanup_completed=cleanup.completed,
            cleanup_pending=not cleanup.completed,
            deleted_token_key_count=cleanup.deleted_token_key_count,
        )

    async def _ensure_token_cleanup_marker(
        self,
        *,
        pointer: ContractPointer,
        contract_revision: int,
        terms_hash: str | None,
        now: datetime,
    ) -> KvEntry:
        key = concord_token_cleanup_key(
            contract_id=pointer.contract_id,
            generation=pointer.generation,
        )
        candidate = ConcordTokenCleanupRecord(
            contractId=pointer.contract_id,
            generation=pointer.generation,
            contractRevision=contract_revision,
            termsHash=terms_hash,
            cleanupStartedAt=now,
        )
        last_conflict: ConcordConflict | None = None
        for _attempt in range(_MAX_OBSERVATION_CAS_ATTEMPTS):
            current = await self._maintenance_store.get_exact(key)
            if current is None:
                try:
                    return await self._maintenance_store.create(key, candidate)
                except ConcordConflict as exc:
                    last_conflict = exc
                    continue
            record = _parse_token_cleanup_entry(
                current,
                key=key,
                contract_id=pointer.contract_id,
                generation=pointer.generation,
            )
            if (
                record.contract_revision == contract_revision
                and record.terms_hash == terms_hash
            ):
                return current
            try:
                return await self._maintenance_store.update(
                    key,
                    candidate,
                    revision=current.revision,
                )
            except ConcordConflict as exc:
                last_conflict = exc
                continue
        raise ConcordConflict(
            ConcordConflictCode.REVISION_CHANGED,
            "Concord token-cleanup marker revision kept changing",
            key=key,
            expected_pointer=pointer,
        ) from last_conflict

    async def _complete_token_cleanup(
        self,
        marker_entry: KvEntry,
        *,
        initial_entries: tuple[ConcordTokenMaintenanceEntry, ...] | None = None,
        log_label: str,
    ) -> _ConcordTokenCleanupAttempt:
        parsed = parse_concord_token_cleanup_key(marker_entry.key)
        if parsed is None:
            raise ConcordConflict(
                ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
                "Concord token-cleanup marker has a noncanonical key",
                key=marker_entry.key,
            )
        contract_id, generation = parsed
        marker = _parse_token_cleanup_entry(
            marker_entry,
            key=marker_entry.key,
            contract_id=contract_id,
            generation=generation,
        )
        cleanup = await _delete_concord_participant_tokens_until_empty(
            self._token_store,
            contract_id=contract_id,
            generation=generation,
            terms_hash=marker.terms_hash,
            initial_entries=initial_entries,
            log_label=log_label,
        )
        if not cleanup.completed:
            if cleanup.failure is not None:
                logger.warning(
                    "%s Concord maintenance token cleanup remains pending "
                    "contract=%s generation=%s deleted_token_keys=%s reason=%s",
                    log_label,
                    contract_id,
                    generation,
                    cleanup.deleted_token_key_count,
                    cleanup.failure,
                )
            return cleanup

        try:
            current = await self._maintenance_store.get_exact(marker_entry.key)
        except (ConcordConflict, ConcordUnavailable) as exc:
            return _ConcordTokenCleanupAttempt(
                completed=False,
                deleted_token_key_count=cleanup.deleted_token_key_count,
                failure=exc,
            )
        if current is None:
            return cleanup
        try:
            current_marker = _parse_token_cleanup_entry(
                current,
                key=marker_entry.key,
                contract_id=contract_id,
                generation=generation,
            )
        except ConcordConflict as exc:
            return _ConcordTokenCleanupAttempt(
                completed=False,
                deleted_token_key_count=cleanup.deleted_token_key_count,
                failure=exc,
            )
        if (
            current_marker.contract_revision != marker.contract_revision
            or current_marker.terms_hash != marker.terms_hash
        ):
            return _ConcordTokenCleanupAttempt(
                completed=False,
                deleted_token_key_count=cleanup.deleted_token_key_count,
            )
        try:
            await self._maintenance_store.delete(
                marker_entry.key,
                revision=current.revision,
            )
        except ConcordConflict as exc:
            try:
                marker_still_exists = (
                    await self._maintenance_store.get_exact(marker_entry.key)
                    is not None
                )
            except (ConcordConflict, ConcordUnavailable) as read_exc:
                return _ConcordTokenCleanupAttempt(
                    completed=False,
                    deleted_token_key_count=cleanup.deleted_token_key_count,
                    failure=read_exc,
                )
            if marker_still_exists:
                return _ConcordTokenCleanupAttempt(
                    completed=False,
                    deleted_token_key_count=cleanup.deleted_token_key_count,
                    failure=exc,
                )
        except ConcordUnavailable as exc:
            return _ConcordTokenCleanupAttempt(
                completed=False,
                deleted_token_key_count=cleanup.deleted_token_key_count,
                failure=exc,
            )
        return cleanup

    async def _exact_token_observations(
        self,
        pointer: ContractPointer,
        participants: tuple[EndpointAddress, ...],
    ) -> tuple[ParticipantTokenObservation, ...]:
        observations: list[ParticipantTokenObservation] = []
        for participant in sorted(participants, key=str):
            key = concord_participant_token_key(
                contract_id=pointer.contract_id,
                generation=pointer.generation,
                participant=participant,
            )
            try:
                entry = await self._token_store.get_exact(key)
            except ConcordUnavailable as exc:
                observations.append(
                    ParticipantTokenObservation(
                        key,
                        None,
                        participant,
                        ConcordObservationState.UNAVAILABLE,
                        diagnostic=str(exc),
                    )
                )
            else:
                observations.append(
                    token_observation_from_entry(key, participant, entry)
                )
        return tuple(observations)

    async def _contract_entries(self) -> tuple[KvEntry, ...]:
        return await self._contract_store.items_exact(concord_contracts_prefix())

    async def _stale_observation_entries(self) -> tuple[KvEntry, ...]:
        return await self._maintenance_store.items_exact("stale.")

    async def _token_cleanup_entries(self) -> tuple[KvEntry, ...]:
        return await self._maintenance_store.items_exact("cleanup.")

    async def _observe_stale(
        self,
        *,
        contract_id: str,
        generation: int,
        status: ContractValidityStatus,
        reason: str | None,
        contract_revision: int | None,
        now: datetime,
    ) -> tuple[datetime, bool]:
        key = concord_stale_observation_key(
            contract_id=contract_id,
            generation=generation,
        )
        candidate_time = _ensure_utc(now)
        candidate = ConcordStaleObservationRecord(
            contractId=contract_id,
            generation=generation,
            firstObservedStaleAt=candidate_time,
            status=status,
            reason=reason,
            contractRevision=contract_revision,
        )
        last_conflict: ConcordConflict | None = None
        for _attempt in range(_MAX_OBSERVATION_CAS_ATTEMPTS):
            current = await self._maintenance_store.get_exact(key)
            if current is None:
                try:
                    await self._maintenance_store.create(key, candidate)
                except ConcordConflict as exc:
                    last_conflict = exc
                    continue
                return candidate_time, True

            record = _parse_stale_observation_entry(
                current,
                key=key,
                contract_id=contract_id,
                generation=generation,
            )
            if record.first_observed_stale_at <= candidate_time:
                return record.first_observed_stale_at, False
            try:
                await self._maintenance_store.update(
                    key,
                    candidate,
                    revision=current.revision,
                )
            except ConcordConflict as exc:
                last_conflict = exc
                continue
            return candidate_time, False

        raise ConcordConflict(
            ConcordConflictCode.REVISION_CHANGED,
            "Concord stale observation revision kept changing",
            key=key,
            expected_pointer=ContractPointer(
                contractId=contract_id,
                generation=generation,
            ),
        ) from last_conflict

    async def _clear_stale_observation(
        self,
        contract_id: str,
        generation: int,
    ) -> bool:
        key = concord_stale_observation_key(
            contract_id=contract_id,
            generation=generation,
        )
        current = await self._maintenance_store.get_exact(key)
        if current is None:
            return False
        _parse_stale_observation_entry(
            current,
            key=key,
            contract_id=contract_id,
            generation=generation,
        )
        try:
            await self._maintenance_store.delete(key, revision=current.revision)
        except ConcordConflict:
            return False
        return True

    async def _clear_orphaned_stale_observations(self) -> int:
        cleared = 0
        for entry in await self._stale_observation_entries():
            try:
                parsed = parse_concord_stale_observation_key(entry.key)
                if parsed is None:
                    continue
                observation = _parse_stale_observation_entry(
                    entry,
                    key=entry.key,
                    contract_id=parsed[0],
                    generation=parsed[1],
                )
            except ConcordConflict:
                continue
            contract_entry = await self._contract_store.get_exact(
                concord_contract_key(
                    contract_id=observation.contract_id,
                    generation=observation.generation,
                )
            )
            if contract_entry is not None:
                continue
            try:
                await self._maintenance_store.delete(
                    entry.key,
                    revision=entry.revision,
                )
            except ConcordConflict:
                continue
            cleared += 1
        return cleared
