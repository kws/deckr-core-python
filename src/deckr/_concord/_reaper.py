"""Reaper."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from time import monotonic
from typing import Any

import anyio

from deckr._concord._keys import (
    concord_contract_key,
    parse_concord_contract_key,
    parse_concord_token_cleanup_key,
)
from deckr._concord._maintenance_api import ConcordMaintenance
from deckr._concord._maintenance_audit import _datetime_from_raw_value, _ensure_utc
from deckr._concord._maintenance_models import (
    CONCORD_REAPER_STALE_CONTRACT_REASON,
    ConcordReaperConfig,
    ConcordReaperScanResult,
    _empty_reaper_counts,
    _now_utc,
    logger,
)
from deckr._concord._maintenance_validation import (
    _open_contract_validity_is_stale,
    _parse_contract_entry,
    _parse_token_cleanup_entry,
)
from deckr._concord._models import (
    ConcordConflict,
    ConcordConflictCode,
    ConcordUnavailable,
    ContractHandle,
    ContractRecord,
    ContractState,
    ContractValidityStatus,
    contract_handle,
)
from deckr.core.identity import ContractPointer
from deckr.core.kv import KvEntry


class ConcordReaperService:
    """Periodic low-frequency scanning over a Concord maintenance capability."""

    def __init__(
        self,
        maintenance: ConcordMaintenance,
        *,
        config: ConcordReaperConfig | Mapping[str, Any] | None = None,
        clock: Callable[[], datetime] = _now_utc,
    ) -> None:
        self._maintenance = maintenance
        self._config = (
            config
            if isinstance(config, ConcordReaperConfig)
            else ConcordReaperConfig.model_validate(dict(config or {}))
        )
        self._clock = clock
        self._closed = False

    @property
    def config(self) -> ConcordReaperConfig:
        return self._config

    async def aclose(self) -> None:
        self._closed = True

    async def run(self, *, stop_event: anyio.Event | None = None) -> None:
        while not self._closed:
            try:
                await self.scan_once()
            except ConcordUnavailable:
                logger.warning(
                    "%s Concord reaper state unavailable; scan will retry",
                    self._config.log_label,
                    exc_info=True,
                )
            if self._closed or (stop_event is not None and stop_event.is_set()):
                return
            if stop_event is None:
                await anyio.sleep(self._config.scan_interval_seconds)
                continue
            with anyio.move_on_after(self._config.scan_interval_seconds):
                await stop_event.wait()
            if stop_event.is_set():
                return

    async def scan_once(self) -> ConcordReaperScanResult:
        started_at = monotonic()
        now = _ensure_utc(self._clock())
        counts: dict[str, int] = {
            "scanned_contract_count": 0,
            "stale_observations_created": 0,
            "stale_observations_cleared": 0,
            "contracts_cancelled": 0,
            "contracts_deleted": 0,
            "token_keys_deleted": 0,
            "token_cleanups_completed": 0,
        }
        for entry in await self._maintenance._token_cleanup_entries():
            try:
                entry_counts = await self._resume_token_cleanup_entry(entry)
            except ConcordConflict:
                logger.info(
                    "%s Concord reaper token cleanup failed closed marker_key=%s",
                    self._config.log_label,
                    entry.key,
                    exc_info=True,
                )
                continue
            for key, value in entry_counts.items():
                counts[key] += value
        for entry in await self._maintenance._contract_entries():
            parsed = parse_concord_contract_key(entry.key)
            if parsed is None:
                continue
            counts["scanned_contract_count"] += 1
            contract_id, generation = parsed
            try:
                entry_counts = await self._scan_contract_entry(
                    entry,
                    contract_id=contract_id,
                    generation=generation,
                    now=now,
                )
            except ConcordConflict:
                logger.info(
                    "%s Concord reaper entry failed closed contract_key=%s "
                    "contract=%s generation=%s",
                    self._config.log_label,
                    entry.key,
                    contract_id,
                    generation,
                    exc_info=True,
                )
                continue
            for key, value in entry_counts.items():
                counts[key] += value
        counts[
            "stale_observations_cleared"
        ] += await self._maintenance._clear_orphaned_stale_observations()
        stale_observation_count = len(
            await self._maintenance._stale_observation_entries()
        )
        token_cleanup_count = len(await self._maintenance._token_cleanup_entries())
        result = ConcordReaperScanResult(
            scanned_contract_count=counts["scanned_contract_count"],
            stale_observation_count=stale_observation_count,
            stale_observations_created=counts["stale_observations_created"],
            stale_observations_cleared=counts["stale_observations_cleared"],
            contracts_cancelled=counts["contracts_cancelled"],
            contracts_deleted=counts["contracts_deleted"],
            token_keys_deleted=counts["token_keys_deleted"],
            token_cleanups_completed=counts["token_cleanups_completed"],
            token_cleanups_pending=token_cleanup_count,
        )
        logger.info(
            "%s Concord reaper scan completed scanned=%s cancelled=%s "
            "deleted=%s token_keys_deleted=%s token_cleanups_completed=%s "
            "token_cleanups_pending=%s stale_observations=%s "
            "stale_created=%s stale_cleared=%s elapsed_ms=%.1f",
            self._config.log_label,
            result.scanned_contract_count,
            result.contracts_cancelled,
            result.contracts_deleted,
            result.token_keys_deleted,
            result.token_cleanups_completed,
            result.token_cleanups_pending,
            result.stale_observation_count,
            result.stale_observations_created,
            result.stale_observations_cleared,
            (monotonic() - started_at) * 1000,
        )
        return result

    async def _resume_token_cleanup_entry(self, entry: KvEntry) -> dict[str, int]:
        counts = _empty_reaper_counts()
        parsed = parse_concord_token_cleanup_key(entry.key)
        if parsed is None:
            raise ConcordConflict(
                ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
                "Concord token-cleanup marker has a noncanonical key",
                key=entry.key,
            )
        contract_id, generation = parsed
        marker = _parse_token_cleanup_entry(
            entry,
            key=entry.key,
            contract_id=contract_id,
            generation=generation,
        )
        contract_key = concord_contract_key(
            contract_id=contract_id,
            generation=generation,
        )
        contract_entry = await self._maintenance._contract_store.get_exact(contract_key)
        if contract_entry is not None:
            record = _parse_contract_entry(
                contract_entry,
                contract_key,
                ContractPointer(contractId=contract_id, generation=generation),
            )
            if contract_entry.revision == marker.contract_revision:
                if (
                    record.state != ContractState.CANCELLED
                    or record.terms_hash != marker.terms_hash
                ):
                    raise ConcordConflict(
                        ConcordConflictCode.CONTRACT_IDENTITY_MISMATCH,
                        "Concord token-cleanup marker differs from retained contract",
                        key=entry.key,
                        expected_pointer=ContractPointer(
                            contractId=contract_id,
                            generation=generation,
                        ),
                    )
                return counts
            try:
                await self._maintenance._maintenance_store.delete(
                    entry.key,
                    revision=entry.revision,
                )
            except ConcordConflict:
                pass
            return counts

        cleanup = await self._maintenance._complete_token_cleanup(
            entry,
            log_label=self._config.log_label,
        )
        counts["token_keys_deleted"] += cleanup.deleted_token_key_count
        if cleanup.completed:
            counts["token_cleanups_completed"] += 1
        return counts

    async def _scan_contract_entry(
        self,
        entry: KvEntry,
        *,
        contract_id: str,
        generation: int,
        now: datetime,
    ) -> dict[str, int]:
        try:
            record = ContractRecord.from_wire(entry.value)
        except (TypeError, ValueError) as exc:
            return await self._scan_invalid_contract_entry(
                entry,
                contract_id=contract_id,
                generation=generation,
                now=now,
                invalid_reason=str(exc),
            )
        if record.contract_id != contract_id or record.generation != generation:
            return await self._scan_invalid_contract_entry(
                entry,
                contract_id=contract_id,
                generation=generation,
                now=now,
                invalid_reason="contract key and record identity differ",
            )
        handle = contract_handle(entry.key, record, entry.revision)
        if record.state == ContractState.CANCELLED:
            return await self._scan_cancelled_contract(handle, record, now=now)
        return await self._scan_open_contract(handle, now=now)

    async def _scan_open_contract(
        self,
        contract: ContractHandle,
        *,
        now: datetime,
    ) -> dict[str, int]:
        counts = _empty_reaper_counts()
        validity = await self._maintenance.validate_exact(contract)
        if _open_contract_validity_is_stale(validity):
            first_observed, created = await self._maintenance._observe_stale(
                contract_id=contract.contract_id,
                generation=contract.generation,
                status=validity.status,
                reason=validity.reason,
                contract_revision=contract.revision,
                now=now,
            )
            if created:
                counts["stale_observations_created"] += 1
            if (
                now - first_observed
            ).total_seconds() >= self._config.stale_grace_seconds:
                try:
                    cancelled = await self._maintenance.cancel_contract(
                        contract,
                        reason=CONCORD_REAPER_STALE_CONTRACT_REASON,
                        log_label=self._config.log_label,
                        now=now,
                    )
                except (ConcordConflict, ValueError):
                    logger.info(
                        "%s Concord reaper stale contract cancel conflict; "
                        "leaving for next scan contract=%s generation=%s "
                        "status=%s",
                        self._config.log_label,
                        contract.contract_id,
                        contract.generation,
                        validity.status.value,
                        exc_info=True,
                    )
                else:
                    if cancelled:
                        counts["contracts_cancelled"] += 1
                    if await self._maintenance._clear_stale_observation(
                        contract.contract_id,
                        contract.generation,
                    ):
                        counts["stale_observations_cleared"] += 1
            return counts

        if validity.status == ContractValidityStatus.UNAVAILABLE:
            return counts
        if await self._maintenance._clear_stale_observation(
            contract.contract_id,
            contract.generation,
        ):
            counts["stale_observations_cleared"] += 1
        return counts

    async def _scan_cancelled_contract(
        self,
        contract: ContractHandle,
        record: ContractRecord,
        *,
        now: datetime,
    ) -> dict[str, int]:
        counts = _empty_reaper_counts()
        if await self._maintenance._clear_stale_observation(
            contract.contract_id,
            contract.generation,
        ):
            counts["stale_observations_cleared"] += 1
        if (
            record.cancelled_at is None
            or (now - record.cancelled_at.astimezone(UTC)).total_seconds()
            < self._config.cancelled_retention_seconds
        ):
            return counts
        result = await self._maintenance.delete_cancelled_contract(
            contract,
            retention_seconds=self._config.cancelled_retention_seconds,
            log_label=self._config.log_label,
            now=now,
        )
        if result.contract_deleted:
            counts["contracts_deleted"] += 1
            if await self._maintenance._clear_stale_observation(
                contract.contract_id,
                contract.generation,
            ):
                counts["stale_observations_cleared"] += 1
        counts["token_keys_deleted"] += result.deleted_token_key_count
        if result.cleanup_completed:
            counts["token_cleanups_completed"] += 1
        return counts

    async def _scan_invalid_contract_entry(
        self,
        entry: KvEntry,
        *,
        contract_id: str,
        generation: int,
        now: datetime,
        invalid_reason: str,
    ) -> dict[str, int]:
        counts = _empty_reaper_counts()
        raw_state = str(entry.value.get("state", ContractState.OPEN.value))
        if raw_state == ContractState.CANCELLED.value:
            if await self._maintenance._clear_stale_observation(
                contract_id,
                generation,
            ):
                counts["stale_observations_cleared"] += 1
            cancelled_at = _datetime_from_raw_value(entry.value.get("cancelledAt"))
            if (
                cancelled_at is None
                or (now - cancelled_at).total_seconds()
                < self._config.cancelled_retention_seconds
            ):
                return counts
            logger.info(
                "%s Concord maintenance retained malformed cancelled contract "
                "contract_key=%s contract=%s generation=%s revision=%s reason=%s",
                self._config.log_label,
                entry.key,
                contract_id,
                generation,
                entry.revision,
                invalid_reason,
            )
            return counts

        first_observed, created = await self._maintenance._observe_stale(
            contract_id=contract_id,
            generation=generation,
            status=ContractValidityStatus.INVALID_CONTRACT,
            reason=invalid_reason,
            contract_revision=entry.revision,
            now=now,
        )
        if created:
            counts["stale_observations_created"] += 1
        if (now - first_observed).total_seconds() < self._config.stale_grace_seconds:
            return counts
        logger.info(
            "%s Concord reaper invalid contract cannot be cancelled safely; "
            "leaving for next scan contract=%s generation=%s",
            self._config.log_label,
            contract_id,
            generation,
        )
        return counts
