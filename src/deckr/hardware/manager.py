"""Shared hardware inventory, claim admission, and authorized message routing."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

import anyio

import deckr.hardware.messages as hw_messages
from deckr.beacon import Beacon, BeaconAdvertisementLease, BeaconAdvertisementSpec
from deckr.concord import (
    Concord,
    ConcordConflict,
    ConcordParticipant,
    ConcordParticipantCandidate,
    ConcordParticipantPolicy,
    ConcordParticipantSelection,
    ConcordUnavailable,
    ContractState,
    ContractValidityStatus,
)
from deckr.contracts.authority import ContractPointer
from deckr.contracts.messages import HARDWARE_MESSAGES_LANE, DeckrMessage
from deckr.contracts.models import JsonObject, thaw_json
from deckr.core.kv import KvConflict, KvUnavailable
from deckr.hardware.claim_policy import (
    claim_matches_inventory,
    descriptor_requires_new_claim,
    device_reference_matches,
)
from deckr.hardware.claims import (
    LiveHardwareClaim,
    _ClaimCandidate,
    _HardwareCommandHandler,
    _HardwareEndpoint,
    _HardwareResetHandler,
    logger,
)
from deckr.hardware.command_replies import reject_hardware_command
from deckr.hardware.descriptors import DeviceDescriptor, DeviceRef
from deckr.hardware.profiles import (
    HARDWARE_CLAIM_PROFILE_ID,
    HARDWARE_FEATURE_ID,
    HardwareAdvertisementDevice,
    HardwareBeaconPayload,
    HardwareClaimTerms,
    ProfileCapacity,
)


@dataclass(slots=True)
class HardwareManagerRuntime:
    endpoint: _HardwareEndpoint
    beacon: Beacon
    concord: Concord
    manager_id: str
    initial_devices: Mapping[str, DeviceDescriptor] | None = None
    labels: Mapping[str, str] | None = None
    command_handler: _HardwareCommandHandler | None = None
    reset_handler: _HardwareResetHandler | None = None
    _devices: dict[str, DeviceDescriptor] = field(init=False, default_factory=dict)
    _advertiser: BeaconAdvertisementLease | None = field(init=False, default=None)
    _advertised_payload: JsonObject | None = field(init=False, default=None)
    _advertisement_dirty: bool = field(init=False, default=True)
    _claims: dict[str, LiveHardwareClaim] = field(init=False, default_factory=dict)
    _claims_by_device: dict[str, LiveHardwareClaim] = field(
        init=False,
        default_factory=dict,
    )
    _claim_manager: ConcordParticipant = field(init=False)
    _lock: anyio.Lock = field(init=False, default_factory=anyio.Lock)
    _advertisement_lock: anyio.Lock = field(init=False, default_factory=anyio.Lock)
    _task_group: anyio.abc.TaskGroup | None = field(init=False, default=None)
    _closed: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        if self.endpoint.address.family != "hardware_manager":
            raise ValueError(
                "hardware manager runtime endpoint must be hardware_manager"
            )
        if self.endpoint.address.endpoint_id != self.manager_id:
            raise ValueError("manager_id must match hardware_manager endpoint id")
        self._devices = dict(self.initial_devices or {})
        self._claim_manager = self.concord.participant(
            self.endpoint,
            ConcordParticipantPolicy(
                profile=HARDWARE_CLAIM_PROFILE_ID,
                select=self._select_claim_contracts,
            ),
        )

    @property
    def live_claims(self) -> tuple[LiveHardwareClaim, ...]:
        return tuple(self._claims[key] for key in sorted(self._claims))

    async def set_device(self, descriptor: DeviceDescriptor) -> None:
        previous = self._devices.get(descriptor.device_id)
        replaced = previous is not None and descriptor_requires_new_claim(
            previous, descriptor
        )
        self._devices[descriptor.device_id] = descriptor
        if replaced:
            self._claims_by_device.pop(descriptor.device_id, None)
            await self._cancel_claims_for_device(
                descriptor.device_id,
                reason=f"hardware device {descriptor.device_id} replaced",
            )
        await self._publish_advertisement_if_changed()
        if self._task_group is not None:
            await self._claim_manager.notify_policy_changed()
            await self._reconcile_claims(
                reason="device inventory changed",
                managed=self._claim_manager.state.contracts,
            )

    async def remove_device(
        self,
        device_id: str,
        reason: str = "removed",
    ) -> None:
        self._devices.pop(device_id, None)
        self._claims_by_device.pop(device_id, None)
        await self._cancel_claims_for_device(
            device_id,
            reason=f"hardware device {device_id} {reason}",
        )
        await self._publish_advertisement()
        if self._task_group is not None:
            await self._claim_manager.notify_policy_changed()
            await self._reconcile_claims(
                reason="device inventory changed",
                managed=self._claim_manager.state.contracts,
            )

    async def start(self, task_group: anyio.abc.TaskGroup) -> None:
        self._closed = False
        self._task_group = task_group
        try:
            await self.concord.bootstrap_owner(self.endpoint)  # noqa: SLF001
            await self._claim_manager.__aenter__()
            await self._reconcile_claims(
                reason="participant bootstrap",
                managed=self._claim_manager.state.contracts,
            )
            await self._publish_advertisement(required=True)
            task_group.start_soon(self._command_subscription_loop)
            task_group.start_soon(self._contract_event_loop)
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        with anyio.CancelScope(shield=True):
            self._closed = True
            with anyio.move_on_after(5):
                async with anyio.create_task_group() as cleanup_group:
                    cleanup_group.start_soon(self._withdraw_advertisement)
                    cleanup_group.start_soon(self._claim_manager.aclose)
            async with self._lock:
                self._claims.clear()
                self._claims_by_device.clear()
            self._task_group = None

    async def replace_devices(
        self,
        devices: Mapping[str, DeviceDescriptor],
        *,
        removed_reason: str = "removed",
    ) -> None:
        next_devices = dict(devices)
        previous = dict(self._devices)
        removed_device_ids = sorted(set(previous) - set(next_devices))
        replaced_device_ids = sorted(
            device_id
            for device_id in set(previous) & set(next_devices)
            if descriptor_requires_new_claim(
                previous[device_id], next_devices[device_id]
            )
        )
        self._devices = next_devices
        for device_id in [*removed_device_ids, *replaced_device_ids]:
            self._claims_by_device.pop(device_id, None)
        for device_id in removed_device_ids:
            await self._cancel_claims_for_device(
                device_id,
                reason=f"hardware device {device_id} {removed_reason}",
            )
        for device_id in replaced_device_ids:
            await self._cancel_claims_for_device(
                device_id,
                reason=f"hardware device {device_id} replaced",
            )
        await self._publish_advertisement_if_changed()
        if self._task_group is not None:
            await self._claim_manager.notify_policy_changed()
            await self._reconcile_claims(
                reason="device snapshot changed",
                managed=self._claim_manager.state.contracts,
            )

    async def handle_hardware_message(self, message: DeckrMessage) -> bool:
        event = hw_messages.hardware_body_from_message(message)
        ref = hw_messages.hardware_device_ref_from_message(message)
        if ref is None or ref.manager_id != self.manager_id:
            return False
        if not isinstance(
            event,
            hw_messages.ControlInputMessage | hw_messages.CapabilityStateChangedMessage,
        ):
            return False
        if not device_reference_matches(ref, self.endpoint.address, self._devices):
            logger.debug(
                "Dropping input/state for unknown hardware device %s/%s",
                ref.manager_id,
                ref.device_id,
            )
            return False
        claim = self._claims_by_device.get(ref.device_id)
        if claim is None or not self._claim_has_authority(claim):
            logger.debug(
                "Dropping unclaimed hardware input/state for %s/%s",
                ref.manager_id,
                ref.device_id,
            )
            return False
        await self.endpoint.send(
            lane=HARDWARE_MESSAGES_LANE,
            recipient=claim.controller_endpoint,
            recipient_session_id=claim.controller_session_id,
            message_type=message.message_type,
            body=hw_messages.hardware_body_to_dict(event),
            subject=message.subject,
            causation_id=message.causation_id,
            contract={
                "contractId": claim.contract.contract_id,
                "generation": claim.contract.generation,
            },
        )
        return True

    async def _command_subscription_loop(self) -> None:
        async with self.endpoint.subscribe(HARDWARE_MESSAGES_LANE) as stream:
            async for envelope in stream:
                await self._handle_command(envelope)

    async def _handle_command(self, envelope: DeckrMessage) -> bool:
        ref = hw_messages.hardware_device_ref_from_message(envelope)
        if ref is None or ref.manager_id != self.manager_id:
            return False
        body = hw_messages.hardware_body_from_message(envelope)
        if not isinstance(
            body,
            hw_messages.ControlCommandMessage
            | hw_messages.CapabilityStateRequestMessage,
        ):
            return False
        if not device_reference_matches(ref, self.endpoint.address, self._devices):
            await reject_hardware_command(self.endpoint, envelope, body, reason="stale")
            return False
        claim = await self._command_claim(ref.device_id, envelope)
        if claim is None:
            await reject_hardware_command(
                self.endpoint, envelope, body, reason="unauthorized"
            )
            return False
        if envelope.sender_session_id != claim.controller_session_id:
            await reject_hardware_command(self.endpoint, envelope, body, reason="stale")
            return False
        if self.command_handler is None:
            await reject_hardware_command(
                self.endpoint, envelope, body, reason="unsupported"
            )
            return False
        try:
            handled = await self.command_handler(envelope)
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            handled = False
        if handled is None:
            await reject_hardware_command(
                self.endpoint, envelope, body, reason="unsupported"
            )
            return False
        if not handled:
            await reject_hardware_command(self.endpoint, envelope, body, reason="stale")
            return False
        return True

    async def _command_claim(
        self,
        device_id: str,
        envelope: DeckrMessage,
    ) -> LiveHardwareClaim | None:
        if self._closed:
            return None
        if envelope.contract is None:
            return None
        claim = self._claims_by_device.get(device_id)
        if claim is None or envelope.sender != claim.controller_endpoint:
            return None
        if (
            envelope.contract.contract_id != claim.contract.contract_id
            or envelope.contract.generation != claim.contract.generation
        ):
            return None
        return claim if self._claim_has_authority(claim) else None

    def _claim_has_authority(self, claim: LiveHardwareClaim) -> bool:
        managed = self._claim_manager.managed_contract(claim.contract)
        return (
            not self._closed
            and managed is not None
            and managed.local_authority
            and managed.validity.valid
        )

    async def _publish_advertisement(self, *, required: bool = False) -> None:
        if self._closed:
            return
        async with self._advertisement_lock:
            if self._closed:
                return
            payload = self._hardware_payload()
            payload_dict = payload.to_dict()
            try:
                if self._advertiser is None or self._advertiser.closed:
                    advertiser = self.beacon.advertise(
                        self.endpoint,
                        BeaconAdvertisementSpec(
                            feature_id=HARDWARE_FEATURE_ID,
                            labels=payload.labels,
                            payload=payload_dict,
                        ),
                    )
                    await advertiser.__aenter__()
                    self._advertiser = advertiser
                else:
                    await self._advertiser.update(
                        labels=payload.labels,
                        payload=payload_dict,
                    )
                self._advertised_payload = payload_dict
                self._advertisement_dirty = False
            except KvConflict:
                logger.info(
                    "Hardware Beacon advertisement changed; creating a fresh one",
                    exc_info=True,
                )
                advertiser = self._advertiser
                self._advertiser = None
                self._advertised_payload = None
                self._advertisement_dirty = True
                if advertiser is not None:
                    await advertiser.aclose()
                if required:
                    raise
            except KvUnavailable:
                logger.warning(
                    "Hardware Beacon advertisements unavailable; retrying later"
                )
                self._advertisement_dirty = True
                if required:
                    raise

    async def _publish_advertisement_if_changed(self) -> None:
        if self._closed:
            return
        async with self._advertisement_lock:
            if self._closed:
                return
            payload = self._hardware_payload()
            payload_dict = payload.to_dict()
            if (
                not self._advertisement_dirty
                and self._advertiser is not None
                and not self._advertiser.closed
                and self._advertised_payload == payload_dict
            ):
                return

        await self._publish_advertisement()

    async def _withdraw_advertisement(self) -> None:
        async with self._advertisement_lock:
            self._advertised_payload = None
            self._advertisement_dirty = True
            if self._advertiser is not None:
                try:
                    await self._advertiser.aclose()
                except (KvConflict, KvUnavailable):
                    logger.debug("Could not withdraw hardware Beacon advertisement")
                self._advertiser = None

    async def _contract_event_loop(self) -> None:
        async with self._claim_manager.watch_state() as stream:
            async for snapshot in stream:
                await self._reconcile_claims(
                    reason=f"managed contracts version {snapshot.version}",
                    managed=snapshot.contracts,
                )

    async def _reconcile_claims(
        self,
        *,
        reason: str,
        managed: Iterable[Any] | None = None,
    ) -> None:
        if self._closed:
            return
        if managed is None:
            managed = self._claim_manager.state.contracts
        async with self._lock:
            if self._closed:
                return
            await self._reconcile_claims_locked(reason=reason, managed=managed)

    async def _reconcile_claims_locked(
        self,
        *,
        reason: str,
        managed: Iterable[Any],
    ) -> None:
        logger.debug("Reconciling hardware manager claims via %s", reason)
        candidates = await self._matching_claim_candidates(managed=managed)
        ordered = self._ordered_claim_candidates(candidates)
        next_claims: dict[str, LiveHardwareClaim] = {}
        next_by_device: dict[str, LiveHardwareClaim] = {}

        for candidate in ordered:
            overlapping = set(candidate.device_ids) & set(next_by_device)
            if overlapping:
                continue
            if not candidate.valid or not candidate.local_authority:
                continue
            if candidate.controller_session_id is None:
                continue
            live = LiveHardwareClaim(
                contract=candidate.contract,
                terms=candidate.terms,
                controller_endpoint=candidate.terms.controller_endpoint,
                controller_session_id=candidate.controller_session_id,
                device_refs=tuple(
                    device.device_ref for device in candidate.terms.devices
                ),
            )
            next_claims[candidate.contract.key] = live
            for device_id in live.device_ids:
                next_by_device[device_id] = live

        lost_claims = {
            key: claim for key, claim in self._claims.items() if key not in next_claims
        }
        self._claims = next_claims
        self._claims_by_device = next_by_device
        if lost_claims:
            await self._reset_lost_claim_devices(lost_claims.values())
        await self._publish_advertisement_if_changed()

    async def _matching_claim_candidates(
        self,
        *,
        managed: Iterable[Any],
    ) -> dict[str, _ClaimCandidate]:
        candidates: dict[str, _ClaimCandidate] = {}
        for observed in managed:
            # A queued snapshot can predate a device-change cancellation. Never
            # reinstall a claim from authority captured before acquiring the lock.
            managed_contract = self._claim_manager.managed_contract(observed.contract)
            if managed_contract is None:
                continue
            validity = managed_contract.validity
            record = managed_contract.record
            if record is None or record.state != ContractState.OPEN:
                continue
            try:
                terms = HardwareClaimTerms.from_wire(thaw_json(record.terms or {}))
            except ValueError:
                continue
            if not claim_matches_inventory(
                terms,
                managed_contract.contract.participants,
                self.endpoint.address,
                self._devices,
            ):
                continue
            candidates[managed_contract.contract.key] = _ClaimCandidate(
                contract=managed_contract.contract,
                terms=terms,
                local_authority=managed_contract.local_authority,
                valid=validity.status == ContractValidityStatus.VALID,
                controller_session_id=(
                    validity.tokens[str(terms.controller_endpoint)].session_id
                    if validity.status == ContractValidityStatus.VALID
                    and str(terms.controller_endpoint) in validity.tokens
                    else None
                ),
            )
        return candidates

    def _select_claim_contracts(
        self,
        candidates: tuple[ConcordParticipantCandidate, ...],
    ) -> ConcordParticipantSelection:
        parsed: dict[str, tuple[ConcordParticipantCandidate, HardwareClaimTerms]] = {}
        for candidate in candidates:
            try:
                terms = HardwareClaimTerms.from_wire(
                    thaw_json(candidate.record.terms or {})
                )
            except ValueError:
                continue
            if not claim_matches_inventory(
                terms,
                candidate.contract.participants,
                self.endpoint.address,
                self._devices,
            ):
                continue
            parsed[candidate.contract.key] = (candidate, terms)

        ordered = [parsed[key] for key in sorted(self._claims) if key in parsed]
        existing = {candidate.contract.key for candidate, _terms in ordered}
        ordered.extend(
            value for key, value in sorted(parsed.items()) if key not in existing
        )
        selected: list[ContractPointer] = []
        selected_devices: set[str] = set()
        for candidate, terms in ordered:
            device_ids = {device.device_ref.device_id for device in terms.devices}
            if device_ids & selected_devices:
                continue
            selected_devices.update(device_ids)
            selected.append(candidate.contract.pointer)
        return ConcordParticipantSelection(selected)

    async def _cancel_claims_for_device(self, device_id: str, *, reason: str) -> None:
        async with self._lock:
            claims = [
                claim
                for claim in self._claims.values()
                if device_id in claim.device_ids
            ]
            cancelled_claims: list[LiveHardwareClaim] = []
            for claim in claims:
                try:
                    cancelled = await self._claim_manager.cancel(
                        claim.contract,
                        reason=reason,
                    )
                except (ConcordConflict, ConcordUnavailable):
                    logger.debug(
                        "Could not cancel hardware claim %s for unavailable device %s",
                        claim.contract.contract_id,
                        device_id,
                        exc_info=True,
                    )
                    continue
                if cancelled:
                    self._claims.pop(claim.contract.key, None)
                    for claimed_device_id in claim.device_ids:
                        self._claims_by_device.pop(claimed_device_id, None)
                    cancelled_claims.append(claim)
            if cancelled_claims:
                await self._reset_lost_claim_devices(cancelled_claims)

    def _ordered_claim_candidates(
        self,
        candidates: Mapping[str, _ClaimCandidate],
    ) -> tuple[_ClaimCandidate, ...]:
        existing = [
            candidates[key]
            for key in sorted(self._claims)
            if key in candidates and self._claims[key].contract.key == key
        ]
        existing_keys = {candidate.contract.key for candidate in existing}
        new = [
            candidate
            for key, candidate in sorted(candidates.items())
            if key not in existing_keys
        ]
        return tuple(existing + new)

    async def _reset_lost_claim_devices(
        self,
        claims: Any,
    ) -> None:
        if self.reset_handler is None:
            return
        reset_devices: set[str] = set()
        for claim in claims:
            reset_devices.update(claim.device_ids)
        for device_id in sorted(reset_devices):
            try:
                await self.reset_handler(device_id)
            except (anyio.BrokenResourceError, anyio.ClosedResourceError):
                logger.debug(
                    "Could not reset closed hardware device session %s", device_id
                )

    def _hardware_payload(self) -> HardwareBeaconPayload:
        claimed = set(self._claims_by_device)
        return HardwareBeaconPayload(
            managerId=self.manager_id,
            managerEndpoint=self.endpoint.address,
            sessionId=self.endpoint.session_id,
            labels=dict(self.labels or {}),
            devices={
                device_id: HardwareAdvertisementDevice(
                    capacity=ProfileCapacity(
                        totalInstances=1,
                        claimedInstances=1 if device_id in claimed else 0,
                        availableInstances=0 if device_id in claimed else 1,
                    ),
                    deviceRef=DeviceRef(
                        managerId=self.manager_id,
                        deviceId=device_id,
                        fingerprint=descriptor.fingerprint,
                    ),
                    descriptor=descriptor,
                )
                for device_id, descriptor in sorted(self._devices.items())
            },
        )
