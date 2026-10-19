"""Hardware advertisement publication owned by the manager lifecycle."""

from deckr.beacon import BeaconAdvertisementSpec
from deckr.core.kv import KvConflict, KvUnavailable
from deckr.hardware.claims import logger
from deckr.hardware.descriptors import DeviceRef
from deckr.hardware.profiles import (
    HARDWARE_FEATURE_ID,
    HardwareAdvertisementDevice,
    HardwareBeaconPayload,
    ProfileCapacity,
)
from deckr.hardware.state import HardwareFailure


class HardwareManagerAdvertising:
    async def _publish_advertisement(self, *, required: bool = False) -> None:
        if self._closed or not self._advertising_ready:
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
                self._observer.failure = None
                await self._observer.channel.publish()
            except KvConflict:
                self._observer.failure = HardwareFailure(
                    "advertisement_conflict", "Advertisement ownership changed"
                )
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
                await self._observer.channel.publish()
                if required:
                    raise
            except KvUnavailable:
                self._observer.failure = HardwareFailure(
                    "discovery_unavailable", "Advertisement store unavailable"
                )
                logger.warning(
                    "Hardware Beacon advertisements unavailable; retrying later"
                )
                self._advertisement_dirty = True
                await self._observer.channel.publish()
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
