"""Public manager observations derived from its actual protocol owners."""

from deckr.hardware._state import HardwareStateChannel
from deckr.hardware.profiles import HARDWARE_FEATURE_ID
from deckr.hardware.state import HardwareFailure, HardwareManagerState, HardwarePhase


class HardwareManagerObserver:
    def __init__(self, runtime) -> None:
        self.runtime = runtime
        self.phase = HardwarePhase.STARTING
        self.failure = None
        self.channel = HardwareStateChannel(self.snapshot)

    def snapshot(self) -> HardwareManagerState:
        runtime = self.runtime
        active = self.phase not in {HardwarePhase.CLOSING, HardwarePhase.CLOSED}
        current = active and runtime.concord.is_current()
        claims = tuple(
            claim
            for claim in runtime.live_claims
            if active and runtime._claim_has_authority(claim)
        )
        return HardwareManagerState(
            version=0,
            phase=(
                HardwarePhase.UNAVAILABLE
                if self.phase == HardwarePhase.READY and not current
                else self.phase
            ),
            inventory=runtime._devices,
            claims=claims,
            discovery_current=active and runtime.beacon.is_current(),
            advertisement_published=(
                active
                and not runtime._advertisement_dirty
                and runtime._advertiser is not None
                and not runtime._advertiser.closed
            ),
            authority_current=current,
            failure=(
                self.failure
                or (
                    HardwareFailure(
                        "authority_unavailable", "Concord authority view is not current"
                    )
                    if self.phase == HardwarePhase.READY and not current
                    else None
                )
            ),
        )

    async def discovery_loop(self) -> None:
        async with self.runtime.beacon.watch(HARDWARE_FEATURE_ID) as states:
            async for _state in states:
                await self.channel.publish()
