"""Generic beacon API; storage and profile policy are caller-owned."""

from deckr._beacon._api import (
    Beacon as Beacon,
)
from deckr._beacon._directory import (
    BeaconDirectory as BeaconDirectory,
)
from deckr._beacon._lease import (
    BeaconAdvertisementLease as BeaconAdvertisementLease,
)
from deckr._beacon._models import (
    BEACON_ADVERTISEMENT_SCHEMA_ID as BEACON_ADVERTISEMENT_SCHEMA_ID,
    AdvertisementRecord as AdvertisementRecord,
    BeaconAdvertisementSpec as BeaconAdvertisementSpec,
    BeaconEndpointSession as BeaconEndpointSession,
    BeaconProtocol as BeaconProtocol,
    BeaconWatchChange as BeaconWatchChange,
    BeaconWatchSnapshot as BeaconWatchSnapshot,
    Candidate as Candidate,
    CandidateStatus as CandidateStatus,
    beacon_advertisement_key as beacon_advertisement_key,
    beacon_feature_prefix as beacon_feature_prefix,
    parse_beacon_advertisement_key as parse_beacon_advertisement_key,
)

__all__ = [
    "BEACON_ADVERTISEMENT_SCHEMA_ID",
    "AdvertisementRecord",
    "Beacon",
    "BeaconAdvertisementLease",
    "BeaconAdvertisementSpec",
    "BeaconDirectory",
    "BeaconEndpointSession",
    "BeaconProtocol",
    "BeaconWatchChange",
    "BeaconWatchSnapshot",
    "Candidate",
    "CandidateStatus",
    "beacon_advertisement_key",
    "beacon_feature_prefix",
    "parse_beacon_advertisement_key",
]
