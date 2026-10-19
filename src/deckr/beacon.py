"""Public exports; implementation modules are separated by responsibility."""

from deckr._authority_buckets import (
    BEACON_ADVERTISEMENT_STORE_POLICY,
    DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME,
    DEFAULT_BEACON_TTL_SECONDS,
)
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
    AdvertisementFilter as AdvertisementFilter,
    AdvertisementRecord as AdvertisementRecord,
    AdvertisementSelector as AdvertisementSelector,
    BeaconAdvertisementSpec as BeaconAdvertisementSpec,
    BeaconDirectoryParser as BeaconDirectoryParser,
    BeaconDirectoryPredicate as BeaconDirectoryPredicate,
    BeaconDirectorySelector as BeaconDirectorySelector,
    BeaconEndpointSession as BeaconEndpointSession,
    BeaconProtocol as BeaconProtocol,
    BeaconWatchChange as BeaconWatchChange,
    BeaconWatchSnapshot as BeaconWatchSnapshot,
    Candidate as Candidate,
    CandidateStatus as CandidateStatus,
    T as T,
    _AdvertisementHandle as _AdvertisementHandle,
    _BeaconAdvertisementMissing as _BeaconAdvertisementMissing,
    _BeaconAdvertisementOwnerChanged as _BeaconAdvertisementOwnerChanged,
    _require_text as _require_text,
    beacon_advertisement_key as beacon_advertisement_key,
    beacon_feature_prefix as beacon_feature_prefix,
    parse_beacon_advertisement_key as parse_beacon_advertisement_key,
)
from deckr._beacon._records import (
    _advertisement_handle as _advertisement_handle,
    _advertisement_matches_handle as _advertisement_matches_handle,
    _candidate_from_entry as _candidate_from_entry,
    _candidate_newest_sort_key as _candidate_newest_sort_key,
    _exact_get as _exact_get,
    _feature_endpoint_key as _feature_endpoint_key,
    _filter_candidates as _filter_candidates,
    _is_materialized_bucket as _is_materialized_bucket,
    _record_from_spec as _record_from_spec,
    _selector_accepts as _selector_accepts,
    _updated_record as _updated_record,
)
from deckr._beacon._support import (
    _UNSET as _UNSET,
    _beacon_lifecycle_log_level as _beacon_lifecycle_log_level,
    _beacon_refresh_interval as _beacon_refresh_interval,
    _beacon_ttl_seconds as _beacon_ttl_seconds,
    _now_utc as _now_utc,
    logger as logger,
)
from deckr._beacon._tasks import (
    _BeaconTaskGroup as _BeaconTaskGroup,
)

__all__ = [
    "BEACON_ADVERTISEMENT_SCHEMA_ID",
    "BEACON_ADVERTISEMENT_STORE_POLICY",
    "DEFAULT_BEACON_ADVERTISEMENT_STORE_NAME",
    "DEFAULT_BEACON_TTL_SECONDS",
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
