"""Support."""

from __future__ import annotations

import logging
import random
from datetime import UTC, datetime

from deckr.core.kv import KvUnavailable

logger = logging.getLogger(__name__)


def _beacon_lifecycle_log_level(feature_id: str) -> int:
    del feature_id
    return logging.INFO


_UNSET = object()


def _now_utc() -> datetime:
    return datetime.now(UTC)


def _beacon_refresh_interval(
    *,
    ttl_seconds: int | float,
) -> float:
    ttl = float(ttl_seconds)
    upper = ttl * 0.75
    lower = ttl * 0.5
    return random.uniform(lower, upper)


def _beacon_ttl_seconds(value: float | int | None, *, bucket: str) -> int:
    if value is None:
        raise KvUnavailable(f"Beacon advertisement bucket {bucket!r} must be TTL-bound")
    ttl = float(value)
    rounded = int(ttl)
    if ttl <= 0 or abs(ttl - rounded) > 0.001:
        raise KvUnavailable(
            f"Beacon advertisement bucket {bucket!r} TTL must be positive whole seconds"
        )
    return rounded
