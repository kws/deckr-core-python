"""NATS construction convenience for the transport-neutral materialized source."""

from __future__ import annotations

from typing import Any

from deckr.core.kv import JsonKvBucket, KvBucketPolicy
from deckr.core.kv_source import KvMaterializedSource
from deckr.substrates._nats_kv_bucket import NatsJsonKvBucket


class NatsKvMaterializedBucket(KvMaterializedSource):
    """Compatibility constructor; currentness is owned by KvMaterializedSource."""

    def __init__(
        self,
        *,
        js: Any = None,
        bucket: str | JsonKvBucket | None = None,
        policy: KvBucketPolicy | None = None,
        key_prefix: str = "",
    ) -> None:
        if isinstance(bucket, str):
            if js is None:
                raise ValueError("js is required when bucket is a name")
            raw = NatsJsonKvBucket(
                js=js, policy=policy or KvBucketPolicy(bucket=bucket, ttl_seconds=None)
            )
        elif bucket is not None:
            raw = bucket
        else:
            if js is None or policy is None:
                raise ValueError("bucket or js+policy is required")
            raw = NatsJsonKvBucket(js=js, policy=policy)
        super().__init__(bucket=raw, key_prefix=key_prefix)
