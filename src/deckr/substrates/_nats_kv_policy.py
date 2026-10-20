"""Nats kv policy."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from deckr.substrates._nats_kv_records import NATS_NANOSECONDS_PER_SECOND


def _duration_nanoseconds(seconds: float | None) -> int | None:
    if seconds is None:
        return None
    return int(float(seconds) * NATS_NANOSECONDS_PER_SECOND)


def _duration_seconds_equal(actual: float | None, expected: float | None) -> bool:
    if expected is None:
        return actual is None or actual == 0
    if actual is None:
        return False
    return abs(float(actual) - float(expected)) <= 0.001


def _raw_duration_nanoseconds(
    config: Mapping[str, Any] | None,
    field: str,
) -> int | None:
    if config is None:
        return None
    value = config.get(field)
    if value is None:
        return None
    return int(value)


def _resolved_stream_ttl_seconds(
    config: Any,
    *,
    raw_config: Mapping[str, Any] | None,
) -> float | None:
    raw_max_age = _raw_duration_nanoseconds(raw_config, "max_age")
    if raw_max_age is not None:
        if raw_max_age == 0:
            return None
        return raw_max_age / NATS_NANOSECONDS_PER_SECOND
    max_age = getattr(config, "max_age", None)
    if max_age is None or max_age == 0:
        return None
    return float(max_age)


async def _nats_stream_raw_config(js: Any, stream_name: str) -> Mapping[str, Any]:
    api_request = getattr(js, "_api_request", None)
    if api_request is None:
        raise RuntimeError("NATS JetStream context does not expose _api_request")
    prefix = getattr(js, "_prefix", "$JS.API")
    timeout = getattr(js, "_timeout", 5)
    response = await api_request(
        f"{prefix}.STREAM.INFO.{stream_name}",
        b"",
        timeout=timeout,
    )
    config = response.get("config") if isinstance(response, Mapping) else None
    if not isinstance(config, Mapping):
        raise RuntimeError(
            f"NATS stream {stream_name!r} response did not include config"
        )
    return config


async def _update_nats_stream_raw_config(
    js: Any,
    stream_name: str,
    config: Mapping[str, Any],
) -> None:
    api_request = getattr(js, "_api_request", None)
    if api_request is None:
        raise RuntimeError("NATS JetStream context does not expose _api_request")
    prefix = getattr(js, "_prefix", "$JS.API")
    timeout = getattr(js, "_timeout", 5)
    await api_request(
        f"{prefix}.STREAM.UPDATE.{stream_name}",
        json.dumps(config).encode("utf-8"),
        timeout=timeout,
    )


async def _create_nats_stream_raw_config(
    js: Any, stream_name: str, config: Mapping[str, Any]
) -> None:
    api_request = getattr(js, "_api_request", None)
    if api_request is None:
        raise RuntimeError("NATS JetStream context does not expose _api_request")
    prefix = getattr(js, "_prefix", "$JS.API")
    await api_request(
        f"{prefix}.STREAM.CREATE.{stream_name}",
        json.dumps(config).encode("utf-8"),
        timeout=getattr(js, "_timeout", 5),
    )
