"""View records."""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from deckr.contracts.authority import ContractPointer
from deckr.contracts.keys import encode_key_token
from deckr.contracts.models import freeze_json, thaw_json
from deckr.core.kv import KvEntry
from deckr.services.runtime import (
    ServiceViewReadContext,
    ServiceViewRef,
    ServiceViewWriteContext,
    ServiceViewWriter,
)

logger = logging.getLogger(__name__)


_HASH_SIZE = 12


@dataclass(frozen=True, slots=True)
class ServiceViewEntry:
    bucket: str
    storage_key: str
    key: str
    value: Mapping[str, Any]
    revision: int
    service_id: str
    service_namespace: str
    service_endpoint: str
    service_session_id: str
    consumer_endpoint: str
    consumer_session_id: str
    writer: ServiceViewWriter
    contract: ContractPointer


@dataclass(frozen=True, slots=True)
class ServiceViewWatchSnapshot:
    version: int
    current: bool
    entry: ServiceViewEntry | None


@dataclass(frozen=True, slots=True)
class ServiceViewWatchChange:
    version: int
    current: bool
    entry: ServiceViewEntry | None
    resnapshot_required: bool


@dataclass(frozen=True, slots=True)
class _ServiceViewLeaseFence:
    service_id: str
    service_namespace: str
    service_endpoint: str
    service_session_id: str
    consumer_endpoint: str
    consumer_session_id: str
    contract: ContractPointer


def _fenced_payload(
    payload: Mapping[str, Any],
    *,
    view_key: str,
    context: ServiceViewWriteContext,
) -> Mapping[str, Any]:
    return freeze_json(
        {
            **thaw_json(payload),
            "viewKey": view_key,
            "serviceId": context.service_id,
            "serviceNamespace": context.service_namespace,
            "serviceEndpoint": str(context.service_endpoint),
            "serviceSessionId": context.service_session_id,
            "consumerEndpoint": str(context.consumer_endpoint),
            "consumerSessionId": context.consumer_session_id,
            "writer": context.writer.value,
            "contractId": context.contract.contract_id,
            "generation": context.contract.generation,
        }
    )


def _payload_hash(payload: Mapping[str, Any]) -> str:
    value = json.dumps(
        thaw_json(payload),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:_HASH_SIZE]


def _service_view_entry_from_kv(entry: KvEntry) -> ServiceViewEntry:
    value = freeze_json(dict(entry.value))
    key = _required_value(value, "viewKey")
    service_id = _required_value(value, "serviceId")
    service_namespace = _required_value(value, "serviceNamespace")
    service_endpoint = _required_value(value, "serviceEndpoint")
    service_session_id = _required_value(value, "serviceSessionId")
    consumer_endpoint = _required_value(value, "consumerEndpoint")
    consumer_session_id = _required_value(value, "consumerSessionId")
    writer = ServiceViewWriter(_required_value(value, "writer"))
    contract_id = _required_value(value, "contractId")
    generation = value.get("generation")
    if not isinstance(generation, int):
        raise ValueError("service view value requires generation")
    contract = ContractPointer(contractId=contract_id, generation=generation)
    return ServiceViewEntry(
        bucket=entry.bucket,
        storage_key=entry.key,
        key=key,
        value=value,
        revision=entry.revision,
        service_id=service_id,
        service_namespace=service_namespace,
        service_endpoint=service_endpoint,
        service_session_id=service_session_id,
        consumer_endpoint=consumer_endpoint,
        consumer_session_id=consumer_session_id,
        writer=writer,
        contract=contract,
    )


def _parse_service_view_entry(entry: KvEntry | None) -> ServiceViewEntry | None:
    if entry is None:
        return None
    try:
        return _service_view_entry_from_kv(entry)
    except (TypeError, ValueError):
        return None


def _required_value(value: Mapping[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"service view value requires {key}")
    return item


def _entry_matches_read_context(
    entry: ServiceViewEntry,
    context: ServiceViewReadContext,
) -> bool:
    return _entry_matches_fence(entry, _read_fence(context))


def _read_fence(context: ServiceViewReadContext) -> _ServiceViewLeaseFence:
    return _ServiceViewLeaseFence(
        service_id=context.service_id,
        service_namespace=context.service_namespace,
        service_endpoint=str(context.service_endpoint),
        service_session_id=context.service_session_id,
        consumer_endpoint=str(context.consumer_endpoint),
        consumer_session_id=context.consumer_session_id,
        contract=context.contract,
    )


def _entry_matches_fence(
    entry: ServiceViewEntry,
    fence: _ServiceViewLeaseFence,
) -> bool:
    return (
        entry.service_id == fence.service_id
        and entry.service_namespace == fence.service_namespace
        and entry.service_endpoint == fence.service_endpoint
        and entry.service_session_id == fence.service_session_id
        and entry.consumer_endpoint == fence.consumer_endpoint
        and entry.consumer_session_id == fence.consumer_session_id
        and entry.contract == fence.contract
    )


def _storage_key_for_context(
    view: ServiceViewRef,
    context: ServiceViewReadContext | ServiceViewWriteContext,
) -> str:
    return _storage_key(view.key, context.contract)


def _storage_key(logical_key: str, contract: ContractPointer) -> str:
    return (
        f"{logical_key}.contract.{encode_key_token(contract.contract_id)}."
        f"{contract.generation}"
    )


def _contract_pointer_from_handle(value: Any) -> ContractPointer:
    return ContractPointer(contractId=value.contract_id, generation=value.generation)


def _is_materialized_bucket(value: Any) -> bool:
    return all(
        hasattr(value, name)
        for name in (
            "start",
            "is_current",
            "wait_current",
            "exact_bucket",
            "get_cached",
            "items_cached",
            "revision_cached",
            "snapshot",
            "subscribe",
            "wait_for_revision",
        )
    )
