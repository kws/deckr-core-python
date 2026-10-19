"""Lane policy."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from deckr.components.manifest import ComponentInstanceSpec
from deckr.contracts.lanes import (
    CORE_LANE_CONTRACTS,
    MessageContract,
    MessageContractRegistry,
)
from deckr.contracts.messages import CORE_LANE_NAMES
from deckr.core.config import ConfigDocument

REMOVED_LANE_CONTRACT_FIELDS = frozenset(
    {
        "current_state",
        "delivery",
        "delivery_semantics",
        "mqtt",
        "remote_endpoints",
        "route_policy",
        "route_table",
        "routes",
        "state",
        "state_policy",
        "store_policy",
        "transport_route",
        "websocket",
    }
)


MESSAGE_CONTRACT_FIELDS = frozenset(
    {
        "allowed_recipient_families",
        "allowed_sender_families",
        "broadcast_targets",
        "default_broadcast_hop_limit",
        "message_types",
        "schema_id",
    }
)


def build_lane_contract_registry(
    instance_specs: Sequence[ComponentInstanceSpec],
    document: ConfigDocument,
) -> MessageContractRegistry:
    return _build_lane_contract_registry(instance_specs, document=document)


def _string_set(value: Any, *, field_name: str) -> frozenset[str]:
    if value is None:
        return frozenset()
    if isinstance(value, str) or not isinstance(value, Sequence):
        raise ValueError(f"{field_name} must be a list of strings")
    items: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item:
            raise ValueError(f"{field_name} must be a list of non-empty strings")
        items.append(item)
    return frozenset(items)


def _optional_string_set(value: Any, *, field_name: str) -> frozenset[str] | None:
    if value is None:
        return None
    return _string_set(value, field_name=field_name)


def _optional_int(value: Any, *, field_name: str) -> int | None:
    if value is None:
        return None
    if not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer")
    return value


def _broadcast_targets(value: Any, *, field_name: str) -> Mapping[str, str]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a table of scope = endpoint_family")
    targets: dict[str, str] = {}
    for scope, endpoint_family in value.items():
        if not isinstance(scope, str) or not scope:
            raise ValueError(f"{field_name} scopes must be non-empty strings")
        if not isinstance(endpoint_family, str) or not endpoint_family:
            raise ValueError(
                f"{field_name} endpoint families must be non-empty strings"
            )
        targets[scope] = endpoint_family
    return targets


def _lane_contract_from_mapping(
    lane: str, source: Mapping[str, Any]
) -> MessageContract:
    schema_id = source.get("schema_id")
    if schema_id is not None and not isinstance(schema_id, str):
        raise ValueError(f"Lane contract {lane!r} schema_id must be a string")
    removed = sorted(key for key in REMOVED_LANE_CONTRACT_FIELDS if key in source)
    if removed:
        keys = ", ".join(removed)
        raise ValueError(
            f"Lane contract {lane!r} removed field(s) {keys} are not part of "
            "the v1 message contract"
        )
    unknown = sorted(set(source) - MESSAGE_CONTRACT_FIELDS)
    if unknown:
        keys = ", ".join(unknown)
        raise ValueError(
            f"Lane contract {lane!r} unknown field(s) {keys} are not part of "
            "the v1 message contract"
        )
    return MessageContract(
        lane=lane,
        schema_id=schema_id,
        message_types=_string_set(
            source.get("message_types"),
            field_name=f"Lane contract {lane!r} message_types",
        ),
        allowed_sender_families=_optional_string_set(
            source.get("allowed_sender_families"),
            field_name="allowed_sender_families",
        ),
        allowed_recipient_families=_optional_string_set(
            source.get("allowed_recipient_families"),
            field_name="allowed_recipient_families",
        ),
        broadcast_targets=_broadcast_targets(
            source.get("broadcast_targets"),
            field_name="broadcast_targets",
        ),
        default_broadcast_hop_limit=_optional_int(
            source.get("default_broadcast_hop_limit"),
            field_name="default_broadcast_hop_limit",
        ),
    )


def _narrow_families(
    base: frozenset[str] | None,
    override: frozenset[str] | None,
    *,
    field_name: str,
    lane: str,
) -> frozenset[str] | None:
    if override is None:
        return base
    if base is not None and not override.issubset(base):
        raise ValueError(
            f"Deployment lane contract {lane!r} must not widen {field_name}"
        )
    return override


def _narrow_broadcast_targets(
    base: Mapping[str, str],
    override: Mapping[str, str],
    *,
    lane: str,
) -> Mapping[str, str]:
    if not override:
        return base
    for scope, endpoint_family in override.items():
        if base.get(scope) != endpoint_family:
            raise ValueError(
                f"Deployment lane contract {lane!r} must not widen broadcast_targets"
            )
    return override


def _narrow_default_broadcast_hop_limit(
    base: int | None,
    override: int | None,
    *,
    lane: str,
) -> int | None:
    if override is None:
        return base
    if base is not None and override > base:
        raise ValueError(
            f"Deployment lane contract {lane!r} must not widen default_broadcast_hop_limit"
        )
    return override


def _narrow_message_types(
    base: frozenset[str],
    override: frozenset[str],
    *,
    lane: str,
) -> frozenset[str]:
    if not override:
        return base
    if base and not override.issubset(base):
        raise ValueError(
            f"Deployment lane contract {lane!r} must not widen message_types"
        )
    return override


def _narrow_lane_contract(
    base: MessageContract, override: MessageContract
) -> MessageContract:
    if override.schema_id is not None and base.schema_id not in {
        None,
        override.schema_id,
    }:
        raise ValueError(
            f"Deployment lane contract {base.lane!r} must not change schema_id"
        )
    return MessageContract(
        lane=base.lane,
        schema_id=base.schema_id or override.schema_id,
        message_types=_narrow_message_types(
            base.message_types,
            override.message_types,
            lane=base.lane,
        ),
        allowed_sender_families=_narrow_families(
            base.allowed_sender_families,
            override.allowed_sender_families,
            field_name="allowed_sender_families",
            lane=base.lane,
        ),
        allowed_recipient_families=_narrow_families(
            base.allowed_recipient_families,
            override.allowed_recipient_families,
            field_name="allowed_recipient_families",
            lane=base.lane,
        ),
        broadcast_targets=_narrow_broadcast_targets(
            base.broadcast_targets,
            override.broadcast_targets,
            lane=base.lane,
        ),
        default_broadcast_hop_limit=_narrow_default_broadcast_hop_limit(
            base.default_broadcast_hop_limit,
            override.default_broadcast_hop_limit,
            lane=base.lane,
        ),
    )


def _build_lane_contract_registry(
    instance_specs: Sequence[ComponentInstanceSpec],
    *,
    document: ConfigDocument | None = None,
    extra_contracts: MessageContractRegistry | Sequence[MessageContract] | None = None,
) -> MessageContractRegistry:
    contracts: dict[str, MessageContract] = dict(CORE_LANE_CONTRACTS)
    if isinstance(extra_contracts, MessageContractRegistry):
        provided_contracts = tuple(extra_contracts.contracts.values())
    else:
        provided_contracts = tuple(extra_contracts or ())

    for contract in provided_contracts:
        if contract.lane in CORE_LANE_CONTRACTS:
            if CORE_LANE_CONTRACTS[contract.lane] != contract:
                raise ValueError(
                    f"Extension lane contract input must not override "
                    f"core lane contract {contract.lane!r}"
                )
            continue
        existing = contracts.get(contract.lane)
        if existing is not None and existing != contract:
            raise ValueError(
                f"Duplicate lane contract {contract.lane!r} declarations "
                "must be identical"
            )
        contracts[contract.lane] = existing or contract

    for spec in instance_specs:
        for contract in spec.definition.manifest.lane_contracts:
            if contract.lane in CORE_LANE_CONTRACTS:
                raise ValueError(
                    f"Component {spec.component_id!r} must not override "
                    f"core lane contract {contract.lane!r}"
                )
            existing = contracts.get(contract.lane)
            if existing is not None and existing != contract:
                raise ValueError(
                    f"Duplicate lane contract {contract.lane!r} declarations "
                    "must be identical"
                )
            contracts[contract.lane] = existing or contract

    if document is not None:
        for lane, source in sorted(document.children("deckr.lane_contracts").items()):
            if lane in CORE_LANE_CONTRACTS:
                raise ValueError(
                    f"Deployment config must not override core lane contract {lane!r}"
                )
            contract = _lane_contract_from_mapping(lane, source)
            existing = contracts.get(lane)
            contracts[lane] = (
                _narrow_lane_contract(existing, contract)
                if existing is not None
                else contract
            )
    return MessageContractRegistry(contracts.values())


def _lane_names_for_specs(
    instance_specs: Sequence[ComponentInstanceSpec],
    *,
    lane_contracts: MessageContractRegistry | None = None,
) -> tuple[str, ...]:
    lane_names: set[str] = set(CORE_LANE_NAMES)
    for spec in instance_specs:
        lane_names.update(spec.lanes.consumes)
        lane_names.update(spec.lanes.publishes)
    if lane_contracts is not None:
        lane_names.update(
            lane for lane in lane_contracts.contracts if lane not in CORE_LANE_CONTRACTS
        )
    return tuple(sorted(lane_names))


def _validate_component_lane_bindings(
    specs: Sequence[ComponentInstanceSpec],
    lane_contracts: MessageContractRegistry,
) -> None:
    for spec in specs:
        spec.definition.validate_resolved_lane_bindings(
            config=spec.config,
            endpoints=spec.endpoints,
            instance_id=spec.instance_id,
            lane_contracts=lane_contracts,
        )
