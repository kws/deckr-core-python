"""Check Python behavior against the separate, CI-pinned Deckr specification."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from deckr.actions.settings import (
    SettingsTargetRef,
    parse_settings_target_key,
    settings_target_key,
)
from deckr.bindings.nats import _headers_for, _subject_for
from deckr.contracts.messages import DeckrMessage
from deckr.core.keys import decode_key_token, encode_key_token
from deckr.protocols.beacon import AdvertisementRecord, beacon_advertisement_key
from deckr.protocols.concord import (
    ContractRecord,
    ParticipantTokenRecord,
    canonical_json_bytes,
    canonical_json_hash,
    concord_contract_key,
    concord_participant_token_key,
)
from deckr.protocols.concord_maintenance import (
    ConcordStaleObservationRecord,
    ConcordTokenCleanupRecord,
)

SPEC_ROOT = Path(
    os.environ.get("DECKR_SPEC_ROOT", Path(__file__).resolve().parents[2] / "deckr")
).resolve()
CONTRACT_ROOT = SPEC_ROOT / "contract" / "v1"

PROTOCOL_MODELS = {
    "schemas/protocols/beacon/advertisement.v1.schema.json": AdvertisementRecord,
    "schemas/protocols/concord/contract.v1.schema.json": ContractRecord,
    "schemas/protocols/concord/participant-token.v1.schema.json": ParticipantTokenRecord,
    "schemas/protocols/concord/stale-observation.v1.schema.json": ConcordStaleObservationRecord,
    "schemas/protocols/concord/token-cleanup.v1.schema.json": ConcordTokenCleanupRecord,
}


def artifact(path: str) -> Any:
    source = CONTRACT_ROOT / path
    try:
        return json.loads(source.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"Cannot read Deckr specification artifact {source}: {exc}. "
            "Check out kws/deckr at ../deckr or set DECKR_SPEC_ROOT to its root. "
            "CI uses the commit in .deckr-spec-revision."
        ) from exc


def cases(name: str) -> list[dict[str, Any]]:
    values = artifact(f"vectors/{name}.v1.json")["cases"]
    if not values:
        raise ValueError(f"No conformance cases in {name}")
    return values


def equal(actual: Any, expected: Any, case: str) -> None:
    if actual != expected:
        raise ValueError(f"{case}: expected {expected!r}, got {actual!r}")


def check_vectors() -> int:
    count = 0
    for case in cases("key-tokens"):
        equal(encode_key_token(case["raw"]), case["token"], "key token encoding")
        equal(decode_key_token(case["token"]), case["raw"], "key token decoding")
        count += 1

    for case in cases("beacon-concord-keys"):
        value = case["input"]
        match case["helper"]:
            case "beacon_advertisement_key":
                key = beacon_advertisement_key(
                    feature_id=value["featureId"],
                    advertisement_id=value["advertisementId"],
                )
            case "concord_contract_key":
                key = concord_contract_key(
                    contract_id=value["contractId"], generation=value["generation"]
                )
            case "concord_participant_token_key":
                key = concord_participant_token_key(
                    contract_id=value["contractId"],
                    generation=value["generation"],
                    participant=value["participant"],
                )
            case other:
                raise ValueError(f"Unknown upstream key helper: {other}")
        equal(key, case["key"], case["id"])
        count += 1

    for case in cases("concord-terms-hash"):
        value = json.loads(case["inputJson"])
        equal(
            canonical_json_bytes(value).decode("utf-8"),
            case["canonicalJson"],
            case["id"],
        )
        equal(canonical_json_hash(value), case["hash"], case["id"])
        count += 1

    for case in cases("nats-lane"):
        message = DeckrMessage.from_wire(artifact(case["fixture"]))
        equal(_subject_for(message), case["subject"], case["id"])
        equal(_headers_for(message), case["headers"], case["id"])
        count += 1

    for case in cases("settings-target-keys"):
        target = SettingsTargetRef.from_wire(case["target"])
        equal(settings_target_key(target), case["key"], case["id"])
        equal(parse_settings_target_key(case["key"]), target, case["id"])
        count += 1
    return count


def check_protocol_fixtures() -> int:
    coverage: set[tuple[str, bool]] = set()
    count = 0
    for entry in artifact("manifest.json")["artifacts"]:
        schema = entry.get("schemaPath")
        if entry["kind"] != "fixture" or schema not in PROTOCOL_MODELS:
            continue
        value = artifact(entry["path"])
        try:
            PROTOCOL_MODELS[schema].from_wire(value)
        except ValueError:
            valid = False
        else:
            valid = True
        equal(valid, entry["valid"], entry["path"])
        coverage.add((schema, entry["valid"]))
        count += 1
    for schema in PROTOCOL_MODELS:
        for valid in (True, False):
            if (schema, valid) not in coverage:
                raise ValueError(
                    f"Missing fixture coverage for {schema}, valid={valid}"
                )
    return count


def main() -> None:
    vector_count = check_vectors()
    fixture_count = check_protocol_fixtures()
    print(
        f"Passed {vector_count} vector cases and {fixture_count} protocol fixtures "
        f"from {CONTRACT_ROOT}"
    )


if __name__ == "__main__":
    main()
