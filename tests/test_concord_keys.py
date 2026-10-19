from __future__ import annotations

from deckr.concord import (
    parse_concord_contract_key,
    parse_concord_participant_token_key,
)


def test_concord_key_parsers_reject_non_positive_generations() -> None:
    assert parse_concord_contract_key("contracts.hardware-contract.0.meta") is None
    assert parse_concord_contract_key("contracts.hardware-contract.-1.meta") is None
    assert (
        parse_concord_participant_token_key(
            "contracts.hardware-contract.0.participants.controller-main"
        )
        is None
    )
    assert (
        parse_concord_participant_token_key(
            "contracts.hardware-contract.-1.participants.controller-main"
        )
        is None
    )
