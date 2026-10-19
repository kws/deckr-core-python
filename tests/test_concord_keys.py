from __future__ import annotations

from deckr.concord import (
    concord_contract_key,
    concord_participant_token_key,
    parse_concord_contract_key,
    parse_concord_participant_token_key,
)


def test_concord_key_parsers_reject_non_positive_generations() -> None:
    contract_id = "hardware-contract"
    participant = "controller:main"
    for generation in (1, 0, -1):
        contract = parse_concord_contract_key(
            concord_contract_key(contract_id=contract_id, generation=generation)
        )
        token = parse_concord_participant_token_key(
            concord_participant_token_key(
                contract_id=contract_id, generation=generation, participant=participant
            )
        )
        if generation == 1:
            assert contract == (contract_id, generation)
            assert token is not None
            assert token[:2] == (contract_id, generation)
            assert str(token[2]) == participant
        else:
            assert contract is None
            assert token is None
