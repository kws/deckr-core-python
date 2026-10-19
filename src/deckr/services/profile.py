"""Deckr service-use admission policy layered over generic Concord records."""

from deckr.concord import ContractRecord
from deckr.contracts.messages import EndpointAddress


def is_service_use_record(
    record: ContractRecord, *, profile: str, service: EndpointAddress
) -> bool:
    """Require the two-party, term-free service-use wire profile."""
    return (
        record.profile == profile
        and len(record.participants) == 2
        and service in record.participants
        and record.terms is None
        and record.terms_hash is None
        and not ({"terms", "terms_hash"} & record.model_fields_set)
    )
