"""Compatibility exports for the Deckr-specific NATS lane adapter."""

from deckr.bindings.nats import (
    DEFAULT_NATS_RECONNECT_MAX_INTERVAL_SECONDS as DEFAULT_NATS_RECONNECT_MAX_INTERVAL_SECONDS,
    DeckrNatsSubstrate as NatsSubstrate,
    _headers_for as _headers_for,
    _subject_for as _subject_for,
)

__all__ = ["NatsSubstrate", "DEFAULT_NATS_RECONNECT_MAX_INTERVAL_SECONDS"]
