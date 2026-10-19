"""Deckr wire-model vocabulary built on the common JSON representation."""

from deckr.core.wire import (
    JsonObject as JsonObject,
    WireModel as DeckrModel,
    freeze_json as freeze_json,
    thaw_json as thaw_json,
    to_camel as to_camel,
)

__all__ = ["DeckrModel", "JsonObject", "freeze_json", "thaw_json", "to_camel"]
