"""Optional transport adapters and local NATS server supervision."""

from importlib import import_module

_EXPORTS = {
    "NatsSubstrate": ("deckr.bindings.nats", "DeckrNatsSubstrate"),
    "NatsServerBinaryResolver": (
        "deckr.substrates.nats_binary",
        "NatsServerBinaryResolver",
    ),
    "NatsServerSupervisor": ("deckr.substrates.nats_server", "NatsServerSupervisor"),
    "SupervisedNatsSubstrate": (
        "deckr.substrates._supervised_substrate",
        "SupervisedNatsSubstrate",
    ),
}
__all__ = list(_EXPORTS)


def __getattr__(name: str):
    if name not in _EXPORTS:
        raise AttributeError(name)
    module, attribute = _EXPORTS[name]
    value = getattr(import_module(module), attribute)
    globals()[name] = value
    return value
