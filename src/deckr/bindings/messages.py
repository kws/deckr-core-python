"""Deckr lane body codecs installed by the application composition layer."""

from dataclasses import replace

from deckr.actions.messages import action_body
from deckr.contracts.lanes import MessageContract
from deckr.contracts.messages import ACTIONS_LANE, HARDWARE_MESSAGES_LANE, SERVICES_LANE
from deckr.hardware.messages import hardware_body_from_message
from deckr.services.messages import service_body

_BODY_CODECS = {
    ACTIONS_LANE: action_body,
    HARDWARE_MESSAGES_LANE: hardware_body_from_message,
    SERVICES_LANE: service_body,
}


def with_deckr_body_codec(contract: MessageContract) -> MessageContract:
    """Bind standard Deckr bodies while leaving extension codecs caller-owned."""
    codec = _BODY_CODECS.get(contract.lane)
    return replace(contract, body_validator=codec) if codec is not None else contract
