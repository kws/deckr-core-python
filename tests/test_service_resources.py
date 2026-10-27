from copy import deepcopy

import pytest
from jsonschema.exceptions import ValidationError as SchemaValidationError
from pydantic import ValidationError

from deckr.services.resources import ResourceSnapshot, ValueSchema


def snapshot():
    return {
        "resourceId": "room.lamp",
        "revision": "session:1",
        "observedAt": "2026-10-27T12:00:00Z",
        "descriptor": {
            "resourceId": "room.lamp",
            "title": "Lamp",
            "profile": "light",
            "properties": {
                "power": {"valueSchema": {"type": "boolean"}, "writable": True}
            },
            "actions": {},
        },
        "properties": {"power": {"quality": "known", "value": False}},
    }


def test_typed_snapshot_keeps_false_and_round_trips():
    value = snapshot()
    assert ResourceSnapshot.model_validate(value).to_dict() == value


@pytest.mark.parametrize(
    "mutation",
    [
        lambda s: s["properties"]["power"].update(value="OFF"),
        lambda s: s["properties"]["power"].update(quality="unknown"),
        lambda s: s["properties"]["power"].pop("value"),
        lambda s: s["descriptor"].update(resourceId="other"),
        lambda s: s["properties"].clear(),
        lambda s: s.update(observedAt="2026-10-27T12:00:00"),
        lambda s: s.update(extra=True),
    ],
)
def test_incoherent_snapshots_fail(mutation):
    value = deepcopy(snapshot())
    mutation(value)
    with pytest.raises((ValueError, SchemaValidationError)):
        ResourceSnapshot.model_validate(value)


def test_structured_schema_preserves_boolean_and_numeric_types():
    schema = ValueSchema(
        type="object",
        properties={
            "power": ValueSchema(type="boolean"),
            "level": ValueSchema(type="number", minimum=0, maximum=100),
        },
        required=["power", "level"],
    )
    schema.validate_value({"power": False, "level": 0})
    for value in (
        {"power": "false", "level": 0},
        {"power": True, "level": True},
        {"power": False, "level": float("nan")},
        {"power": False, "level": 101},
        {"power": False, "level": 1, "target": "other"},
    ):
        with pytest.raises((ValueError, SchemaValidationError)):
            schema.validate_value(value)
    with pytest.raises(ValidationError):
        ValueSchema(type="array")
