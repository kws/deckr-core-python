import base64
from io import BytesIO

import pytest
from hardware_client_helpers import descriptor
from PIL import Image

from deckr.hardware import CapabilitySchema, HardwareUnavailable
from deckr.hardware.validation import validate_command, validate_input, validate_payload


def test_command_schema_union_cannot_mistake_empty_frame_for_clear():
    capability = descriptor().controls[0].output_capabilities[0]
    validate_command(capability, "clear", {})
    with pytest.raises(HardwareUnavailable):
        validate_command(capability, "set_frame", {})


def test_embedded_schema_references_work_and_remote_references_are_not_fetched():
    schema = CapabilitySchema(
        schema={
            "$defs": {"number": {"type": "number", "minimum": 0}},
            "$ref": "#/$defs/number",
        }
    )
    validate_payload(schema, 2)
    with pytest.raises(HardwareUnavailable):
        validate_payload(schema, -1)
    remote = CapabilitySchema(schema={"$ref": "https://example.invalid/payload.json"})
    with pytest.raises(HardwareUnavailable):
        validate_payload(remote, {})


def test_missing_descriptor_schema_fails_closed():
    with pytest.raises(HardwareUnavailable):
        validate_payload(None, {})


@pytest.mark.parametrize("kind", ["hardware", "service"])
@pytest.mark.parametrize(
    "document",
    [
        {"type": "nonexistent"},
        {"$schema": "https://example.invalid/schema", "type": "number"},
        {
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "number",
            "exclusiveMinimum": 1,
        },
    ],
)
def test_descriptors_reject_invalid_or_unknown_schema_dialects(kind, document):
    from pydantic import ValidationError

    from deckr.services import ServicePayloadSchema

    schema_type = CapabilitySchema if kind == "hardware" else ServicePayloadSchema
    with pytest.raises(ValidationError):
        schema_type(schemaId="org.example.value.v1", schema=document)


def test_declared_schema_dialect_controls_keyword_meaning():
    # Draft 4 uses boolean exclusiveMinimum; 2020-12 instead requires a number.
    schema = CapabilitySchema(
        schema={
            "$schema": "http://json-schema.org/draft-04/schema#",
            "type": "number",
            "minimum": 1,
            "exclusiveMinimum": True,
        }
    )
    validate_payload(schema, 2)
    with pytest.raises(HardwareUnavailable, match="minimum"):
        validate_payload(schema, 1)


def _frame(*, encoding="png", size=(72, 72)):
    stream = BytesIO()
    Image.new("RGB", size, color="red").save(stream, format=encoding)
    return {"image": base64.b64encode(stream.getvalue()).decode(), "encoding": encoding}


@pytest.mark.parametrize("encoding", ["png", "jpeg"])
def test_raster_accepts_decodable_frames_matching_device_dimensions(encoding):
    capability = descriptor().controls[0].output_capabilities[0]
    validate_command(capability, "set_frame", _frame(encoding=encoding))


@pytest.mark.parametrize(
    "case", ["base64", "not_image", "format", "size", "declared_size", "truncated"]
)
def test_raster_rejects_invalid_bytes_format_and_dimensions(case):
    capability = descriptor().controls[0].output_capabilities[0]
    params = _frame()
    if case == "base64":
        params["image"] = "not base64!"
    elif case == "not_image":
        params["image"] = base64.b64encode(b"not an image").decode()
    elif case == "format":
        params["encoding"] = "jpeg"
    elif case == "size":
        params = _frame(size=(71, 72))
    elif case == "declared_size":
        params = _frame(size=(71, 72)) | {"width": 72}
    else:
        params["image"] = base64.b64encode(
            base64.b64decode(params["image"])[:40]
        ).decode()
    with pytest.raises(HardwareUnavailable) as rejected:
        validate_command(capability, "set_frame", params)
    assert rejected.value.code == "invalid_capability_payload"


def test_core_input_semantics_apply_even_with_permissive_embedded_schema():
    from deckr.hardware import CapabilityDescriptor

    capability = CapabilityDescriptor(
        capabilityId="dial",
        family="dev.deckr.input.encoder",
        type="relative",
        direction="input",
        access=["emits"],
        eventTypes=["rotate"],
        valueSchema={"schema": {"type": "object"}},
        constraints=[
            {"type": "range", "subject": "delta", "minimum": -2, "maximum": 2}
        ],
    )
    validate_input(capability, "rotate", {"delta": 2, "direction": "clockwise"})
    for value in [
        {"delta": 0},
        {"delta": 1, "direction": "counterclockwise"},
        {"delta": 3},
    ]:
        with pytest.raises(HardwareUnavailable):
            validate_input(capability, "rotate", value)
