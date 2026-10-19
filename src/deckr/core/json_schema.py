"""Local JSON Schema validation with explicit dialect selection."""

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from jsonschema.validators import validator_for
from referencing import Registry


def local_validator(document):
    validator = (
        validator_for(document, default=None)
        if "$schema" in document
        else Draft202012Validator
    )
    if validator is None:
        raise ValueError("Unsupported JSON Schema dialect")
    try:
        validator.check_schema(document)
    except SchemaError as exc:
        raise ValueError(f"Invalid JSON Schema: {exc.message}") from exc
    # Embedded references can resolve locally; arbitrary network retrieval is
    # never part of processing an advertised descriptor or a wire message.
    return validator(document, registry=Registry())
