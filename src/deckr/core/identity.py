"""Protocol identities; application families and reserved IDs are extension policy."""

from __future__ import annotations

import re

from pydantic import ConfigDict, Field, RootModel, field_validator

from deckr.core.wire import WireModel


def _require_identity_part(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    if value.strip() != value:
        raise ValueError(
            f"{field_name} must not contain leading or trailing whitespace"
        )
    if not value.strip():
        raise ValueError(f"{field_name} must not be whitespace")
    return value


class EndpointAddress(RootModel[str]):
    """An application-independent endpoint address serialized as ``<family>:<endpoint_id>``."""

    model_config = ConfigDict(frozen=True)

    @field_validator("root")
    @classmethod
    def _validate_root(cls, value: str) -> str:
        if not isinstance(value, str):
            raise TypeError("Endpoint address must be a string")
        if value.strip() != value:
            raise ValueError(
                "Endpoint address must not contain leading or trailing whitespace"
            )
        family, sep, endpoint_id = value.partition(":")
        if not sep:
            raise ValueError(
                "Endpoint address must have shape '<endpoint_family>:<endpoint_id>'"
            )
        if re.fullmatch(r"[a-z][a-z0-9_-]*", family) is None:
            raise ValueError("Invalid endpoint family")
        _require_identity_part(endpoint_id, field_name="Endpoint id")
        if ":" in endpoint_id:
            raise ValueError("Endpoint id must not contain ':'")
        return value

    @property
    def family(self) -> str:
        return self.root.split(":", 1)[0]

    @property
    def endpoint_id(self) -> str:
        return self.root.split(":", 1)[1]

    def __eq__(self, other: object) -> bool:
        return isinstance(other, EndpointAddress) and self.root == other.root

    def __hash__(self) -> int:
        return hash(self.root)

    def __str__(self) -> str:
        return self.root


def endpoint_address(endpoint_family: str, endpoint_id: str) -> EndpointAddress:
    return EndpointAddress.model_validate(f"{endpoint_family}:{endpoint_id}")


def parse_endpoint_address(address: str | EndpointAddress) -> EndpointAddress:
    return (
        address
        if isinstance(address, EndpointAddress)
        else EndpointAddress.model_validate(address)
    )


def _require_text(value: str, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    if value.strip() != value:
        raise ValueError(
            f"{field_name} must not contain leading or trailing whitespace"
        )
    if not value:
        raise ValueError(f"{field_name} must not be empty")
    return value


class ContractPointer(WireModel):
    contract_id: str = Field(alias="contractId")
    generation: int

    @field_validator("contract_id")
    @classmethod
    def _validate_contract_id(cls, value: str) -> str:
        return _require_text(value, field_name="contract id")

    @field_validator("generation")
    @classmethod
    def _validate_generation(cls, value: int) -> int:
        if value < 1:
            raise ValueError("generation must be greater than zero")
        return value
