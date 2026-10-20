"""Standalone regression checks; upstream vectors run in check_spec_conformance."""

import pytest

from deckr.action_runtime import action_runtime_payload
from deckr.actions.messages import (
    BindingCommandTarget,
    CapabilityRequirement,
    ClosePageBody,
    MatchedCapability,
    OpenPageBody,
    PageSessionCommandTarget,
    PageSessionMetadata,
)


def test_page_commands_roundtrip_explicit_targets_and_reject_old_bodies():
    target = PageSessionCommandTarget(
        kind="page_session", pageSessionId="page", generation=2
    )
    assert action_runtime_payload("close_page", ClosePageBody(target=target)) == (
        {"target": {"kind": "page_session", "pageSessionId": "page", "generation": 2}},
        None,
    )
    body = OpenPageBody(
        target=BindingCommandTarget(kind="binding", bindingId="binding", generation=4),
        descriptor={
            "pageId": "page",
            "bindings": [{"controlId": "button", "target": {"kind": "self"}}],
        },
    )
    assert action_runtime_payload("open_page", body)[0]["target"]["generation"] == 4
    for name, payload in [
        ("close_page", {}),
        ("open_page", {"descriptor": body.descriptor.to_dict()}),
        ("replace_page", body.to_dict()),
    ]:
        with pytest.raises(ValueError):
            action_runtime_payload(name, payload)
    for generation in [-1, 0.5, True, "2"]:
        with pytest.raises(ValueError):
            PageSessionCommandTarget(
                kind="page_session", pageSessionId="page", generation=generation
            )
    with pytest.raises(ValueError):
        PageSessionMetadata.model_validate(
            {
                "providerInstanceId": "provider",
                "providerId": "provider",
                "actionInstanceId": "action",
                "configId": "config",
                "pageId": "page",
                "pageSessionId": "session",
                "contextId": "context",
            }
        )


def test_matching_names_and_views_reject_duplicates():
    for update in [
        {"eventTypes": ["press", "press"]},
        {"commandTypes": ["clear", "clear"]},
        {"views": ["raw", "raw"]},
        {"preferences": [{"eventTypes": ["press", "press"]}]},
    ]:
        with pytest.raises(ValueError):
            CapabilityRequirement.model_validate(
                {
                    "name": "button",
                    "preferences": [{"family": "dev.deckr.input.button"}],
                    **update,
                }
            )
    with pytest.raises(ValueError):
        MatchedCapability.model_validate(
            {
                "capability": {
                    "deviceRef": {"managerId": "manager", "deviceId": "device"},
                    "controlId": "button",
                    "capabilityId": "button",
                },
                "family": "dev.deckr.input.button",
                "type": "activation",
                "direction": "input",
            }
        )
