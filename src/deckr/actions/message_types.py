"""Message types."""

from __future__ import annotations

from deckr.actions._body import ActionExtensionBody, ActionMessageBody
from deckr.actions.lifecycle import (
    ActionInstanceLifecycleBody,
    ActionLifecycleRejectedBody,
    BindingAttachedBody,
    BindingDetachedBody,
    BindingOutputBody,
    BindingOverlayBody,
    BindingOverlayClearBody,
    CapabilityInputBody,
    PageSessionLifecycleBody,
)
from deckr.actions.pages import ClosePageBody, OpenPageBody, ReplacePageBody

ACTION_INSTANCE_CREATED = "actionInstanceCreated"


ACTION_INSTANCE_DESTROYED = "actionInstanceDestroyed"


BINDING_ATTACHED = "bindingAttached"


BINDING_DETACHED = "bindingDetached"


PAGE_SESSION_OPENED = "pageSessionOpened"


PAGE_SESSION_CLOSED = "pageSessionClosed"


ACTION_LIFECYCLE_REJECTED = "actionLifecycleRejected"


CAPABILITY_INPUT = "capabilityInput"


BINDING_OUTPUT = "bindingOutput"


BINDING_OVERLAY = "bindingOverlay"


BINDING_OVERLAY_CLEAR = "bindingOverlayClear"


OPEN_PAGE = "openPage"


REPLACE_PAGE = "replacePage"


CLOSE_PAGE = "closePage"


ACTION_EXTENSION = "actionExtension"


ACTION_PROVIDER_COMMAND_MESSAGE_TYPES = frozenset(
    {
        BINDING_OUTPUT,
        BINDING_OVERLAY,
        BINDING_OVERLAY_CLEAR,
    }
)


CONTROLLER_EXTENSION_COMMAND_MESSAGE_TYPES = frozenset(
    {
        ACTION_LIFECYCLE_REJECTED,
        OPEN_PAGE,
        REPLACE_PAGE,
        CLOSE_PAGE,
    }
)


COMMAND_MESSAGE_TYPES = (
    ACTION_PROVIDER_COMMAND_MESSAGE_TYPES | CONTROLLER_EXTENSION_COMMAND_MESSAGE_TYPES
)


ACTION_BODY_BY_MESSAGE_TYPE: dict[str, type[ActionMessageBody]] = {
    ACTION_INSTANCE_CREATED: ActionInstanceLifecycleBody,
    ACTION_INSTANCE_DESTROYED: ActionInstanceLifecycleBody,
    BINDING_ATTACHED: BindingAttachedBody,
    BINDING_DETACHED: BindingDetachedBody,
    PAGE_SESSION_OPENED: PageSessionLifecycleBody,
    PAGE_SESSION_CLOSED: PageSessionLifecycleBody,
    ACTION_LIFECYCLE_REJECTED: ActionLifecycleRejectedBody,
    CAPABILITY_INPUT: CapabilityInputBody,
    BINDING_OUTPUT: BindingOutputBody,
    BINDING_OVERLAY: BindingOverlayBody,
    BINDING_OVERLAY_CLEAR: BindingOverlayClearBody,
    OPEN_PAGE: OpenPageBody,
    REPLACE_PAGE: ReplacePageBody,
    CLOSE_PAGE: ClosePageBody,
    ACTION_EXTENSION: ActionExtensionBody,
}
