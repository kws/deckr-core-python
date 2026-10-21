"""Explicit authority stub for isolated service handler tests.

This does not implement Concord. Tests for agreement creation, revocation and
participant-token validity must use the real managed provider and Concord.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import anyio

from deckr.contracts.messages import entity_subject
from deckr.services import ServiceRequestContext, service_message


class ServiceAuthorityStub:
    def __init__(self, store=None):
        self.store = store
        self.authorizations = {}
        self.cancelled = []
        self.closed = False
        self._changed = anyio.Event()

    def request(self, body, authorization, *, sender=None, session=None):
        context = authorization.view_write_context
        authorization = replace(authorization, body=body)
        self.authorizations.setdefault(context.contract, authorization)
        message = service_message(
            sender=sender or context.consumer_endpoint,
            sender_session_id=session or context.consumer_session_id,
            recipient=context.service_endpoint,
            recipient_session_id=context.service_session_id,
            subject=entity_subject(
                "service",
                serviceId=context.service_id,
                namespace=body.service_namespace,
                name=body.name,
            ),
            contract=context.contract,
            body=body,
        )
        return ServiceRequestContext(self, message, authorization)

    def contract(self, contract):
        pointer = getattr(contract, "pointer", None)
        if pointer is None:
            from deckr.contracts.authority import ContractPointer

            pointer = ContractPointer(
                contractId=contract.contract_id, generation=contract.generation
            )
        authorization = self.authorizations.get(pointer)
        if authorization is None or self.closed:
            return None
        context = authorization.view_write_context

        class Access:
            def restricted(_self, check):
                _self.check = check
                return _self

            async def put(_self, view, payload):
                await _self.check()
                result = await self.store.put(
                    view=view, payload=payload, context=context
                )
                return result or SimpleNamespace(revision=1)

        def view_access(store, *, require_current=None):
            return Access().restricted(require_current)

        return SimpleNamespace(view_access=view_access)

    async def authorize(self, message):
        return self.authorizations[message.contract]

    @asynccontextmanager
    async def watch_state(self):
        async def states():
            while True:
                changed = self._changed
                yield SimpleNamespace(contracts=tuple(self.authorizations))
                await changed.wait()

        yield states()

    async def cancel_authorized(self, authorization, *, reason=None):
        self.cancelled.append((authorization, reason))
        self.authorizations.pop(authorization.view_write_context.contract, None)
        changed, self._changed = self._changed, anyio.Event()
        changed.set()
        return True

    async def aclose(self):
        self.closed = True
        self.authorizations.clear()
        self._changed.set()
