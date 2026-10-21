"""Declarative consumer adapter for services with full-replacement scopes."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from contextlib import asynccontextmanager
from typing import Any

from deckr.contracts.messages import service_address
from deckr.services._client import DeckrServices
from deckr.services.discovery import newest_service_descriptor
from deckr.services.errors import ServiceUnavailable, UnsupportedServiceScope
from deckr.services.messages import (
    ServiceError,
    ServiceMessageBody,
    ServiceMessageStatus,
    service_reply,
)
from deckr.services.protocol import (
    ServiceBackendStatus,
    ServiceProtocol,
    ServiceViewRef,
)
from deckr.services.subscription_manager import SharedResourceSubscriptionManager
from deckr.services.subscription_session import ResourceSubscriptionSession
from deckr.services.subscription_state import (
    ServiceSubscriptionMessage,
    ServiceSubscriptionState,
)
from deckr.services.view_context import service_view_key


class ResourceServiceClient:
    """Service-owned names and policy over the one managed subscription boundary."""

    def __init__(
        self,
        services: DeckrServices,
        *,
        protocol: ServiceProtocol,
        family: str,
        scope_operation: str,
        scope_parameter: str,
        resource_parameter: str,
        subscriber_prefix: str,
        resource_required: bool = False,
        request_timeout: float = 8,
        negotiation_timeout: float = 30,
    ) -> None:
        self.services = services
        self.protocol = protocol
        self.family = family
        self.scope_operation = scope_operation
        self.scope_parameter = scope_parameter
        self.resource_parameter = resource_parameter
        self.subscriber_prefix = subscriber_prefix
        self.resource_required = resource_required
        self.request_timeout = request_timeout
        self.negotiation_timeout = negotiation_timeout

    @asynccontextmanager
    async def subscription_session(
        self,
        service_id: str,
        *,
        resources: Collection[str] = (),
        timeout_seconds: float | None = None,
    ):
        timeout = (
            self.negotiation_timeout if timeout_seconds is None else timeout_seconds
        )
        subscriber = f"{self.subscriber_prefix}:{service_id}"
        key = (
            "service-resource-subscriptions",
            self.protocol.namespace,
            service_id,
            self.family,
            self.scope_operation,
            self.scope_parameter,
            self.request_timeout,
            subscriber,
            timeout,
        )

        def factory():
            return SharedResourceSubscriptionManager(
                self.services,
                name=f"{self.protocol.namespace}:{service_id}:{self.family}",
                descriptor=lambda remaining: self._descriptor(service_id, remaining),
                ensure_resources=None,
                release_resources=None,
                set_resources=lambda lease, selected: self._set_resources(
                    lease, subscriber, selected
                ),
                view_for_resource=lambda descriptor, resource: ServiceViewRef(
                    descriptor.views[self.family].store_name,
                    service_view_key(descriptor.service_id, self.family, resource),
                ),
                message_from_view=self._message_from_view,
                service_use_timeout_seconds=timeout,
            )

        manager = self.services.get_shared_manager(key, factory)
        session = ServiceResourceSession(
            self, await manager.open_session(resources), resources, subscriber
        )
        try:
            yield session
        finally:
            await session.aclose()

    async def _descriptor(self, service_id: str, timeout_seconds: float | None):
        def matches(descriptor):
            return (
                descriptor.service_id == service_id
                and descriptor.endpoint == service_address(service_id)
                and descriptor.backend_status != ServiceBackendStatus.UNAVAILABLE
            )

        try:
            return await self.services.wait_for_descriptor(
                self.protocol,
                matches,
                select=newest_service_descriptor,
                timeout_seconds=timeout_seconds,
                diagnostics={
                    "serviceId": service_id,
                    "serviceNamespace": self.protocol.namespace,
                },
            )
        except TimeoutError as exc:
            advertised = self.services.resolve_descriptor(
                self.protocol,
                lambda descriptor: descriptor.service_id == service_id
                and descriptor.endpoint == service_address(service_id),
                select=newest_service_descriptor,
            )
            if (
                advertised is not None
                and advertised.backend_status == ServiceBackendStatus.UNAVAILABLE
            ):
                raise ServiceUnavailable(
                    "service_backend_unavailable",
                    "Advertised backend is unavailable",
                    {"serviceId": service_id, **advertised.diagnostics},
                ) from exc
            raise ServiceUnavailable(
                "discovery_timeout",
                "Timed out waiting for service",
                {"serviceId": service_id, "serviceNamespace": self.protocol.namespace},
            ) from exc

    async def _set_resources(
        self, lease, subscriber: str, resources: frozenset[str]
    ) -> None:
        reply = await lease.request(
            self.scope_operation,
            {self.scope_parameter: sorted(resources), "subscriberId": subscriber},
            timeout_seconds=self.request_timeout,
        )
        if reply.status != ServiceMessageStatus.OK:
            error = reply.error or ServiceError(
                code="scope_request_failed", message="Scope request failed"
            )
            raise ServiceUnavailable(error.code, error.message, dict(error.diagnostics))

    def _message_from_view(self, resource: str, payload):
        return ServiceSubscriptionMessage(
            resource=resource,
            state=ServiceSubscriptionState.READY
            if payload is not None
            else ServiceSubscriptionState.UNAVAILABLE,
            payload=payload,
            error=None
            if payload is not None
            else ServiceError(
                code="resource_view_unavailable",
                message="Resource view is unavailable",
                diagnostics={self.resource_parameter: resource},
            ),
        )

    def reply(self, operation: str, *, status=ServiceMessageStatus.OK, **kwargs):
        definition = self.protocol.messages.get(operation)
        if definition is None:
            raise UnsupportedServiceScope(f"Unsupported service operation: {operation}")
        body = ServiceMessageBody(
            serviceNamespace=self.protocol.namespace,
            name=operation,
            intent=definition.intent,
            exchangePattern=definition.exchange_pattern,
        )
        return service_reply(body, status=status, **kwargs)


class ServiceResourceSession:
    def __init__(
        self,
        client: ResourceServiceClient,
        session: ResourceSubscriptionSession[str],
        resources: Collection[str],
        subscriber: str,
    ) -> None:
        self._client = client
        self._session = session
        self._resources = set(resources)
        self._subscriber = subscriber
        self._closed = False

    @property
    def messages(self):
        return self._session.messages

    async def set(self, resources: Collection[str]) -> ServiceMessageBody:
        if self._closed:
            return self._client.reply(
                self._client.scope_operation,
                status=ServiceMessageStatus.UNAVAILABLE,
                code="subscription_session_closed",
                message="Subscription session is closed",
            )
        selected = set(resources)
        await self._session.set(selected)
        self._resources = selected
        return self._client.reply(self._client.scope_operation)

    async def command(
        self,
        operation: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout_seconds: float | None = None,
    ) -> ServiceMessageBody:
        client = self._client
        # Resolve the definition before handling errors; unknown operations have no reply contract.
        client.reply(operation)
        if operation == client.scope_operation:
            return client.reply(
                operation,
                status=ServiceMessageStatus.REJECTED,
                code="subscription_method_required",
                message="Use the session set method",
            )
        try:
            if self._closed:
                raise ServiceUnavailable(
                    "subscription_session_closed", "Subscription session is closed"
                )
            values = dict(params or {})
            target = values.get(client.resource_parameter)
            if target is not None and (
                not isinstance(target, str) or not target.strip()
            ):
                raise ValueError("Resource must be a non-empty string")
            if client.resource_required and target is None:
                raise ValueError(f"{client.resource_parameter} is required")
            if target is not None and target not in self._resources:
                raise ServiceUnavailable(
                    "resource_not_in_scope", "Resource is not in this session scope"
                )
            if target is not None:
                values["subscriberId"] = self._subscriber
            return await self._session.request(
                operation,
                values,
                required_resource=target,
                timeout_seconds=client.request_timeout
                if timeout_seconds is None
                else timeout_seconds,
            )
        except ServiceUnavailable as exc:
            return client.reply(
                operation,
                status=ServiceMessageStatus.UNAVAILABLE,
                code=exc.code,
                message=exc.message,
                diagnostics=exc.diagnostics,
            )
        except UnsupportedServiceScope as exc:
            return client.reply(
                operation,
                status=ServiceMessageStatus.REJECTED,
                code="unsupported_operation",
                message=str(exc),
            )
        except ValueError as exc:
            return client.reply(
                operation,
                status=ServiceMessageStatus.REJECTED,
                code="invalid_params",
                message=str(exc),
            )
        except Exception as exc:
            return client.reply(
                operation,
                status=ServiceMessageStatus.UNAVAILABLE,
                code="service_message_failed",
                message="Service message failed",
                diagnostics={"reason": str(exc)},
            )

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            self._resources.clear()
            await self._session.aclose()
