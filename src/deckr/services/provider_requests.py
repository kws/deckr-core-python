"""Bounded provider dispatch and authority for deferred backend operations."""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any

import anyio

from deckr.contracts.messages import DeckrMessage, message_is_expired
from deckr.core.util.operations import operation_guard
from deckr.services.authorization import AuthorizedServiceMessage
from deckr.services.errors import ServiceUnavailable, ServiceUseAuthorizationError
from deckr.services.messages import (
    SERVICE_MESSAGE,
    ServiceMessageBody,
    ServiceMessageStatus,
    service_body,
    service_reply,
)
from deckr.services.payload_validation import validate_service_payloads

if TYPE_CHECKING:
    from deckr.services.provider import ServiceProvider

logger = logging.getLogger(__name__)


class _InvalidHandlerReply(Exception):
    pass


ServiceHandler = Callable[["ServiceRequestContext"], Awaitable[ServiceMessageBody]]


class ServiceRequestContext:
    """One admitted request. Retained contexts never regain lost authority."""

    def __init__(
        self,
        provider: ServiceProvider,
        message: DeckrMessage,
        authorization: AuthorizedServiceMessage,
    ) -> None:
        self.provider = provider
        self.message = message
        self.authorization = authorization
        self.body = authorization.body
        self._guards: list[Callable[[], None]] = []
        self._closed = False
        self._failure: ServiceUseAuthorizationError | None = None

    def check(self) -> None:
        if self._failure is not None:
            raise self._failure
        try:
            if self._closed or message_is_expired(self.message):
                raise ServiceUseAuthorizationError(
                    "request_expired", "Request is no longer active"
                )
            if self.provider.contract(self.authorization.contract) is None:
                raise ServiceUseAuthorizationError(
                    "contract_local_authority_lost", "Service-use authority was lost"
                )
            for guard in self._guards:
                guard()
        except ServiceUseAuthorizationError as exc:
            self._failure = exc
            raise

    async def authorize(self) -> AuthorizedServiceMessage:
        self.check()
        return await self.provider.authorize(self.message)

    def restrict(self, guard: Callable[[], None]) -> None:
        self._guards.append(guard)
        self.check()

    def reply(
        self,
        result: Mapping[str, Any] | None = None,
        *,
        status: ServiceMessageStatus = ServiceMessageStatus.OK,
        code: str | None = None,
        message: str | None = None,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> ServiceMessageBody:
        return service_reply(
            self.body,
            result=result,
            status=status,
            code=code,
            message=message,
            diagnostics=diagnostics,
        )

    async def execute(
        self,
        handler: ServiceHandler,
        *,
        timeout_seconds: float,
    ) -> ServiceMessageBody:
        """Cancel suspended work on revocation and recheck before returning."""
        try:
            with anyio.fail_after(timeout_seconds):
                failure: BaseException | None = None
                async with anyio.create_task_group() as tasks:
                    with anyio.CancelScope() as scope:
                        tasks.start_soon(self._monitor, scope)
                        try:
                            await self.authorize()
                            with operation_guard(self.check):
                                result = await handler(self)
                            self.check()
                        except BaseException as exc:
                            failure = exc
                        finally:
                            tasks.cancel_scope.cancel()
                if self._failure is not None:
                    raise self._failure
                if failure is not None:
                    raise failure
            if self._failure is not None:
                raise self._failure
            return result
        finally:
            self._closed = True

    async def _monitor(self, scope: anyio.CancelScope) -> None:
        async with self.provider.watch_state() as states:
            async for _state in states:
                try:
                    self.check()
                except ServiceUseAuthorizationError:
                    scope.cancel()
                    return


async def serve_requests(
    provider: ServiceProvider,
    handler: ServiceHandler,
    *,
    timeout_seconds: float | Callable[[str], float] = 8.0,
    max_concurrency: int = 32,
) -> None:
    """Apply backpressure before spawning work; never accumulate waiting tasks."""
    if not 1 <= max_concurrency <= 256:
        raise ValueError("max_concurrency must be between 1 and 256")
    limiter = anyio.Semaphore(max_concurrency)

    async def dispatch(request) -> None:
        try:
            try:
                body = service_body(request.message)
            except (TypeError, ValueError):
                return
            try:
                if request.failure is not None:
                    raise request.failure
                context = ServiceRequestContext(
                    provider, request.message, request.authorization
                )
                timeout = (
                    timeout_seconds(body.name)
                    if callable(timeout_seconds)
                    else timeout_seconds
                )
                if timeout <= 0:
                    raise ValueError("request timeout must be positive")
                reply = await context.execute(handler, timeout_seconds=timeout)
                definition = provider.protocol.messages[body.name]
                if (
                    reply.name,
                    reply.service_namespace,
                    reply.intent,
                    reply.exchange_pattern,
                ) != (
                    body.name,
                    body.service_namespace,
                    definition.intent,
                    definition.exchange_pattern,
                ) or reply.status is None:
                    raise _InvalidHandlerReply(
                        "Handler returned an invalid service reply"
                    )
                try:
                    validate_service_payloads(definition, reply)
                except ValueError as exc:
                    raise _InvalidHandlerReply(str(exc)) from exc
            except _InvalidHandlerReply:
                logger.exception("Service handler returned an invalid reply")
                reply = service_reply(
                    body,
                    status=ServiceMessageStatus.ERROR,
                    code="invalid_service_reply",
                    message="Service handler returned an invalid reply",
                )
            except ServiceUseAuthorizationError as exc:
                reply = service_reply(
                    body,
                    status=ServiceMessageStatus.REJECTED,
                    code="service_use_contract_invalid",
                    message=exc.message,
                    diagnostics={"reason": exc.code, **exc.diagnostics},
                )
            except TimeoutError:
                reply = service_reply(
                    body,
                    status=ServiceMessageStatus.UNAVAILABLE,
                    code="service_command_timeout",
                    message="Service command timed out",
                )
            except ServiceUnavailable as exc:
                reply = service_reply(
                    body,
                    status=ServiceMessageStatus.UNAVAILABLE,
                    code=exc.code,
                    message=exc.message,
                    diagnostics=exc.diagnostics,
                )
            except ValueError as exc:
                reply = service_reply(
                    body,
                    status=ServiceMessageStatus.REJECTED,
                    code="invalid_params",
                    message=str(exc),
                )
            except Exception:
                logger.exception("Service handler failed: %s", body.name)
                reply = service_reply(
                    body,
                    status=ServiceMessageStatus.ERROR,
                    code="service_handler_failed",
                    message="Service handler failed",
                )
            await provider.endpoint.reply_to(
                request.message, message_type=SERVICE_MESSAGE, body=reply.to_dict()
            )
        except Exception:
            logger.exception("Could not send service reply")
        finally:
            limiter.release()

    async with anyio.create_task_group() as tasks:
        async for request in provider.requests():
            await limiter.acquire()
            tasks.start_soon(dispatch, request)
