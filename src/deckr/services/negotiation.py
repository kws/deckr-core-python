"""ServiceUseNegotiator."""

from __future__ import annotations

import anyio

from deckr.concord import (
    Concord,
    ConcordAgreementSpec,
    ConcordConflict,
    ConcordLifecyclePhase,
    ConcordUnavailable,
    ContractValidityStatus,
)
from deckr.lanes import EndpointSession
from deckr.services._client_helpers import _service_use_conflict
from deckr.services.runtime import (
    ServiceDescriptor,
    ServiceUnavailable,
    ServiceUseLease,
    service_use_negotiation_terminal_status,
)


class ServiceUseNegotiator:
    def __init__(self, endpoint: EndpointSession, concord: Concord) -> None:
        self._endpoint = endpoint
        self._concord = concord

    async def _propose_service_use(
        self,
        descriptor: ServiceDescriptor,
        *,
        timeout_seconds: float | None,
    ) -> ServiceUseLease:
        agreement = None

        async def wait_for_valid() -> ServiceUseLease:
            nonlocal agreement
            try:
                agreement = self._concord.propose(
                    self._endpoint,
                    ConcordAgreementSpec(
                        participants=(self._endpoint.address, descriptor.endpoint),
                        profile=descriptor.use_profile,
                        terms=None,
                        participant_sessions={
                            str(descriptor.endpoint): descriptor.session_id,
                        },
                    ),
                )
                await agreement.__aenter__()
            except ConcordConflict as exc:
                raise _service_use_conflict(
                    descriptor,
                    exc,
                    agreement=None,
                ) from exc
            except ConcordUnavailable as exc:
                raise ServiceUnavailable(
                    "contract_unavailable",
                    "Concord authority is unavailable during service-use setup",
                    {
                        "reason": str(exc),
                        "profile": descriptor.use_profile,
                        "serviceId": descriptor.service_id,
                        "serviceSessionId": descriptor.session_id,
                    },
                ) from exc
            except (anyio.BrokenResourceError, anyio.ClosedResourceError) as exc:
                raise ServiceUnavailable(
                    "service_use_closed",
                    "Concord authority closed during service-use setup",
                    {
                        "profile": descriptor.use_profile,
                        "serviceId": descriptor.service_id,
                        "serviceSessionId": descriptor.session_id,
                    },
                ) from exc
            try:
                state = await agreement.wait_for_state(
                    lambda current: current.valid
                    or current.phase
                    in {
                        ConcordLifecyclePhase.UNAVAILABLE,
                        ConcordLifecyclePhase.TERMINAL,
                        ConcordLifecyclePhase.CLOSED,
                    }
                    or (
                        current.validity is not None
                        and service_use_negotiation_terminal_status(
                            current.validity.status
                        )
                    )
                )
            except (anyio.BrokenResourceError, anyio.ClosedResourceError) as exc:
                raise ServiceUnavailable(
                    "service_use_closed",
                    "Concord agreement closed during service-use setup",
                    {
                        "profile": descriptor.use_profile,
                        "serviceId": descriptor.service_id,
                        "serviceSessionId": descriptor.session_id,
                    },
                ) from exc
            validity = state.validity
            if state.valid and validity is not None:
                return ServiceUseLease(
                    agreement=agreement,
                    descriptor=descriptor,
                    consumer_endpoint=self._endpoint.address,
                    consumer_session_id=self._endpoint.session_id,
                )
            contract = state.contract
            if validity is None:
                raise ServiceUnavailable(
                    "service_use_conflict",
                    state.reason or "Service-use agreement could not acquire authority",
                    {
                        "contractId": (
                            contract.contract_id if contract is not None else None
                        ),
                        "generation": (
                            contract.generation if contract is not None else None
                        ),
                        "profile": descriptor.use_profile,
                        "serviceId": descriptor.service_id,
                        "serviceSessionId": descriptor.session_id,
                    },
                )
            if (
                state.phase == ConcordLifecyclePhase.UNAVAILABLE
                or validity.status == ContractValidityStatus.UNAVAILABLE
                or not state.current
            ):
                code = "contract_unavailable"
                message = "Service-use authority became unavailable during setup"
            elif validity.status != ContractValidityStatus.VALID:
                code = f"contract_{validity.status.value}"
                message = "Service-use contract became terminal during negotiation"
            else:
                code = "contract_local_authority_lost"
                message = "Service-use agreement lost local authority during setup"
            raise ServiceUnavailable(
                code,
                message,
                {
                    "status": validity.status.value,
                    "reason": validity.reason,
                    "phase": state.phase.value,
                    "current": state.current,
                    "localAuthority": state.local_authority,
                    "contractId": (
                        contract.contract_id if contract is not None else None
                    ),
                    "generation": contract.generation if contract is not None else None,
                    "profile": descriptor.use_profile,
                    "serviceId": descriptor.service_id,
                    "serviceSessionId": descriptor.session_id,
                },
            )

        try:
            if timeout_seconds is None:
                return await wait_for_valid()
            with anyio.fail_after(timeout_seconds):
                return await wait_for_valid()
        except TimeoutError as exc:
            contract_id = None
            generation = None
            if agreement is not None:
                contract = agreement.state.contract
                if contract is not None:
                    contract_id = contract.contract_id
                    generation = contract.generation
                    try:
                        await agreement.cancel("contract_timeout")
                    except ConcordConflict:
                        pass
                await agreement.aclose()
            raise ServiceUnavailable(
                "contract_timeout",
                "Timed out waiting for service-use contract to become valid",
                {
                    "contractId": contract_id,
                    "generation": generation,
                    "profile": descriptor.use_profile,
                    "serviceId": descriptor.service_id,
                    "serviceSessionId": descriptor.session_id,
                },
            ) from exc
        except BaseException:
            if agreement is not None:
                with anyio.CancelScope(shield=True):
                    await agreement.aclose()
            raise
