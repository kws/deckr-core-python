"""Local service-use authority checks over a managed Concord agreement."""

from __future__ import annotations

from dataclasses import dataclass

from deckr.concord import (
    ConcordAgreementLease,
    ConcordLifecyclePhase,
    ContractHandle,
    ContractValidityStatus,
)
from deckr.contracts.messages import EndpointAddress
from deckr.services._validation import _require_text
from deckr.services.discovery import ServiceDescriptor
from deckr.services.errors import ServiceUnavailable


@dataclass(slots=True)
class ServiceUseLease:
    agreement: ConcordAgreementLease
    descriptor: ServiceDescriptor
    consumer_endpoint: EndpointAddress
    consumer_session_id: str

    def __post_init__(self) -> None:
        self.consumer_session_id = _require_text(
            self.consumer_session_id,
            field_name="consumer session id",
        )

    @property
    def contract(self) -> ContractHandle:
        return self.agreement.contract

    async def require_authority(self) -> None:
        state = self.agreement.state
        validity = state.validity
        if validity is None:
            raise ServiceUnavailable(
                "contract_starting",
                "Service-use agreement has not reached current state",
            )
        if state.valid:
            return
        if (
            state.phase == ConcordLifecyclePhase.UNAVAILABLE
            or validity.status == ContractValidityStatus.UNAVAILABLE
            or not state.current
        ):
            code = "contract_unavailable"
        elif validity.status != ContractValidityStatus.VALID:
            code = f"contract_{validity.status.value}"
        else:
            code = "contract_local_authority_lost"
        raise ServiceUnavailable(
            code,
            "Service-use agreement has no current local authority",
            {
                "status": validity.status.value,
                "reason": validity.reason,
                "phase": state.phase.value,
                "current": state.current,
                "localAuthority": state.local_authority,
                "contractId": self.contract.contract_id,
                "generation": self.contract.generation,
                "profile": self.contract.profile,
                "serviceId": self.descriptor.service_id,
                "serviceSessionId": self.descriptor.session_id,
            },
        )

    async def refresh(self) -> None:
        """Compatibility alias; this checks authority and does not renew tokens."""
        await self.require_authority()
