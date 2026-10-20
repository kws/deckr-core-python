"""JSON-lines driver for an external mixed-language lifecycle test."""

import asyncio
import json
import sys
from dataclasses import dataclass

import anyio

from deckr.bindings.nats import DeckrNatsSubstrate
from deckr.concord import (
    Concord,
    ConcordAgreementSpec,
    ConcordParticipantPolicy,
    ConcordParticipantSelection,
    canonical_json_hash,
)
from deckr.contracts.lanes import DEFAULT_MESSAGE_CONTRACT_REGISTRY
from deckr.core.identity import EndpointAddress, parse_endpoint_address
from deckr.core.kv import KvBucketPolicy


@dataclass(frozen=True)
class Endpoint:
    address: EndpointAddress = parse_endpoint_address("controller:python")
    session_id: str = "python-session"


def send(value):
    print(json.dumps(value), flush=True)


def status(contract, validity, authority):
    record = validity.contract if validity else None
    token = validity.tokens.get("controller:python") if validity else None
    return {
        "contractId": contract.contract_id,
        "generation": contract.generation,
        "authority": authority,
        "valid": validity.valid if validity else False,
        "termsHash": record.terms_hash if record else None,
        "calculatedHash": canonical_json_hash(record.terms) if record else None,
        "refreshSeq": token.refresh_seq if token else 0,
    }


async def main():
    url, prefix = sys.argv[1:]
    substrate = DeckrNatsSubstrate(
        url=url,
        lane_contracts=DEFAULT_MESSAGE_CONTRACT_REGISTRY,
        reconnect_max_interval_seconds=0.1,
    )
    await substrate.connect()
    concord = Concord(
        substrate.kv_bucket(KvBucketPolicy(prefix + "_contracts", None)),
        substrate.kv_bucket(
            KvBucketPolicy(prefix + "_tokens", 120, allow_write_ttl=True)
        ),
    )
    proposal = participant = None

    async def close_mode():
        nonlocal proposal, participant
        if proposal is not None:
            await proposal.aclose()
            proposal = None
        if participant is not None:
            await participant.aclose()
            participant = None

    async with anyio.create_task_group() as tasks:
        concord.start(tasks)
        await concord.wait_current()
        send({"ready": True})
        try:
            while line := await asyncio.to_thread(sys.stdin.readline):
                command = json.loads(line)
                try:
                    if command["op"] == "mode":
                        await close_mode()
                        if command["mode"] == "participant":
                            participant = concord.participant(
                                Endpoint(),
                                ConcordParticipantPolicy(
                                    "org.example.interop.v1",
                                    select=lambda candidates: ConcordParticipantSelection(
                                        [item.contract.pointer for item in candidates]
                                    ),
                                ),
                            )
                            await participant.__aenter__()
                        send({"ok": True})
                    elif command["op"] == "propose":
                        proposal = concord.propose(
                            Endpoint(),
                            ConcordAgreementSpec(
                                profile="org.example.interop.v1",
                                participants=[
                                    "controller:rust",
                                    "controller:python",
                                    "controller:typescript",
                                ],
                                terms=command["terms"],
                            ),
                        )
                        await proposal.__aenter__()
                        send({"contractId": proposal.state.contract.contract_id})
                    elif command["op"] == "status":
                        records = []
                        if proposal is not None:
                            state = proposal.state
                            if state.contract:
                                records.append(
                                    status(state.contract, state.validity, state.valid)
                                )
                        if participant is not None:
                            for item in participant.state.contracts:
                                records.append(
                                    status(
                                        item.contract,
                                        item.validity,
                                        item.local_authority,
                                    )
                                )
                        send({"current": concord.is_current(), "contracts": records})
                    elif command["op"] == "stop":
                        await close_mode()
                        send({"ok": True})
                        break
                    else:
                        raise ValueError("Unknown operation")
                except Exception as exc:
                    send({"error": str(exc)})
        finally:
            await close_mode()
            await concord.aclose()
            await substrate.aclose()
            tasks.cancel_scope.cancel()


if __name__ == "__main__":
    anyio.run(main)
