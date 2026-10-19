from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock

import anyio
import pytest

from deckr._concord._lease import (
    ConcordParticipantLease,
    ConcordParticipantLeasePhase,
)
from deckr._concord._owner import ConcordOwnerBootstrap
from deckr.concord import (
    Concord,
    ConcordAgreementSpec,
    ConcordConflict,
    ConcordConflictCode,
    ConcordLifecyclePhase,
    ConcordParticipantPolicy,
    ConcordParticipantSelection,
    ContractState,
    concord_participant_token_key,
)
from deckr.contracts.authority import ContractPointer
from deckr.contracts.messages import controller_address, hardware_manager_address
from deckr.testing import (
    LifecycleFaultHarness,
    LifecycleFaultPhase,
    MemoryJsonKvBucket,
)

CONTROLLER = controller_address("lifecycle-guards")
MANAGER = hardware_manager_address("lifecycle-guards")
PROFILE = "dev.deckr.test.lifecycle-guards.v1"


@dataclass(frozen=True, slots=True)
class _Endpoint:
    address: object
    session_id: str


@asynccontextmanager
async def _running_concord(
    contracts: MemoryJsonKvBucket | None = None,
    tokens: MemoryJsonKvBucket | None = None,
) -> AsyncIterator[Concord]:
    concord = Concord(
        contracts or MemoryJsonKvBucket(bucket="contracts"),
        tokens or MemoryJsonKvBucket(bucket="tokens", ttl_seconds=60),
    )
    async with anyio.create_task_group() as tasks:
        concord.start(tasks)
        await concord.wait_current()
        try:
            yield concord
        finally:
            await concord.aclose()
            tasks.cancel_scope.cancel()


def _spec(**changes) -> ConcordAgreementSpec:
    values = {
        "profile": PROFILE,
        "participants": (CONTROLLER, MANAGER),
        "participant_sessions": {str(MANAGER): "manager-session"},
    }
    values.update(changes)
    return ConcordAgreementSpec(**values)


def _accept_all(candidates) -> ConcordParticipantSelection:
    return ConcordParticipantSelection(
        candidate.contract.pointer for candidate in candidates
    )


def test_agreement_intent_rejects_invalid_identity_and_session_shapes() -> None:
    with pytest.raises(ValueError, match="at least one"):
        ConcordAgreementSpec(profile=PROFILE, participants=())
    with pytest.raises(ValueError, match="unique"):
        ConcordAgreementSpec(
            profile=PROFILE,
            participants=(CONTROLLER, CONTROLLER),
        )
    with pytest.raises(ValueError, match="profile"):
        ConcordAgreementSpec(profile="", participants=(CONTROLLER,))
    with pytest.raises(ValueError, match="named participants"):
        _spec(participant_sessions={"service:not-a-participant": "session"})
    with pytest.raises(ValueError, match="session id"):
        _spec(participant_sessions={str(MANAGER): ""})

    pointer = ContractPointer(contractId="predecessor", generation=1)
    assert _spec(supersedes=pointer).supersedes is pointer
    assert _spec(supersedes=pointer.model_dump(by_alias=True)).supersedes == pointer
    assert _spec(created_by=CONTROLLER).created_by == CONTROLLER


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("endpoint", "spec", "message"),
    [
        (
            _Endpoint(controller_address("unnamed"), "session"),
            _spec(),
            "must be named",
        ),
        (
            _Endpoint(CONTROLLER, "controller-session"),
            _spec(created_by=MANAGER),
            "creator must match",
        ),
        (
            _Endpoint(CONTROLLER, "controller-session"),
            _spec(
                participant_sessions={
                    str(CONTROLLER): "different-session",
                    str(MANAGER): "manager-session",
                }
            ),
            "conflicts with the endpoint session",
        ),
    ],
)
async def test_failed_agreement_entry_is_closed_without_a_contract(
    endpoint: _Endpoint,
    spec: ConcordAgreementSpec,
    message: str,
) -> None:
    async with _running_concord() as concord:
        agreement = concord.propose(endpoint, spec)
        with pytest.raises(ValueError, match=message):
            await agreement.__aenter__()
        assert agreement.closed
        with pytest.raises(RuntimeError, match="has not been entered"):
            _ = agreement.contract


@pytest.mark.asyncio
async def test_agreement_and_participant_contexts_reject_reentry_and_closed_work() -> (
    None
):
    async with _running_concord() as concord:
        agreement = concord.propose(
            _Endpoint(CONTROLLER, "controller-session"),
            _spec(),
        )
        await agreement.__aenter__()
        with pytest.raises(RuntimeError, match="re-entered"):
            await agreement.__aenter__()
        await agreement.aclose()
        with pytest.raises(ConcordConflict) as agreement_error:
            await agreement.cancel("too-late")
        assert agreement_error.value.code == ConcordConflictCode.AGREEMENT_CLOSED

        participant = concord.participant(
            _Endpoint(MANAGER, "manager-session"),
            ConcordParticipantPolicy(profile=PROFILE, select=_accept_all),
        )
        await participant.__aenter__()
        with pytest.raises(RuntimeError, match="re-entered"):
            await participant.__aenter__()
        with pytest.raises(ValueError, match="does not manage"):
            await participant.cancel(
                ContractPointer(contractId="unknown", generation=1)
            )
        await participant.aclose()
        with pytest.raises(anyio.ClosedResourceError):
            await participant.notify_policy_changed()
        with pytest.raises(anyio.ClosedResourceError):
            await participant.cancel(
                ContractPointer(contractId="unknown", generation=1)
            )


@pytest.mark.asyncio
async def test_close_cancels_an_entry_blocked_before_contract_commit() -> None:
    contracts = LifecycleFaultHarness(bucket="contracts")
    barrier = contracts.block_next("create", phase=LifecycleFaultPhase.BEFORE)
    async with _running_concord(contracts=contracts) as concord:
        agreement = concord.propose(
            _Endpoint(CONTROLLER, "controller-session"),
            _spec(),
        )
        enter_error: list[BaseException] = []

        async def enter() -> None:
            try:
                await agreement.__aenter__()
            except BaseException as exc:
                enter_error.append(exc)

        async with anyio.create_task_group() as tasks:
            tasks.start_soon(enter)
            await barrier.wait()
            await agreement.aclose()

        assert agreement.state.phase == ConcordLifecyclePhase.CLOSED
        assert not agreement.state.local_authority
        assert agreement.closed
        assert contracts.mutation_count == 0
        assert enter_error


@pytest.mark.asyncio
async def test_agreement_close_wakes_subscribers_and_pending_waiters() -> None:
    assert _spec(profile=None).profile is None
    async with _running_concord() as concord:
        agreement = concord.propose(
            _Endpoint(CONTROLLER, "controller-session"),
            _spec(),
        )
        await agreement.__aenter__()

        waiter_error: list[BaseException] = []

        async def wait_for_impossible_state() -> None:
            try:
                await agreement.wait_for_state(lambda _state: False)
            except BaseException as exc:
                waiter_error.append(exc)

        async with agreement.watch_state() as states:
            assert (await anext(states)).phase == ConcordLifecyclePhase.READY
            observed = []

            async def receive_change() -> None:
                observed.append((await anext(states)).phase)

            async with anyio.create_task_group() as tasks:
                tasks.start_soon(wait_for_impossible_state)
                tasks.start_soon(receive_change)
                await anyio.sleep(0)
                await agreement.aclose()

            async for state in states:
                observed.append(state.phase)
            assert ConcordLifecyclePhase.CLOSING in observed
        assert len(waiter_error) == 1
        assert isinstance(waiter_error[0], anyio.ClosedResourceError)

        assert agreement.state.phase == ConcordLifecyclePhase.CLOSED


@pytest.mark.asyncio
async def test_lease_guards_callbacks_and_idempotent_close() -> None:
    sync_states = []

    async with _running_concord() as concord:
        contract = await concord._create_contract(  # noqa: SLF001
            (CONTROLLER, MANAGER),
            profile=PROFILE,
            created_by=CONTROLLER,
        )
        lease = ConcordParticipantLease(
            concord._repository,  # noqa: SLF001
            concord._view,  # noqa: SLF001
            contract=contract,
            participant=CONTROLLER,
            session_id="controller-session",
            on_change=sync_states.append,
        )
        token = await lease.attach()
        assert await lease.attach() is token
        assert lease.state.phase == ConcordParticipantLeasePhase.ACTIVE
        await lease.aclose()
        await lease.aclose()
        assert lease.state.phase == ConcordParticipantLeasePhase.CLOSED
        assert sync_states[-1].phase == ConcordParticipantLeasePhase.CLOSED
        with pytest.raises(ConcordConflict) as attach_error:
            await lease.attach()
        assert attach_error.value.code == ConcordConflictCode.LEASE_CLOSED
        with pytest.raises(ConcordConflict) as refresh_error:
            await lease.refresh()
        assert refresh_error.value.code == ConcordConflictCode.LEASE_CLOSED


@pytest.mark.asyncio
async def test_heartbeat_rechecks_authority_after_sleep(monkeypatch) -> None:
    async with _running_concord() as concord:
        contract = await concord._create_contract(  # noqa: SLF001
            (CONTROLLER, MANAGER),
            profile=PROFILE,
            created_by=CONTROLLER,
        )
        lease = ConcordParticipantLease(
            concord._repository,  # noqa: SLF001
            concord._view,  # noqa: SLF001
            contract=contract,
            participant=CONTROLLER,
            session_id="controller-session",
        )
        token = await lease.attach()

        async def close_during_sleep(_delay: float) -> None:
            lease.relinquish_local_authority()

        monkeypatch.setattr("deckr._concord._lease.anyio.sleep", close_during_sleep)
        await lease.heartbeat_loop()
        assert lease.token is None
        assert lease.owned_token is token
        await lease.aclose()


@pytest.mark.asyncio
async def test_owner_bootstrap_revision_retry_and_terminal_conflict() -> None:
    pointer = ContractPointer(contractId="stale", generation=1)
    revision_changed = ConcordConflict(
        ConcordConflictCode.REVISION_CHANGED,
        "changed",
    )
    terminal = ConcordConflict(
        ConcordConflictCode.PARTICIPANT_NOT_NAMED,
        "wrong owner",
    )
    store = SimpleNamespace(
        cancel_pointer=AsyncMock(side_effect=[revision_changed, True, terminal])
    )
    owner = ConcordOwnerBootstrap(store, SimpleNamespace())

    await owner._cancel_until_resolved(pointer, CONTROLLER)  # noqa: SLF001
    with pytest.raises(ConcordConflict) as exc_info:
        await owner._cancel_until_resolved(pointer, CONTROLLER)  # noqa: SLF001
    assert exc_info.value.code == ConcordConflictCode.PARTICIPANT_NOT_NAMED
    assert store.cancel_pointer.await_count == 3


@pytest.mark.asyncio
async def test_materialized_view_staleness_immediately_revokes_agreement() -> None:
    async with (
        _running_concord() as concord,
        concord.propose(
            _Endpoint(CONTROLLER, "controller-session"),
            _spec(),
        ) as agreement,
    ):
        await concord._attach(  # noqa: SLF001
            agreement.contract,
            MANAGER,
            "manager-session",
            token_id="manager-token",
        )
        await agreement.wait_for_state(lambda state: state.valid)
        async with concord._view._broadcaster.lock:  # noqa: SLF001
            concord._view._token_current = False  # noqa: SLF001
            concord._view._publish_locked(set())  # noqa: SLF001

        unavailable = await agreement.wait_for_state(
            lambda state: state.phase == ConcordLifecyclePhase.UNAVAILABLE,
            timeout_seconds=2,
        )
        assert not unavailable.local_authority
        assert not unavailable.valid


@pytest.mark.asyncio
async def test_participant_preserves_other_session_proposal_without_creating_token() -> (
    None
):
    async with (
        _running_concord() as concord,
        concord.participant(
            _Endpoint(MANAGER, "current-manager-session"),
            ConcordParticipantPolicy(profile=PROFILE, select=_accept_all),
        ) as participant,
    ):
        contract = await concord._create_contract(  # noqa: SLF001
            (CONTROLLER, MANAGER),
            profile=PROFILE,
            created_by=CONTROLLER,
            participant_sessions={
                str(CONTROLLER): "controller-session",
                str(MANAGER): "different-manager-session",
            },
        )
        await concord._attach(  # noqa: SLF001
            contract,
            CONTROLLER,
            "controller-session",
            token_id="controller-token",
        )
        await participant.notify_policy_changed()
        await participant._attachments._reconcile_pointer(contract.pointer)
        record = await concord.contract_record(contract)
        assert record is not None and record.state is ContractState.OPEN
        token_key = concord_participant_token_key(
            contract_id=contract.contract_id,
            generation=contract.generation,
            participant=MANAGER,
        )
        assert (  # noqa: SLF001
            await concord._repository.token_exact.get_exact(token_key) is None
        )
        assert participant.managed_contracts == ()


@pytest.mark.asyncio
async def test_initial_selector_failure_cleans_up_failed_participant_entry() -> None:
    def fail(_candidates):
        raise RuntimeError("policy failed during bootstrap")

    async with _running_concord() as concord:
        participant = concord.participant(
            _Endpoint(MANAGER, "manager-session"),
            ConcordParticipantPolicy(profile=PROFILE, select=fail),
        )
        with pytest.raises(RuntimeError, match="policy failed"):
            await participant.__aenter__()
        assert participant.state.phase == ConcordLifecyclePhase.CLOSED
        assert participant.managed_contracts == ()


@pytest.mark.asyncio
async def test_materialized_view_staleness_revokes_participant_before_cleanup() -> None:
    async with (
        _running_concord() as concord,
        concord.participant(
            _Endpoint(MANAGER, "manager-session"),
            ConcordParticipantPolicy(profile=PROFILE, select=_accept_all),
        ) as participant,
    ):
        contract = await concord._create_contract(  # noqa: SLF001
            (CONTROLLER, MANAGER),
            profile=PROFILE,
            created_by=CONTROLLER,
            participant_sessions={
                str(CONTROLLER): "controller-session",
                str(MANAGER): "manager-session",
            },
        )
        await concord._attach(  # noqa: SLF001
            contract,
            CONTROLLER,
            "controller-session",
            token_id="controller-token",
        )
        await participant.wait_for_state(
            lambda state: any(
                item.contract.pointer == contract.pointer and item.local_authority
                for item in state.contracts
            ),
            timeout_seconds=2,
        )
        async with concord._view._broadcaster.lock:  # noqa: SLF001
            concord._view._token_current = False  # noqa: SLF001
            concord._view._publish_locked(set())  # noqa: SLF001

        unavailable = await participant.wait_for_state(
            lambda state: state.phase == ConcordLifecyclePhase.UNAVAILABLE,
            timeout_seconds=2,
        )
        assert all(not item.local_authority for item in unavailable.contracts)


@pytest.mark.asyncio
async def test_closed_participant_selector_queue_rejects_waiters() -> None:
    async with _running_concord() as concord:
        participant = concord.participant(
            _Endpoint(MANAGER, "manager-session"),
            ConcordParticipantPolicy(profile=PROFILE, select=_accept_all),
        )
        await participant.__aenter__()
        snapshot = await concord._view.snapshot(  # noqa: SLF001
            PROFILE,
            participant=MANAGER,
            state=ContractState.OPEN,
        )
        await participant.aclose()
        await participant._queue_selector_snapshot(snapshot)  # noqa: SLF001
        with pytest.raises(anyio.ClosedResourceError):
            await participant._queue_selector_snapshot(  # noqa: SLF001
                snapshot,
                wait=True,
            )


def test_participant_policy_requires_valid_profile_and_selector() -> None:
    with pytest.raises(ValueError, match="profile"):
        ConcordParticipantPolicy(profile="", select=_accept_all)
    with pytest.raises(TypeError, match="callable"):
        ConcordParticipantPolicy(profile=PROFILE, select=None)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_exact_read_failure_revokes_a_current_agreement():
    tokens = LifecycleFaultHarness(bucket="tokens", ttl_seconds=60)
    async with (
        _running_concord(tokens=tokens) as concord,
        concord.propose(
            _Endpoint(CONTROLLER, "session"),
            _spec(participants=(CONTROLLER,), participant_sessions={}),
        ) as agreement,
    ):
        await agreement.wait_for_state(lambda state: state.valid)
        tokens.fail_next("get_exact", phase=LifecycleFaultPhase.BEFORE)
        await agreement._refresh_from_view(exact=True)
        assert not agreement.state.local_authority
        assert agreement._lease.token is None


@pytest.mark.asyncio
async def test_local_lease_revocation_cancels_even_with_live_shared_token():
    async with (
        _running_concord() as concord,
        concord.propose(
            _Endpoint(CONTROLLER, "session"),
            _spec(participants=(CONTROLLER,), participant_sessions={}),
        ) as agreement,
    ):
        await agreement.wait_for_state(lambda state: state.valid)
        agreement._lease.relinquish_local_authority()
        await agreement._lose_authority(
            ConcordLifecyclePhase.TERMINAL, "local owner stopped"
        )
        assert not agreement.state.local_authority
        assert (
            await concord.contract_record(agreement.contract)
        ).state is ContractState.CANCELLED


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["agreement", "participant"])
async def test_close_wins_when_initial_readiness_returns_after_cancellation(
    monkeypatch, kind
):
    async with _running_concord() as concord:
        context = (
            concord.propose(
                _Endpoint(CONTROLLER, "session"),
                _spec(participants=(CONTROLLER,), participant_sessions={}),
            )
            if kind == "agreement"
            else concord.participant(
                _Endpoint(MANAGER, "session"),
                ConcordParticipantPolicy(profile=PROFILE, select=_accept_all),
            )
        )
        entered = anyio.Event()
        release = anyio.Event()
        original = concord._view.wait_current

        async def delayed_readiness():
            # Model an in-flight operation that acknowledges after cancellation.
            with anyio.CancelScope(shield=True):
                await original()
                entered.set()
                await release.wait()

        monkeypatch.setattr(concord._view, "wait_current", delayed_readiness)
        errors = []

        async def enter():
            try:
                await context.__aenter__()
            except BaseException as exc:
                errors.append(exc)

        async with anyio.create_task_group() as group:
            group.start_soon(enter)
            await entered.wait()
            group.start_soon(context.aclose)
            with anyio.fail_after(1):
                while not context._close_started:
                    await anyio.sleep(0)
            release.set()
        assert errors
        assert context.state.phase is ConcordLifecyclePhase.CLOSED
        assert not context.state.current
