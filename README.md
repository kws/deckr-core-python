# Deckr Python core

This repository contains the controller-independent Python implementation of
Deckr, published as the `deckr` package. It provides Python wire models, runtime
primitives, Beacon and Concord APIs, hardware and service APIs, and NATS support.

The contract-first source of truth is the
[Deckr protocol and schema reference](https://github.com/kws/deckr)
repository. Specifications, schemas, fixtures, conformance vectors, and protocol
documentation are maintained there. Python models and tests in this repository
are implementation code, not the authority for the contracts.

Python regression tests cover protocol behavior and Deckr binding policy separately.
They do not replace the reference repository’s conformance vectors.

## Place in the project

The [project repository map](https://github.com/kws/deckr#project-repositories)
describes the reference, Python/Rust/TypeScript cores, and integration harness.
This repository owns the Python API and its implementation tests. Applications
own controller routing, rendering, configuration policy, drivers, and action or
integration behavior. Generic hosting and configuration utilities remain here
when they are reusable across those applications.

The cores implement shared contracts with language-specific APIs and independent
package versions and coverage. Real-broker interoperability is tested in
[deckr-integration-tests](https://github.com/kws/deckr-integration-tests);
its results apply to the workloads and source revisions recorded by that harness.
Run Python commands below from this repository's root.

## Reference material

- [Reference overview](https://github.com/kws/deckr#readme)
- [Schema consumption](https://github.com/kws/deckr/blob/main/docs/schema-guide.md)
- [Architecture](https://github.com/kws/deckr/blob/main/docs/deckr-architecture.md)
- [Beacon](https://github.com/kws/deckr/blob/main/docs/beacon.md),
  [Concord](https://github.com/kws/deckr/blob/main/docs/concord.md), and
  [NATS](https://github.com/kws/deckr/blob/main/docs/deckr-nats-binding.md)
- [Hardware](https://github.com/kws/deckr/blob/main/docs/deckr-hardware.md),
  [services](https://github.com/kws/deckr/blob/main/docs/deckr-services.md), and
  [Action Runtime](https://github.com/kws/deckr/blob/main/docs/deckr-action-runtime.md)
- [Conformance scenarios](https://github.com/kws/deckr/blob/main/deckr-conformance-scenarios.md)

The Python distribution does not bundle or generate reference artifacts.
The former `deckr.contracts.artifacts` module and its exports from
`deckr.contracts` have been removed with that bundle. Obtain reference artifacts
from the reference repository at the revision recorded in
[.deckr-spec-revision](.deckr-spec-revision).

## Specification dependency

The core targets the alpha Action Runtime contract at the Git revision in
[.deckr-spec-revision](.deckr-spec-revision).
Page commands require explicit `BindingCommandTarget` or
`PageSessionCommandTarget` values from `deckr.actions.messages`; `close_page`
uses `ClosePageBody`, and page-session metadata requires `generation`.
Earlier targetless commands are rejected.

`deckr.actions.matching.match_capability_requirements` selects capabilities
within a supplied negotiated device, control, and optional configured scope.
`validate_binding_resources` checks application-selected results, including
preference order, complete supported name sets, provenance, and exact sources.
Equivalent candidates remain application policy. No requirements means no
matches. Every match must name its requirement.

`deckr.action_runtime.validate_action_runtime_message` checks service subjects
and typed payloads. `validate_page_command` checks `PageCommandContext`
observations against a command and returns its retained target. The application
must supply current agreement, session, claim, and target observations and call
the helper again while committing a serialized transition. The helper does not
allocate pages or acquire authority.

`deckr.actions.rendering` defines support, baseline content, `BindingRenderBody`,
and `BindingRenderResultBody`. `validate_rendering_support` checks named matches
and aliases; `validate_render_command` checks current `RenderCommandContext`
facts and admission limits. Controllers must recheck these facts at dispatch.
Image decoding, font choice, rendering, and output ordering belong to controllers.
`DeckrMessage.wire_size_bytes` preserves the received UTF-8 envelope size on NATS.
Managed service sends accept `causation_id`, and hardware commands accept a
reserved `message_id` for registering rejection correlation before publication.

Hardware discovery candidates expose immutable manager `labels` snapshots.
Managed claim events include `CommandRejectedMessage` with the original envelope
and correlation; a rejection alone leaves claim authority intact.
`ManagedService.state.contract` exposes the observed agreement identity.
Use managed operations to check current authority when acting on that observation.

CI checks out the immutable `kws/deckr` commit in `.deckr-spec-revision` and runs
`scripts/check_spec_conformance.py` against its artifacts. The script compares
Python key encoding, Beacon/Concord keys, terms hashing, NATS message mapping,
settings target keys, protocol records, and Action Runtime matching, page, and rendering admission semantics
with upstream vectors and positive/negative fixtures. These checks cover those
implemented cases; they do not establish complete protocol or distributed
lifecycle conformance.

To reproduce CI without changing an existing sibling specification checkout:

```bash
git clone https://github.com/kws/deckr.git .deckr-spec
git -C .deckr-spec checkout --detach "$(cat .deckr-spec-revision)"
DECKR_SPEC_ROOT="$PWD/.deckr-spec" uv run python scripts/check_spec_conformance.py
```

The script defaults to `../deckr`; `DECKR_SPEC_ROOT` selects another repository
root containing `contract/v1/`. Missing artifacts fail the check. Local checks
use the selected checkout as it stands, allowing upstream changes to be tested
before updating the pin. Ordinary unit tests remain independent of that checkout.

When updating the pin, review upstream changes, run this check and the Python
validation suite, and coordinate the revision with the Rust and TypeScript
implementations. Push the upstream commit before CI needs it. For a private
`kws/deckr` repository, configure the `DECKR_SPEC_TOKEN` Actions secret with read
access; otherwise CI uses its default GitHub token.

When validating a coordinated contract or runtime change, update the source commits in
`deckr-integration-tests/sources.lock.json` and follow that repository's
[source-pin workflow](https://github.com/kws/deckr-integration-tests#source-pins-and-coordinated-changes).
All three core specification pins must agree with the harness's reference pin.

## Repository layout

```text
src/deckr/    Python package: runtime, wire models, services, hardware, and testing helpers
tests/        Python implementation tests
scripts/      Python diagnostics and lifecycle coverage checks
.deckr-spec-revision  Specification commit used by CI
pyproject.toml
uv.lock
```

Controller orchestration, rendering, layout persistence, and settings placement
belong in application repositories such as
[`deckr-controller`](https://github.com/kws/deckr-controller). Concrete drivers and
device behavior belong in the hardware-manager applications.

## Development

Requirements: Python 3.11+ and `uv`.

Install the package and development tooling:

```bash
uv sync
```

For the same broker-test coverage as CI, install `uv sync --extra supervised-nats`
before running the suite. Without that extra or `NATS_SERVER_BINARY`, the optional
real-broker tests skip.

Run the Python validation suite:

```bash
uv run ruff check .
uv run ruff format --check .
uv run lint-imports
uv run pytest --cov-branch --cov-report=json:coverage.json
uv run python scripts/report_lifecycle_branch_coverage.py coverage.json
uv run python scripts/check_spec_conformance.py
```

Focused protocol tests can use `MemoryJsonKvBucket`, `ConcordRuntimeHarness`,
and `ConcordMaintenanceHarness` from `deckr.testing`. Use the runtime harness
for ordinary Concord tests and the maintenance harness for maintenance coverage.

Tests should assert observable behavior: accepted or rejected messages, stored
records, authority transitions, delivered values, and completed cleanup. Use
controlled faults and synchronization to exercise races. Architecture and public
API boundary checks are also useful. Avoid test-local protocol models, internal
call-count scaffolding without a behavioral outcome, and exception suppression
that can hide assertion failures. Branch coverage is reported as a diagnostic,
not a pass/fail percentage; it does not establish specification conformance.

Build the Python wheel and source distribution:

```bash
uv build
```

Optional extras are `deckr[cli]`, `deckr[nats]`, and `deckr[supervised-nats]`.
The supervised NATS extra includes the `deckr-nats-server-bin` binary package
for hosts that supervise a local NATS server.
`deckr[all]` installs all three extras. The CLI, component hosting, and generic
configuration helpers support application hosts; they do not supply a controller,
device driver, or action implementation.

To inspect a running broker's Deckr state:

```bash
uv run --extra nats python scripts/nats_state_report.py --url nats://127.0.0.1:4222
```

## Package boundaries

Keep Python code reusable across Deckr components. The dependency direction is:

```text
Deckr runtime / components
    -> hardware, services, actions (Deckr profile policy)
    -> bindings (Deckr subjects, body codecs, authority-store names)
    -> protocols.beacon / protocols.concord / protocols.concord_maintenance
    -> core (wire values, identities, KV ports, currentness, task ownership)

substrates.nats_kv -> core (JSON KV transport, heartbeat and continuity checks)
bindings.nats -> substrates.nats_kv + contracts + lanes
```

Generic callers use `deckr.protocols.beacon` and `deckr.protocols.concord`, provide
KV ports, and choose their own endpoint families and profile policies. The private
`_beacon` and `_concord` packages implement those APIs. They have no Deckr-domain
or NATS imports. `deckr.core.kv.JsonKvBucket` describes exact storage operations;
`KvMaterializedSource` owns snapshots and currentness independently of transport.

Deckr endpoint restrictions live in `deckr.contracts.messages`. Service-use
admission and hardware claim selection live in their extension packages. The
runtime injects Deckr body codecs through `MessageContract.body_validator`.
Applications constructing lane contracts directly can use
`deckr.bindings.messages.with_deckr_body_codec` for standard Deckr messages.

The existing `deckr.beacon`, `deckr.concord`, `deckr.concord_maintenance`, and
`deckr.substrates.nats` imports remain compatibility entry points. Their Deckr
store constants and subject adapter are defined in `deckr.bindings`.
`ServiceUseLease.require_authority()` checks local authority; its older `refresh()`
spelling remains an alias and does not renew tokens.

`.importlinter` enforces the generic boundaries and prevents core/contracts from
importing hardware or actions, and hardware from importing actions. Exact lifecycle
and subscription inventories guard ownership routes. Production modules have a
650-line ceiling enforced by tests; split responsibilities before reaching it.
Long-lived contexts keep lifecycle coordination together, with models, storage,
indexing, attachment workers, and domain policy in separate modules.

Authority is checked when a capability is used. Source discontinuity permanently
revokes existing local leases, even if a recovered snapshot coalesces notifications.
Owner bootstrap preserves healthy participation from other runtimes or sessions;
a different session or a missing local registry entry is not proof of failure.
Managed service view streams terminate on revocation even while idle.

Import the hardware package directly as `deckr.hardware`.

Real-broker tests are enabled by `uv sync --extra supervised-nats`, or by setting
`NATS_SERVER_BINARY` to a local server executable. They run a temporary authenticated
loopback broker and verify replay, idle heartbeats, and consumer loss. CI installs
the extra so these tests run there.

`DeckrNatsSubstrate.aclose()` cancels an outstanding initial connection attempt,
including cleanup of a client that arrives after cancellation. Required Concord
renewals use an absolute monotonic expiry; blocked refreshes and late completions
cannot extend local authority. Invalid JSON records remain invalid observations
without making unrelated shared state unavailable.

NATS buckets are created with their complete policy atomically, including TTL
deletion markers, so concurrent processes cannot observe an intermediate policy.
Explicit lists read values from one owned snapshot consumer and delete it before
returning, including cancellation cleanup. They do not list keys and then fetch
every value separately.

KV watches bound outstanding broker deliveries to their configured buffer size
and acknowledge each record when the reader consumes it. Large initial snapshots
and slow readers therefore drain through a bounded window. A separate slot holds
the snapshot barrier; connection, heartbeat, and sequence failures still end the
watch and require an explicitly observed recovery.

With the reference and three implementations checked out as siblings,
[`deckr-integration-tests`](https://github.com/kws/deckr-integration-tests)
uses public managed service APIs for its Compose smoke and recovery workloads.
Its [native Concord runner](https://github.com/kws/deckr-integration-tests#native-concord-lifecycle-test)
uses `scripts/interop_peer.py` for concurrent startup, attachment, numeric terms,
automatic renewal, broker loss, and recovery across languages.

A supervised restart can avoid waiting for token expiry. Load an authenticated
[restart handoff](https://github.com/kws/deckr/blob/main/docs/deckr-deployment.md#verified-restart-handoff)
with `deckr.core.restart.restart_handoff_from_env()` and pass it as
`Deckr(restart=proof)` (or `Concord(..., restart=proof)` for protocol-only hosts).
The configured launcher reads `DECKR_RESTART_HANDOFF` and `DECKR_INSTANCE_ID`.
It confirms cancellation of predecessor participation, including consumer-created
contracts, before yielding replacement endpoints. `Deckr.endpoint_sessions`
provides process-local session diagnostics for the supervisor; it is not an
authority query. A host owns at most 256 simultaneous endpoint contexts, and
closed contexts are removed from this diagnostic registry. Beacon cleanup removes only proven exited sessions. Other live
sessions remain intact, and replacement endpoints always need fresh sessions.
The supervisor retains unresolved predecessor entries across repeated crashes.
Without confirmed exit evidence, normal token validity and contention apply.

## Services and Action Runtime

`Deckr.services(endpoint)` creates a `DeckrServices` context. Use `use_matching()`
to discover and negotiate a matching service, or `use()` for a selected descriptor.
The returned `ManagedService` provides guarded messages, requests, protected
views, and observable lifecycle state. `Deckr.service_provider(...)` creates the
provider boundary for an application-owned `ServiceProtocol`. Backend behavior
and domain consent remain with the application.
Provider entry waits and retries bounded bootstrap attempts while earlier
ownership is unresolved. It does not advertise during that wait, and `aclose()`
cancels entry. Managed client state retains `cleanup_failures` and
`last_cleanup_failure` after recovery. Client shutdown reports failures from
current or pending cleanup; an earlier completed recovery failure does not fail
a later successful shutdown.

`deckr.actions` contains action, binding, page, settings, and payload models.
`deckr.action_runtime` supplies the Action Runtime service declaration, availability
payloads, and message conversion helpers. Action Runtime uses
`service:action-runtime.<providerInstanceId>` and `serviceMessage` on the
`services` lane. Compose these helpers with the service APIs; there is no separate
core `actions` lane. The controller owns live routing references and sends
hardware output through a separate hardware claim.

The Action Runtime declaration advertises the exact payload-schema references
from the upstream catalogue. Importing the declaration registers its local
Pydantic validators through `register_service_payload_validator`; generic service
send/receive validation therefore resolves those exact references without reading
the specification checkout or fetching schemas. Registration is for trusted local
implementations, cannot replace an existing binding, and applies only to an exact
`{"$ref": schemaId}` document. Other inline schemas still use local JSON Schema
validation; unknown external references fail closed. The conformance script also
compares the complete service declaration against the upstream catalogue.

## Managed hardware

`Deckr.hardware(endpoint)` creates a `DeckrHardware` context for a controller
endpoint. `ComponentContext.hardware(endpoint)` uses the same host factory.
Clients in one runtime share a typed `HardwareDirectory`; clients using the same
`EndpointSession` also share one hardware message subscription. Close clients
before their endpoint. Runtime shutdown closes clients before Beacon and Concord.

Selection remains application policy. Directory candidates contain the exact
manager endpoint/session, device reference, immutable descriptor, and advertised
capacity. Capacity is advisory: only a valid Concord agreement authorizes work.
`directory.state.current` distinguishes stale discovery from a current empty
result. `directory.watch_state()` includes an initial snapshot and a final closed
snapshot; `directory.candidates()` rejects stale queries. Discovery loss alone
does not revoke an existing hardware agreement.

This example uses an already-entered `Deckr` runtime and a device advertising a
`raster.bitmap` capability on `key.0.0`:

```python
from deckr.hardware import HardwareClaimSpec
from deckr.runtime import Deckr


async def use_device(deckr: Deckr, manager_id: str, device_id: str) -> None:
    async with (
        deckr.endpoint("controller:main") as endpoint,
        deckr.hardware(endpoint) as hardware,
    ):
        selected = await hardware.wait_for_device(
            lambda candidate: (
                candidate.manager_endpoint.endpoint_id == manager_id
                and candidate.device_ref.device_id == device_id
            ),
            timeout_seconds=10,
        )
        async with hardware.claim(HardwareClaimSpec((selected,))) as claim:
            await claim.command(
                device_id, "raster.bitmap", "clear", control_id="key.0.0"
            )
            async with claim.events() as events:
                async for event in events:
                    print(event.body)
```

A `HardwareClaimSpec` can group several devices from one manager session. Entry
waits for authority, with a default ten-second negotiation timeout. Each
`ManagedHardwareClaim` owns exactly one public Concord proposal context and its
cleanup. Its `state`, `watch_state()`, and `wait_for_state(predicate)` expose
immutable `HardwareClaimState` snapshots, including the contract pointer, local
authority, descriptors, and structured failure. Watches coalesce current state.

`command()` validates the target, supported operation, and descriptor command
schema, then publishes under the exact agreement and endpoint sessions. Its return
value is the published envelope, not an execution acknowledgement. `request_state()`
also validates the returned scope and value and rechecks authority after waiting.
Descriptor JSON Schemas are validated locally using `jsonschema`; embedded/local
references are supported and remote references are never fetched.

`events()` delivers validated input and state changes in arrival order to one
consumer. Its queue defaults to 64 events (`event_buffer_size`, range 1–4096).
Overflow revokes the claim with `HardwareUnavailable.code == "input_overflow"`;
discrete input is never coalesced. A consumer wakes on authority loss even when
idle. Closing an event context discards its backlog; opening another starts a new
stream. Clients and routers admit at most 64 pending or active claims per endpoint,
and a runtime admits at most 256 clients.

Recovery is explicit: `hardware.use(spec, recover=same_hardware)` returns a
`ManagedHardwareUse` that negotiates successors with bounded retry backoff.
Import `same_hardware` from `deckr.hardware`; it selects the same manager and
device identities with materially unchanged descriptors, and declines ambiguous
sessions. A custom `HardwareRecoveryPolicy` may return a new spec or `None` to
wait for another directory update. Each successor has a fresh opaque contract ID
and generation 1. Old claim handles remain invalid, commands are never replayed,
and event streams belong to one agreement. Observe `use.watch_state()` and reopen
`use.claim.events()` after establishing application routing for a successor.
Routing, rendering, action bindings, persistence, and broader device-selection
policy remain in the controller.

The existing `hardware_manager(...)` factory remains the driver entry point.
Hardware command handlers receive a `HardwareCommandContext`, with the validated
`message` and `device`. Retain that context with queued work and call
`context.authorize()` immediately before each device operation. Use
`await context.run_sync(write, ...)` for blocking I/O: it rechecks authority in
the worker thread after waiting for thread capacity. Cancellation, device
replacement/removal, and manager shutdown permanently invalidate old work;
a successor claim never authorizes an earlier command. Physical initialization
and safe reset operations remain device-local behavior.

`HardwareManagerRuntime.state`, `watch_state()`, and `wait_for_state(predicate)`
now expose immutable inventory and live claims, discovery currentness,
advertisement publication status, authority currentness, and structured failures.
Claim loss is observable before driver reset callbacks or remote cleanup finish.
The component factory delegates to the host to preserve worker ownership.

## Releases

This repository releases a single Python distribution: `deckr`. Its version is
independent of the reference contract bundle version.

- The source of truth for the published version is the root `pyproject.toml`.
- Use package tags in the form `deckr-vX.Y.Z`.
- Stable releases use normal PEP 440 versions such as `0.3.0`.
- After each stable release, bump immediately to the next development line,
  e.g. `0.4.0.dev0`, in a separate follow-up commit.

### Release Flow

1. Update `version` in `pyproject.toml` to the stable release number.
2. Run the validation suite:

   ```bash
   uv run ruff check .
   uv run ruff format --check .
   uv run lint-imports
   uv run pytest
   uv run python scripts/check_spec_conformance.py
   ```

3. Refresh the lockfile:

   ```bash
   uv lock --refresh
   ```

4. Commit the release, for example:

   ```bash
   git commit -am "chore(deckr): release v0.3.0"
   ```

5. Tag the release commit:

   ```bash
   git tag deckr-v0.3.0
   ```

6. Build from the tag so the artifacts match the stable version exactly:

   ```bash
   git checkout deckr-v0.3.0
   uv build
   git checkout -
   ```

7. Publish the wheel and sdist using your usual PyPI workflow.
8. Immediately bump `pyproject.toml` to the next development version, refresh
   the lockfile, and commit that separately:

   ```bash
   uv lock --refresh
   git commit -am "chore(deckr): bump to development release 0.4.0.dev0"
   ```

The stable tag should always point at the stable release commit, not the later
`.dev0` commit.

## Provider requests and retained resources

`await provider.serve(handler, timeout_seconds=8, max_concurrency=32)` applies
backpressure before creating request workers. The handler receives a
`ServiceRequestContext` with the original message and authorization. Its `reply`
method preserves protocol metadata; the dispatcher validates result/error payloads.
A handler returning an invalid result produces `invalid_service_reply`, not an
unvalidated success. Timeouts return `service_command_timeout`.

The context monitors live provider authority and cancels suspended work on loss.
`check()` also verifies expiry and any resource constraints at dispatch. Use
`guarded_call` for asynchronous backend dispatch and `guarded_run_sync` for blocking
adapters; the latter checks inside the worker after thread-capacity waits. Adapters
that perform several external operations must call `check_operation()` again
between effects. Guards never recreate or transfer agreement authority. Cancelling
an already dispatched external operation only abandons its result; do not replay it.

`ServiceResourceRegistry` owns full-replacement scopes by contract, consumer
endpoint/session, and subscriber. Call `replace(request, subscriber, resources)` for
scope changes and `require_resource(request, subscriber, resource)` before commands.
Start it under the service task group. Its `on_change` callback reconciles the
current `resources` union outside request contexts and retries backend failures.
Backend availability changes projections, not Concord lifetime.

`publish(resource, payload)` uses live managed view access and orders writes with
scope removal. Its optional `is_current` predicate rejects stale backend snapshots,
including after a KV readiness wait. `invalidate(resource)` removes projections
without clearing retained intent. Cleanup uses exact owned fences and revisions,
reconciles actual owned scopes against current authority, and retries failures.
A scope mutation that fails after changing local state retires the agreement.
The default limits are 256 subscriber scopes and 1,024 resources per scope; empty
scopes release their entries. Close the registry before closing its view store and
provider, and ensure all component cleanup runs even if one close fails.

`ResourceServiceClient` configures a `SharedResourceSubscriptionManager` with domain
operation, parameter and view-family names. `subscription_session` yields a
`ServiceResourceSession` with `messages`, `set(resources)`, `command(...)` and
`aclose()`. Its shared-manager key includes namespace, service, family, scope
operation and parameter, subscriber identity, request timeout and negotiation
timeout. Domain adapters supply declarations rather than duplicate lifecycle code.

`AsyncSingleFlight` shares concurrent read-only lookups by key. It does not cache
results or share command authority. Cancellation removes the pending entry under
a shield and wakes waiters with `SharedOperationCancelled`; later calls may retry.
`model_payload_schema` binds a native model to an exact reference schema ID.
Reference fixtures remain external and must test native model conformance.

For implementation tests, `deckr.testing.runtime.mock_deckr` provides real managed
Beacon/Concord over in-memory KV with mocked lane delivery. `ServiceAuthorityStub`
is only for isolated handler tests; it must not substitute for Concord regressions.
