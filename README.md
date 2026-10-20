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

## Reference material

- [Reference overview](https://github.com/kws/deckr#readme)
- [Schema consumption](https://github.com/kws/deckr/blob/main/docs/schemas.md)
- [Architecture](https://github.com/kws/deckr/blob/main/docs/deckr-architecture.md)
- [Beacon](https://github.com/kws/deckr/blob/main/docs/beacon.md),
  [Concord](https://github.com/kws/deckr/blob/main/docs/concord.md), and
  [NATS](https://github.com/kws/deckr/blob/main/docs/nats.md)
- [Migration guide](https://github.com/kws/deckr/blob/main/docs/migration-guide.md)

The Python distribution does not bundle or generate reference artifacts.
The former `deckr.contracts.artifacts` module and its exports from
`deckr.contracts` have been removed with that bundle. Obtain reference artifacts
from the reference repository and pin an appropriate commit or release when
consuming them.

## Repository layout

```text
src/deckr/    Python package: runtime, wire models, services, hardware, and testing helpers
tests/        Python implementation tests
scripts/      Python diagnostics and lifecycle coverage checks
pyproject.toml
uv.lock
```

Controller orchestration, rendering policy, filesystem configuration, and
device-manager-specific behavior belong in
[`deckr-controller`](https://github.com/kws/deckr-controller).

## Development

Requirements: Python 3.11+ and `uv`.

Install the package and development tooling:

```bash
uv sync
```

Run the Python validation suite:

```bash
uv run ruff check .
uv run ruff format --check .
uv run lint-imports
uv run pytest --cov-branch --cov-report=json:coverage.json
uv run python scripts/report_lifecycle_branch_coverage.py coverage.json
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

With the three implementations checked out as siblings, the external
[`mixed-language` harness](../mixed-language/README.md) uses
`scripts/interop_peer.py` to test real concurrent startup, attachment, lossless
numeric terms, automatic renewal, broker loss, and recovery across languages.

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
