# Deckr Python core

This repository contains the controller-independent Python implementation of
Deckr, published as the `deckr` package. It provides Python wire models, runtime
primitives, Beacon and Concord APIs, hardware and service APIs, and NATS support.

The contract-first source of truth is the
[Deckr protocol and schema reference](https://github.com/kws/deckr)
repository. Specifications, schemas, fixtures, conformance vectors, and protocol
documentation are maintained there. Python models and tests in this repository
are implementation code, not the authority for the contracts.

This separation preserves the existing Python runtime behavior. Alignment with
the current reference contracts is separate work; passing these tests does not
establish conformance to the latest reference release.

## Reference material

- [Reference overview](https://github.com/kws/deckr#readme)
- [Schema consumption](https://github.com/kws/deckr/blob/main/docs/schemas.md)
- [Architecture](https://github.com/kws/deckr/blob/main/docs/runtime-architecture.md)
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
uv run lint-imports
uv run pytest --cov-branch --cov-report=json:coverage.json
uv run python scripts/check_lifecycle_branch_coverage.py coverage.json
```

Focused protocol tests can use `MemoryJsonKvBucket`, `ConcordRuntimeHarness`,
and `ConcordMaintenanceHarness` from `deckr.testing`. Use the runtime harness
for ordinary Concord tests and the maintenance harness for maintenance coverage.

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

Keep Python code reusable across Deckr components. Internal dependencies are
enforced by `.importlinter`:

- `deckr.core` must not import `deckr.hardware` or `deckr.actions`.
- `deckr.contracts` must not import `deckr.actions` or `deckr.hardware`.
- `deckr.hardware` must not import `deckr.actions`.

Import the hardware package directly as `deckr.hardware`.

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
