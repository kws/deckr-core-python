# AGENTS.md

This file adds agent-specific guidance for the `deckr-core-python` repository.

Use [README.md](./README.md) for setup, package purpose, validation, and the
human-facing release workflow. The upstream reference remains authoritative for
contracts. Keep shared guidance in the README and use this file for placement
rules and implementation hints.

## What Lives Here

`deckr` is the controller-independent Python implementation for the Deckr ecosystem.
The [Deckr reference repository](https://github.com/kws/deckr) owns the
authoritative specifications, schemas, fixtures, conformance vectors, and
protocol documentation. Keep those reference
materials there; do not regenerate them from Python models or bundle them here.
See the [project repository map](https://github.com/kws/deckr#project-repositories)
for the reference, three language cores, and integration harness.

Use this repo for:

- Python wire models and generic Beacon/Concord implementations
- Reusable managed hardware and service boundaries and Action Runtime helpers
- Generic runtime, component-hosting, configuration, and transport utilities

Keep controller routing, rendering, layout persistence, and settings placement in
application repositories such as
[`deckr-controller`](https://github.com/kws/deckr-controller). Concrete device
drivers and backend/action behavior belong in their owning applications.

## Directory Guide

- `src/deckr/contracts`
  - Python wire models, message validation, and endpoint/key helpers.
  - Implements the reference contracts; does not own their specification.
- `src/deckr/core`
  - Generic wire values, identities, KV ports and sources, task ownership, and
    host configuration utilities.
- `src/deckr/protocols`, `src/deckr/_beacon`, `src/deckr/_concord`
  - Public protocol APIs and their private implementations.
  - Keep them independent of Deckr profiles and NATS.
- `src/deckr/bindings`, `src/deckr/substrates`, `src/deckr/lanes.py`
  - Deckr binding policy, concrete transports, and endpoint delivery.
  - Generic NATS KV transport must not depend on Deckr profiles or protocols.
- `src/deckr/services`
  - Managed service consumers/providers, declarations, and protected views.
  - Depend on KV ports rather than a concrete NATS transport.
- `src/deckr/actions`
  - Action, binding, page, settings, and payload models and helpers.
- `src/deckr/action_runtime`
  - Action Runtime service declaration, availability, and message conversions.
  - Use the `services` lane and standard service-use agreements.
- `src/deckr/hardware`
  - Descriptors, managed clients/claims, and the hardware-manager boundary.
  - Must not depend on `deckr.actions`.
- `src/deckr/runtime.py`, `src/deckr/components`, `src/deckr/cli.py`
  - Host composition and component/CLI lifetimes; keep application policy external.
- `tests`
  - Tests for this Python package only; do not scan other repositories.
- `scripts`
  - Python implementation diagnostics and validation tooling.
  - `check_spec_conformance.py` reads upstream fixtures and vectors directly from
    `../deckr` or `DECKR_SPEC_ROOT`. CI checks out `.deckr-spec-revision`; keep
    upstream artifacts external and ordinary unit tests checkout-independent.

## Placement Rules

- Put code in `deckr` only if it is intended to be reused by multiple Deckr
  components.
- Prefer a narrow dependency surface. Small, stable contracts are better than
  moving implementation details here for convenience.
- If a change starts to look controller-specific, stop and move it to
  `deckr-controller`.

## Dependency Rules

These are enforced in [`.importlinter`](./.importlinter):

- `deckr.core` must not import `deckr.hardware`
- `deckr.core` must not import `deckr.actions`
- `deckr.contracts` must not import `deckr.actions` or `deckr.hardware`
- `deckr.hardware` must not import `deckr.actions`
- Generic core and protocols must not import Deckr profile or transport layers
- Generic NATS KV transport must not import protocol or Deckr policy
- Lane dispatch must not import domain bodies; services must not import substrates

After touching package boundaries or import structure, run:

```bash
uv run lint-imports
```

## Development Commands

Use `uv` consistently. The [README validation steps](README.md#development)
include the CI broker extra and coverage report:

```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run lint-imports
uv run pytest
uv run python scripts/check_spec_conformance.py
uv build
```

Cross-language tests live in
[deckr-integration-tests](https://github.com/kws/deckr-integration-tests).
Use public managed APIs in its workers; fix Python implementation defects here.
Coordinate specification and integration source pins using the README workflow.
Use repository URLs for cross-project documentation links and document only
coverage supported by the relevant tests.

## Release Notes

Do not duplicate the full release procedure here; follow the release section in
[README.md](./README.md#releases).

Short version:

- root `pyproject.toml` owns the published `deckr` version
- tag stable releases as `deckr-vX.Y.Z`
- after a stable release, bump immediately to the next `X.(Y+1).0.dev0`
- refresh `uv.lock` after every version change

## Hardware Imports

Use `deckr.hardware` directly in code, tests, and documentation.
