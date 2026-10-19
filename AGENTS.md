# AGENTS.md

This file adds agent-specific guidance for the `deckr-core-python` repository.

Use [README.md](./README.md) as the primary source of truth for setup, package
purpose, and the human-facing release workflow. Keep shared guidance in the
README and use this file for placement rules and implementation hints.

## What Lives Here

`deckr` is the controller-independent Python implementation for the Deckr ecosystem.
The [Deckr reference repository](https://github.com/kws/deckr) owns the
authoritative specifications, schemas, fixtures, conformance vectors, and
protocol documentation. Keep those reference
materials there; do not regenerate them from Python models or bundle them here.
Contract alignment and Python behavior changes are separate from repository
separation.

Use this repo for:

- Python implementations of shared hardware contracts and wire models
- Python action-provider-facing manifests, messages, and lifecycle primitives
- reusable Python runtime utilities that are not controller-specific

Do not place controller orchestration, rendering policy, filesystem config, or
device-manager-specific behavior here. That belongs in the
[`deckr-controller`](https://github.com/kws/deckr-controller) repository.

## Directory Guide

- `src/deckr/contracts`
  - Python wire models, message validation, and endpoint/key helpers.
  - Implements the reference contracts; does not own their specification.
- `src/deckr/core`
  - Generic runtime utilities and messaging primitives.
  - Must not depend on `deckr.hardware` or `deckr.actions`.
- `src/deckr/actions`
  - Runtime-neutral `actions` lane wire contracts, provider-instance catalogs,
    settings targets, endpoint helpers, and action-facing capability contracts.
- `src/deckr/hardware`
  - Shared hardware-facing contracts.
  - Must not depend on `deckr.actions`.
- `tests`
  - Tests for this Python package only; do not scan other repositories.
- `scripts`
  - Python implementation diagnostics and validation tooling.

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

After touching package boundaries or import structure, run:

```bash
uv run lint-imports
```

## Development Commands

Use `uv` consistently:

```bash
uv sync
uv run ruff check .
uv run lint-imports
uv run pytest
uv build
```

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
