# CI hardening: coverage gate, dependency pin, version matrix

**Date:** 2026-07-04

## Goal
Close the CI-infra gaps from the assessment: coverage was configured but never
measured/enforced; deps were unpinned (tested != shipped); the NetBox matrix was a
single version with no stated policy.

## Constraints
- Set the coverage floor from a measured baseline (76%), not blind.
- Keep CI green; no unverified NetBox version added.

## Touched Surfaces
- `.coveragerc` — rewritten to a valid standalone config (the previous
  `[coverage:*]` sections only work embedded in setup.cfg, so coverage ignored it):
  `[run] source/omit`, `[paths]`, `[report]`.
- `development/Dockerfile` — installs `coverage` into the image.
- `.github/workflows/ci.yml` — the plugin-test step now runs under
  `coverage run` and enforces `coverage report --fail-under=70` (baseline 76%);
  `pip-audit` now audits the pinned `constraints.txt`; a comment documents the
  min+latest version-matrix policy (v4.6.4 is currently both — the newest 4.6.x).
- `constraints.txt` (new) — pins the tested runtime dependency versions
  (`cryptography`, `httpx`, `netboxlabs-netbox-branching`, `pyzipper`).

## Approach
Measured the baseline (76%) in the CI container, set the floor at 70% for margin.
Pinned the tested versions into `constraints.txt` (a poetry.lock would not be
consumed by the `uv pip install` image and adds tooling; a constraints file is
consumed directly by `pip-audit` and documents tested==shipped). Verified v4.6.4 is
the newest published 4.6.x, so the single-entry matrix already satisfies
min+latest.

## Validation
`coverage report --fail-under=70` exits 0 at 76%; yamllint clean; harness.

## Rollback
Revert; the coverage gate and constraints are additive.

## Decision Log
- 70% floor from a 76% baseline: a margin so incidental churn doesn't red the build;
  raise deliberately over time.
- constraints.txt over poetry.lock: consumed by the existing pip-audit step and the
  uv-based image without adopting the poetry toolchain.
- No blind NetBox-version bump: only the latest PUBLISHED 4.6.x is used; unverified
  versions are not added.

## Bundled changes
- CI enforces a 70% coverage floor; audits pinned deps; documents the version-matrix
  policy. Runtime deps pinned in constraints.txt.
