# CI Playwright Install Timeout

## Goal

Prevent GitHub-hosted CI runs from spending the full job timeout inside the
Playwright browser install step before repository checks execute.

## Constraints

- Keep CI aligned with the repository harness.
- Preserve the existing Chromium UI harness.
- Avoid changing plugin runtime behavior, Forward API behavior, or NetBox sync
  behavior.

## Touched Surfaces

- `.github/workflows/ci.yml`
- `scripts/check_harness.py`
- `docs/03_Plans/completed/2026-06-02-ci-playwright-install-timeout.md`

## Scope

- Keep the CI browser install deterministic and bounded.
- Preserve the existing Chromium UI harness.
- Do not change plugin runtime behavior, Forward API behavior, or NetBox sync
  behavior.

## Approach

- Replace `npx playwright install --with-deps chromium` with
  `timeout 20m npx playwright install chromium` in CI.
- Update the repository harness expectation to match the CI command.

## Implementation

- CI now installs the pinned Chromium browser with a 20-minute shell timeout.
- The harness check now requires the same bounded install command.

## Validation

- `npm ci`
- `npx playwright install chromium`
- `python scripts/check_harness.py`
- `invoke harness-check`

## Rollback

Restore the prior CI browser install command and matching harness expectation.

## Decision Log

- The cancelled GitHub runs completed apt dependency installation and downloaded
  the Chromium archive to 100%, then remained in the browser install process
  until the 45-minute job timeout cancelled the workflow.
- The CI image already has the Linux browser dependency set needed by the
  current harness, so the release gate should install the browser only and let
  the UI harness prove runtime compatibility.
