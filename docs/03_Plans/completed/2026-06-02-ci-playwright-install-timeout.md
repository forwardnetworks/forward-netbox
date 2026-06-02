# CI Hosted Browser Validation Scope

## Goal

Prevent GitHub-hosted CI runs from spending the full job timeout inside
Playwright browser setup while preserving local browser validation as a release
gate.

## Constraints

- Keep CI aligned with the repository harness.
- Preserve the existing local Chromium UI harness.
- Avoid changing plugin runtime behavior, Forward API behavior, or NetBox sync
  behavior.

## Touched Surfaces

- `.github/workflows/ci.yml`
- `scripts/check_harness.py`
- `docs/00_Project_Knowledge/agent-workflow.md`
- `docs/00_Project_Knowledge/release-playbook.md`
- `docs/03_Plans/completed/2026-06-02-ci-playwright-install-timeout.md`

## Scope

- Keep GitHub-hosted CI focused on non-browser repository, Docker, scenario,
  and plugin test gates.
- Preserve the existing Chromium UI harness for local release validation through
  `invoke playwright-test` and `invoke ci`.
- Do not change plugin runtime behavior, Forward API behavior, or NetBox sync
  behavior.

## Approach

- Remove hosted Node/Playwright setup from GitHub Actions.
- Remove the hosted `npm run test:ui` step from GitHub Actions.
- Update the repository harness expectation to match the hosted CI scope.
- Document that Playwright proof remains in the local release gate.

## Implementation

- GitHub CI no longer installs Playwright browsers or runs the UI harness.
- The harness check now requires the hosted non-browser CI shape.
- The release playbook keeps `invoke playwright-test` in the local gate and
  states that GitHub CI is the non-browser hosted gate.

## Validation

- `python scripts/check_harness.py`
- `invoke harness-check`
- `invoke playwright-test` remains the local browser validation command.

## Rollback

Restore the prior hosted browser install and `npm run test:ui` workflow steps,
then restore the matching harness expectation.

## Decision Log

- The cancelled GitHub runs completed apt dependency installation and downloaded
  the Chromium archive to 100%, then remained in the browser install process
  until the job timeout cancelled the workflow.
- Increasing the browser install timeout did not move the jobs quickly enough to
  make hosted browser setup useful as a release blocker.
- The local release gate already proves browser compatibility with
  `invoke playwright-test`, so GitHub CI should avoid duplicating the hosted
  browser setup path.
