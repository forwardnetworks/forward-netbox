# Playwright UI Harness

## Goal

Add a deterministic browser harness for the Forward NetBox UI so visible workflow regressions are caught by local validation and GitHub CI.

## Constraints

- Use synthetic fixture identifiers only; do not commit customer network IDs, snapshot IDs, credentials, or private screenshots.
- Exercise NetBox plugin pages through real browser navigation rather than direct template tests.
- Avoid live Forward API calls in the UI harness.
- Keep the harness compatible with the existing Docker development stack and CI's separate database migration step.

## Touched Surfaces

- Playwright dependency and browser harness script.
- Synthetic Django management command for UI fixtures.
- Invoke tasks and GitHub Actions CI.
- Project knowledge docs and harness checker requirements.

## Approach

Apply pending Django migrations, then seed a local superuser, Forward source, Forward sync, ingestion, completed Job, metrics, log rows, and one issue through a management command. Drive the browser through the unauthenticated redirect, login, sync list, sync detail, ingestion detail, sync creation form, and mobile sync list. Capture screenshot artifacts outside git and assert key page text plus horizontal overflow checks.

### QA Inventory

- Pages: login, Forward sync list, Forward sync detail, Forward ingestion detail, Forward sync add form.
- Roles: unauthenticated visitor and NetBox superuser.
- Data states: ready sync, completed ingestion, completed job logs, snapshot metrics, ingestion issue.
- Viewports: desktop `1440x1000`, mobile `390x900`.
- Visual checks: screenshot capture and layout overflow assertions.

## Validation

- `npm ci`
- `npx playwright install chromium`
- `invoke harness-check`
- `invoke harness-test`
- `invoke playwright-test`
- `invoke lint`
- `invoke check`
- `invoke scenario-test`
- `invoke test`
- `invoke docs`

## Rollback

Remove the Playwright package files, the browser harness script, the UI fixture management command, the invoke task, CI browser steps, and the docs references.

## Decision Log

- Chose a management command instead of browser-created source records because the source form validates against Forward and would make the UI harness depend on live SaaS state.
- Chose a completed local Job fixture so the ingestion detail page renders progress, statistics, metrics, and logs without starting a worker job.
- Kept browser-script migrations enabled by default for local runs, with a CI skip flag after the dedicated migration step to avoid memory pressure in the running web container.
- Set user privilege fields only when the active NetBox user model exposes them, because NetBox 4.5's custom user model does not use Django's standard `is_staff` field in CI.
- Kept screenshots as ignored local artifacts so CI and local runs produce evidence without committing generated images.
