# NetBox 4.5.8 Native Workflow Alignment

## Goal

Update the local development stack to NetBox `4.5.8` and align the sync workflow with NetBox-native operator behavior.

## Constraints

- Stay within the supported NetBox `4.5.x` line.
- Use NetBox and NetBox Branching native job, status, table, and model hooks.
- Do not add transform-map authoring, endpoint/filter models, or plugin-side data shaping.
- Do not commit customer-derived network IDs, snapshot IDs, tenant labels, credentials, or screenshots.
- Keep changes independently testable before larger validation/drift-policy work begins.

## Touched Surfaces

- `development/.env`
- `forward_netbox/models.py`
- `forward_netbox/jobs.py`
- `forward_netbox/tables.py`
- `forward_netbox/tests/`
- `docs/03_Plans/active/`

## Approach

1. Change the local Docker NetBox base image from `v4.5.0` to `v4.5.8`.
2. Preserve a sync's last terminal status when creating or rescheduling scheduled jobs; only a brand-new scheduled sync should move from `NEW` to `QUEUED`.
3. Show `scheduled` in the default sync table columns.
4. Return an empty `docs_url` for plugin models so NetBox does not expose a broken local-docs button on model edit pages.

Keep the larger validation, drift policy, query contract, and release automation work in their dedicated active harness plans.

## Validation

- Unit tests added for scheduled enqueue status transitions.
- Unit test added for recurring reschedule status preservation.
- Unit tests added for default scheduled column and disabled model docs URLs.
- Pulled `netboxcommunity/netbox:v4.5.8`.
- Rebuilt local Docker images against NetBox `4.5.8`.
- Applied local NetBox database migrations to `4.5.8`.
- Verified `GET /api/status/` reports `netbox-version: 4.5.8`.
- `invoke check`
- Targeted status/docs/table regression tests.
- `pre-commit run --all-files`
- `invoke harness-check`
- `invoke harness-test`
- `invoke sensitive-check`
- `PATH="$PWD/.venv-release/bin:$PATH" invoke docs`
- `invoke scenario-test`
- `invoke test`
- `invoke playwright-test`
- `PATH="$PWD/.venv-release/bin:$PATH" invoke ci`

## Rollback

Revert `development/.env` to the previous NetBox tag and revert the status/table/docs-url behavior changes. Remove this plan if no production code changes remain.

## Decision Log

- Rejected transform-map and endpoint/filter architecture because Forward NQE remains the authoring and shaping boundary.
- Rejected setting scheduled syncs to `QUEUED` on every recurring save because it hides the result of the last real ingestion.
- Rejected plugin-local docs links because this project publishes external docs and does not deploy per-model local NetBox docs pages.
