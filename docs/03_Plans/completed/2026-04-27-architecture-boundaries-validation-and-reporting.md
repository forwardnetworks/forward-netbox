# Architecture Boundaries, Validation, And Reporting

## Goal

Move the remaining sync architecture toward explicit harness layers while preserving the existing NetBox-native UI/API sync path and Branching-backed review workflow.

## Constraints

- Keep large imports on the same native multi-branch path used by the UI, API, jobs, and smoke command.
- Block unsafe validation results before branch creation.
- Do not commit customer network IDs, snapshot IDs, credentials, or tenant labels.
- Keep NetBox-ready inventory shaping in NQE and Python focused on validation, planning, execution, and reporting.

## Touched Surfaces

- `forward_netbox/models.py`
- `forward_netbox/utilities/`
- `forward_netbox/api/`
- `forward_netbox/forms.py`
- `forward_netbox/views.py`
- `forward_netbox/tables.py`
- `forward_netbox/templates/`
- `forward_netbox/management/commands/`
- `forward_netbox/tests/`
- `docs/`

## Approach

- Added a query-fetch boundary for snapshot context resolution, preflight sample checks, full query execution, diff fallback, and per-model result metadata.
- Persisted per-model execution metadata on `ForwardIngestion`.
- Added `ForwardValidationRun` and `ForwardDriftPolicy` models.
- Added automatic validation before branch creation in the multi-branch executor.
- Added standalone sync validation through the UI/API/job path.
- Added drift-policy checks for processed snapshots, query failures, zero-row models, and destructive-change thresholds.
- Updated docs and boundary maps for the contracts, query fetch, planning, validation, execution, adapters, and reporting layers.

## Validation

- `python manage.py makemigrations forward_netbox --check --dry-run`
- `invoke check`
- targeted Django tests for sync/model changes
- full `invoke test`

## Rollback

Revert the validation/reporting migration, remove the new query-fetch and validation utilities, restore the previous multi-branch planner internals, and remove the validation/drift UI/API routes. Existing pre-change ingestion records remain valid because the new ingestion fields are nullable or defaulted.

## Decision Log

- Rejected a separate large-import method because the supported workflow must remain the native NetBox UI/API sync path.
- Rejected branch creation before validation because blocked policy results should not allocate Branching branches.
- Rejected normalizing query output in Python because row shape and identity must remain explicit NQE contracts.
- Deferred adapter extraction because `sync.py` needs a separate behavior-preserving test pass before file movement.

## Remaining Follow-Up

- Add Playwright screenshots for the new validation and drift-policy UI after the browser gate is stable.
- Keep adapter extraction in a separate behavior-preserving plan.
- Add force-override workflow only if operators need to override a blocked validation in a branch-backed review flow.
