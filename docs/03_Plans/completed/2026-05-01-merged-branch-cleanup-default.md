## Goal

Clean up Forward-created branch schemas automatically after successful merges, including auto-merge runs, to avoid lingering temporary schemas in NetBox/PostgreSQL.

## Constraints

- Preserve current behavior for manual review mode where operators can still keep a branch by explicit choice.
- Keep merge state handling and baseline readiness logic unchanged.
- Avoid introducing non-native branch lifecycle mechanisms.

## Touched Surfaces

- `forward_netbox/models.py`
- `forward_netbox/jobs.py`
- `forward_netbox/tests/test_models.py`

## Approach

1. Add a single helper on `ForwardIngestion` to remove the linked branch safely (`_cleanup_merged_branch`).
2. Extend `ForwardIngestion.sync_merge()` with `remove_branch` (default `True`) and invoke cleanup after successful merge finalization.
3. Route queued merge jobs through the same `sync_merge(remove_branch=...)` path and set job default `remove_branch=True`.
4. Add model tests to verify default cleanup and explicit preservation (`remove_branch=False`).

## Rollback

- Revert this change set to restore prior behavior where only job-based merge with `remove_branch=True` deletes branches.

## Decision Log

- Chosen: make cleanup default-on after successful merge to match operator expectations and reduce DBA noise from temporary schemas.
- Rejected: adding a new plugin-wide setting now; not required for immediate operational fix and increases surface area.
- Rejected: retaining duplicate cleanup logic in jobs and models; central helper reduces divergence risk.

## Validation

- `invoke ci` (harness, tests, docs, packaging, and UI checks).
