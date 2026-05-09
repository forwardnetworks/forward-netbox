# Fast Bootstrap Execution Backend

## Goal

Add an opt-in execution backend for large initial imports that uses the existing
Forward query, validation, adapter, and reporting contracts but writes directly
to NetBox instead of creating Branching shards.

This keeps the reviewable Branching path as the default while giving operators a
trusted-baseline path for inventories whose initial load is too large for
practical branch review.

## Constraints

- Branching remains the default and recommended steady-state backend.
- Fast bootstrap must be selected through the normal sync UI/API workflow.
- Fast bootstrap must not create a separate importer or bypass NQE validation,
  drift policies, coalesce contracts, row-level issue recording, or model
  adapters.
- Baseline readiness is marked only after the direct-write run completes.
- No customer identifiers, network IDs, snapshot IDs, credentials, or screenshots
  are committed.

## Touched Surfaces

- `forward_netbox/choices.py`
- `forward_netbox/forms.py`
- `forward_netbox/utilities/model_validation.py`
- `forward_netbox/utilities/sync_facade.py`
- `forward_netbox/utilities/sync_orchestration.py`
- `forward_netbox/utilities/sync_state.py`
- `forward_netbox/utilities/fast_bootstrap_executor.py`
- form, model, orchestration, and executor tests
- architecture and user guide documentation

## Approach

- Add `execution_backend` to sync parameters with `branching` as the default.
- Keep the existing Branching executor unchanged for reviewable syncs.
- Add a fast bootstrap executor that creates one branchless ingestion, runs
  preflight and drift validation, applies all enabled model workloads through
  the existing row adapters, runs deletes after upserts, and marks the ingestion
  baseline-ready only after successful completion.
- Keep row-level skip/failure reporting and model result reporting unchanged.

## Validation

- Form/model tests for backend persistence and display.
- Orchestration tests for backend dispatch.
- Executor tests for branchless baseline ingestion behavior.
- Targeted Django test cases while developing.
- Full harness/release gate before publishing.
- Fresh local NetBox reset and UI-driven fast-bootstrap smoke run against the
  configured live dataset before recommending release.

## Completion Evidence

- Added the `Fast bootstrap` execution backend behind the normal sync
  UI/API/management-command workflow; `Branching` remains the default.
- Fast bootstrap reuses `ForwardQueryFetcher`, `ForwardValidationRunner`,
  branch workload contracts, row adapters, model results, ingestion issues, and
  sync runtime state.
- Fast bootstrap now fails with `SyncError` and leaves `baseline_ready=False`
  when ingestion issues are recorded.
- The live large-model smoke run completed the `dcim.interface` phase with
  560151 rows written before exposing a NetBox cable constraint.
- Added a `dcim.cable` guard that skips and aggregates LAG endpoint rows because
  NetBox does not allow cables terminated directly to LAG interfaces.
- Verified the exact failed live cable row now returns a skip and does not
  create another ingestion issue.
- `invoke ci` passed locally after the fast path and cable guard changes.

## Rollback

Set sync `execution_backend` back to `branching`. The fast bootstrap backend is
isolated behind parameter validation and orchestration dispatch, so code rollback
removes the choice, form field, executor, and tests without changing existing
Branching semantics.

## Decision Log

- Rejected a separate management-command importer because it would create a
  second operator workflow and bypass the UI validation/reporting surfaces.
- Rejected making fast bootstrap the default because it removes Branching diff
  review and should be an explicit trusted-baseline decision.
