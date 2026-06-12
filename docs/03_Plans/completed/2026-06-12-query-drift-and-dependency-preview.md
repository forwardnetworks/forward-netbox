# Query Drift And Dependency Preview Tranche

## Goal

Make query governance and dependency planning visible before operators start or
rerun a sync:

- show query drift status and repair actions on the sync detail page
- provide a repair/update action for enabled query ID bindings
- provide a read-only dependency dry-run preview that reuses the Branching
  planner and does not create branches or ingestion rows

## Constraints

- Do not release or tag this tranche yet.
- Preserve query ID and repository-path execution as the canonical path for
  Forward diffs.
- Keep the dry-run read-only; it may fetch Forward rows to build the same plan a
  sync would build, but it must not enqueue jobs, create branches, create
  ingestions, or mutate sync state.
- Keep support and UI summaries count-based. Do not persist or export raw
  customer rows.
- Keep actions permissioned through existing NetBox permissions.

## Touched Surfaces

- `forward_netbox/views.py`
- `forward_netbox/templates/forward_netbox/forwardsync.html`
- `forward_netbox/templates/forward_netbox/forwardsync_dependency_preview.html`
- `forward_netbox/tests/test_health.py`
- `forward_netbox/tests/test_sync.py` or view-focused tests as appropriate

## Approach

1. Reuse `sync_health_summary()` for the sync detail page so operators see
   query drift status without opening Health first.
2. Reuse `ForwardSyncRefreshQueryIdsView` as the repair/update action and make
   the action visible near the query drift status.
3. Add a `ForwardSyncDependencyPreviewView` that calls the Branching planner in
   plan-only mode, builds the existing `build_plan_preview()` payload, and
   renders a compact summary plus JSON evidence.
4. Surface the preview link on the sync page near ingestion controls and
   workload preview.
5. Add tests that prove the visible context and dependency preview path use the
   planner without starting a sync or creating branch/ingestion side effects.

## Validation

- Focused Django tests for query drift UI context and dependency preview.
- Existing plan-preview tests continue to prove delete dependency summaries.
- `invoke harness-check`
- `invoke test` or the smallest isolated test subset while iterating, followed
  by broader local gates if the touched surface warrants it.

Validation evidence captured for this tranche:

- `invoke test-isolated --test-label forward_netbox.tests.test_health.ForwardSyncHealthTest`
  passed with 33 tests.
- `invoke lint` passed.
- `invoke harness-check` passed.
- `invoke harness-test` passed with 127 tests.
- `invoke docs` passed.
- `invoke check` passed.

## Rollback

Remove the dependency preview view/template, remove the sync-detail status and
action additions, and keep the existing Health-tab query drift export/refresh
behavior unchanged.

## Decision Log

- Use the existing Branching planner instead of a new dependency estimator so
  the preview cannot drift from sync behavior.
- Keep repair/update as an explicit operator action instead of silently
  rewriting query IDs during page render.
- Keep live drift export separate from repair/update because export is
  read-only diagnostics while refresh mutates map bindings.
