# Forward Native Validation And Drift

## Goal

Add a NetBox-native preflight workflow that validates Forward snapshot data before branch creation, persists drift context, and gives operators a reviewable reason for allowing, blocking, or force-merging an ingestion.

## Constraints

- Keep NQE authoring inside Forward, not in the plugin.
- Keep the plugin NQE-first: Python validates identity, persists rows, and enforces policy; it does not reshape inventory output.
- Keep branch-backed review and merge as the default safety model.
- Keep manual merge as the default operator path.
- Preserve NetBox Branching semantics and stay within the supported NetBox `4.5.x` line.
- Do not introduce a query builder, transform-map editor, transform DSL, or general NQE authoring UI in NetBox.

## Touched Surfaces

- `forward_netbox/models.py`
- `forward_netbox/forms.py`
- `forward_netbox/views.py`
- `forward_netbox/tables.py`
- `forward_netbox/api/`
- `forward_netbox/jobs.py`
- `forward_netbox/utilities/`
- `forward_netbox/queries/`
- `forward_netbox/tests/`
- `docs/00_Project_Knowledge/`
- `docs/01_User_Guide/`
- `docs/02_Reference/`

## Approach

Introduce `ForwardValidationRun` as the first persistent workflow object. A validation run resolves the target snapshot, resolves a baseline snapshot when policy requires one, executes read-only checks for enabled NetBox models, records row counts and failures, computes drift/risk summaries, and stores the pass/fail decision before a branch is created.

Introduce `ForwardDriftPolicy` after validation runs are useful on their own. Each `ForwardSync` references one policy. The default baseline should be the most recent successfully merged ingestion for the sync. Policy fields should cover processed-snapshot requirements, query errors, zero rows for enabled models, snapshot health regression, destructive-change thresholds, and enabled/disabled state.

Wire validation into three entry points:

- standalone `Run Validation` action from the sync detail page
- automatic preflight phase of ad hoc ingestion
- automatic preflight phase of scheduled sync execution

Expose the results in the existing UI rather than hiding them in logs. The sync detail page should show the linked drift policy, latest validation run, and last-known snapshot resolution. The validation run detail page should show target/baseline snapshot IDs, snapshot health, enabled models, per-model counts, blocking reasons, and whether ingestion was allowed. The ingestion detail page should link to the validation run and show the preflight drift summary next to branch diff context.

Keep normal merge manual. Disable normal merge when gating rules fail. Add an explicit force-merge path only after validation and policy decisions are persisted; force merge must record actor, timestamp, reason, and the failed policy checks being overridden.

## Validation

- Unit tests for snapshot resolution and baseline selection.
- Model tests for drift policy validation.
- Workflow tests for validation pass, validation fail before branch creation, and validation pass leading to ingestion creation.
- UI/API tests for validation actions and visible blocking reasons.
- Scenario tests proving branch creation is skipped when validation blocks ingestion.
- Playwright coverage for the sync validation action and validation detail page.
- `invoke harness-check`
- `invoke harness-test`
- `invoke lint`
- `invoke check`
- `invoke scenario-test`
- `invoke test`
- `invoke playwright-test`
- `invoke docs`

## Rollback

Remove the validation and drift policy models, migrations, UI/API routes, jobs, utilities, tests, and documentation. Existing sync/ingestion records should continue to work after removing nullable validation references.

## Decision Log

- Rejected plugin-side NQE authoring because Forward remains the source of query logic and inventory shaping.
- Rejected Python-side normalization as a compatibility escape hatch because the current release direction is strict NQE output contracts.
- Rejected automatic merge as the default because branch-backed operator review is the primary safety model.
- Rejected merge blocking without a force-merge audit path because operators need an explicit break-glass workflow for known-acceptable drift.
