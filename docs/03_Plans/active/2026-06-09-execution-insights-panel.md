# Execution Insights Panel

## Goal

Surface a compact, operator-facing summary of Forward API usage and NQE query
mode mix on execution-run detail pages, backed by the existing telemetry we
already compute for support bundles and health summaries.

## Constraints

- Keep the change read-only in presentation layers.
- Reuse existing API usage and query-mode summaries.
- Do not change sync execution behavior or query accounting.
- Keep the panel compact and scan-friendly.

## Touched Surfaces

- `forward_netbox/utilities/execution_ledger_serialization.py`
- `forward_netbox/views.py`
- `forward_netbox/templates/forward_netbox/forwardexecutionrun.html`
- `forward_netbox/templates/forward_netbox/inc/execution_insights.html`
- `forward_netbox/tests/test_models.py`
- `forward_netbox/tests/test_log_export.py`

## Approach

Add a small execution-insights helper that summarizes:

- Forward HTTP attempt volume
- NQE query and diff call counts
- 429s and throttle sleep time
- budget status and headroom
- query-mode mix (`query`, `query_id`, `query_path`)
- fetch-mode mix
- top model results by volume

Render that summary on the execution-run detail page and include the same
compact summary in the support bundle so the evidence stays aligned.

## Validation

- Add a run-detail regression that renders the execution-insights panel.
- Add a support-bundle regression for the compact insights summary.
- Run the targeted model and export tests.
- Run repo lint and harness checks.

## Rollback

Remove the summary helper, panel include, view context, and regressions.

## Decision Log

- Chose a compact run-detail panel and support-bundle summary instead of a
  broader dashboard so the change stays close to the existing run evidence and
  does not create a new reporting surface.
