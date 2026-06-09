# Sync Failure Summary Surface

## Goal

Show a compact, actionable failure summary on the sync and execution-run detail
pages, and in the list rows, when the latest execution run fails so operators can
see the failing shard, query reference, and error text without digging through
step JSON. Also expose query_id/query_path directly in the execution-step table
so triage does not require opening the step detail page.

## Constraints

- Keep the change read-only in presentation layers.
- Do not change sync execution behavior or retry logic.
- Preserve existing execution-step details for deeper debugging.

## Touched Surfaces

- `forward_netbox/views.py`
- `forward_netbox/templates/forward_netbox/forwardsync.html`
- `forward_netbox/templates/forward_netbox/forwardexecutionrun.html`
- `forward_netbox/templates/forward_netbox/inc/execution_failure_banner.html`
- `forward_netbox/tables.py`
- `forward_netbox/utilities/execution_ledger_serialization.py`
- `forward_netbox/tests/test_models.py`
- `forward_netbox/tests/test_log_export.py`

## Approach

Derive a small failure summary from the latest execution run and render it as an
alert on the sync and execution-run detail pages plus a compact list-row summary.
Prefer the first failed step's model, shard index, query reference, and error
text. Fall back to run-level error text when step-level text is missing. Surface
the execution-step query_id/query_path fields in the table view.

## Validation

- Add sync-detail, run-detail, list-row, step-table, and support-bundle
  regressions for the failure summary and query-reference columns.
- Run the targeted model and template tests.
- Run repo lint and harness checks.

## Rollback

Remove the view context, template alert, helper, and regression test.

## Decision Log

- Chose a compact summary over reworking the step JSON panel because the issue is
  discoverability, not data loss.
- Added the summary to the support bundle as well so support and operators see
  the same failure context.
