# API Usage Health Surfacing

## Goal

Show the latest stored Forward API/NQE budget in the read-only Sync Health
summary so operators can see API pressure without first exporting a support
bundle or running scale benchmark.

## Constraints

- Do not make live Forward API calls from Sync Health.
- Reuse the support-bundle API usage sanitizer/evaluator.
- Keep historical runs without API usage evidence informational.
- Keep UI changes small and covered by health view tests.

## Touched Surfaces

- `forward_netbox/utilities/execution_ledger_serialization.py`
- `forward_netbox/utilities/health.py`
- `forward_netbox/templates/forward_netbox/forwardsync_health.html`
- `forward_netbox/tests/test_health.py`
- `docs/00_Project_Knowledge/validation-matrix.md`

## Approach

Expose the existing API usage support summary helper publicly, add it to
`sync_health_summary()`, and render a compact Sync Health card with budget
status, configured rate, observed rate, 429 count, and NQE call/page counts.

## Validation

- `invoke test-isolated --test-label='forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_view_renders_diagnostics'`
  - Passed: 2 tests.
- `invoke lint`
  - Passed.
- `invoke check`
  - Passed.
- `invoke docs`
  - Passed.
- `invoke harness-check`
  - Passed.

## Rollback

Remove the health summary `api_usage` field and template card. Support bundles
and scale benchmark continue to carry API usage evidence.

## Decision Log

- Health uses stored run job data only; live source checks remain explicit export
  actions.
