# API Usage Budget Hardening

## Goal

Make Forward SaaS API/NQE usage a regression surface that sync runs can report
directly, so release and support checks can identify unsafe pacing, 429
evidence, and NQE call volume without parsing log text by hand.

## Constraints

- Preserve the existing Forward API counters and sync log payload shape.
- Treat Forward SaaS pacing as stricter than custom/on-prem deployments.
- Do not add live Forward calls to health, docs, or local validation checks.
- Keep the evaluator pure and deterministic so it can be reused by future
  release gates and support-bundle checks.

## Touched Surfaces

- `forward_netbox/utilities/api_usage.py`
- `forward_netbox/utilities/sync_orchestration.py`
- `forward_netbox/tests/test_api_usage.py`
- `forward_netbox/tests/test_sync_orchestration.py`
- `docs/00_Project_Knowledge/validation-matrix.md`

## Approach

Add a small API usage evaluator that consumes the existing
`ForwardClient.api_usage_summary()` payload. It classifies usage as `passed`,
`warning`, or `failed`, exposes the metrics that drove the decision, and applies
SaaS-specific warnings for disabled pacing. Sync finalization stores the
evaluator result under the existing API usage summary and includes the status in
the operator log entry.

## Validation

- `invoke test-isolated --test-label='forward_netbox.tests.test_api_usage forward_netbox.tests.test_sync_orchestration.ForwardSyncOrchestrationHelperTest.test_record_forward_api_usage_stores_summary_and_log'`
  - Passed: 8 tests.
- `invoke harness-check`
  - Passed.
- `invoke harness-test`
  - Passed: 127 tests.
  - Restored unrelated generated `release-readiness-audit.json` fixture churn.
- `invoke lint`
  - Passed.
- `invoke check`
  - Passed.
- `invoke docs`
  - Passed.

## Rollback

Remove the evaluator module, remove the budget field from sync API usage
logging, and revert the validation-matrix guidance. Existing raw counters remain
the fallback evidence surface.

## Decision Log

- Do not derive actual per-minute throughput from counters in this tranche; the
  current summary does not include elapsed wall time. The first durable gate is
  configured pacing plus hard evidence such as 429 failures.
- Apply the Forward SaaS hard-block budget to Forward SaaS and unknown source
  types. Explicit custom deployments retain raw counter reporting without SaaS
  rate-budget warnings.
