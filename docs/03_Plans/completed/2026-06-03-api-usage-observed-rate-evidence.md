# API Usage Observed Rate Evidence

## Goal

Record observed Forward HTTP attempt rate in the existing API usage summary so
support bundles and scale benchmark reports can distinguish configured SaaS
pacing from measured runtime behavior.

## Constraints

- Do not change request pacing behavior.
- Do not make additional Forward API calls.
- Avoid noisy warnings for tiny syncs or short unit-test windows.
- Preserve compatibility for existing support bundles without observed-rate
  fields.

## Touched Surfaces

- `forward_netbox/utilities/forward_api_impl.py`
- `forward_netbox/utilities/api_usage.py`
- `forward_netbox/utilities/execution_ledger_serialization.py`
- `forward_netbox/utilities/scale_benchmark.py`
- API usage, Forward client, support-bundle, and scale benchmark tests
- `docs/00_Project_Knowledge/validation-matrix.md`

## Approach

Track first and last HTTP attempt monotonic timestamps inside `ForwardClient`.
Expose `usage_window_seconds` and `observed_http_attempts_per_minute` from
`api_usage_summary()`. Evaluate observed rate only when the sample contains
enough attempts and window duration; otherwise include the metric as evidence
without changing budget status.

## Validation

- `invoke test-isolated --test-label='forward_netbox.tests.test_forward_api.ForwardClientTest.test_api_usage_summary_counts_http_attempts_retries_and_429s forward_netbox.tests.test_forward_api.ForwardClientTest.test_reset_api_usage_summary_preserves_rate_limit_configuration forward_netbox.tests.test_forward_api.ForwardClientTest.test_api_usage_summary_reports_observed_http_attempt_rate forward_netbox.tests.test_api_usage forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle forward_netbox.tests.test_scale_benchmark.ScaleBenchmarkReportTest forward_netbox.tests.test_sync_orchestration.ForwardSyncOrchestrationHelperTest.test_record_forward_api_usage_stores_summary_and_log'`
  - Passed: 26 tests.
- `invoke lint`
  - Passed after Black reformatted `forward_netbox/tests/test_scale_benchmark.py`.
- `invoke check`
  - Passed.
- `invoke docs`
  - Passed.
- `invoke harness-check`
  - Passed.

## Rollback

Remove observed-rate fields from the client summary, evaluator, support-bundle
whitelist, and benchmark check. Configured-rate and 429 budget checks continue
to work.

## Decision Log

- Observed rate is calculated from HTTP attempt intervals, not wall-clock sync
  duration, because the rate-limit risk is request burst pressure.
- Observed rate only affects status when the sample is large enough to avoid
  false positives from short runs.
