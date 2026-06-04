# API Usage Diagnostic Surfacing

## Goal

Expose the stored Forward API/NQE usage budget in support bundles and scale
benchmark reports so release and support workflows can inspect the same
structured evidence that sync completion records.

## Constraints

- Do not make additional live Forward API calls.
- Keep support-bundle payloads sanitized; include counters and budget status
  only.
- Preserve compatibility for old bundles that do not contain API usage evidence.
- Treat missing API evidence as informational in scale benchmark reports, not a
  release-blocking failure for historical artifacts.

## Touched Surfaces

- `forward_netbox/utilities/execution_ledger_serialization.py`
- `forward_netbox/utilities/scale_benchmark.py`
- `forward_netbox/tests/test_log_export.py`
- `forward_netbox/tests/test_scale_benchmark.py`
- `docs/00_Project_Knowledge/validation-matrix.md`

## Approach

Read `forward_api_usage` from the execution run's job data when exporting a
support bundle. Preserve the raw counter fields, attach or recompute the budget
evaluation, and record whether the evidence was available. Add a scale benchmark
check that passes, warns, or fails based on that budget when present and reports
missing historical evidence as info.

## Validation

- `invoke test-isolated --test-label='forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle forward_netbox.tests.test_scale_benchmark.ScaleBenchmarkReportTest'`
  - Passed: 13 tests.
- `invoke lint`
  - Passed.
- `invoke check`
  - Passed.
- `invoke docs`
  - Passed.
- `invoke harness-check`
  - Passed.

## Rollback

Remove the support-bundle `api_usage` section, remove the scale benchmark check,
and revert the validation-matrix wording. Sync runs will still retain raw API
usage in job data.

## Decision Log

- Missing API usage evidence is not a failure for old bundles because older sync
  jobs did not persist the budget payload.
- Support bundles whitelist only API usage counter fields and budget evaluation
  output; they do not export raw logs or request payloads through this section.
