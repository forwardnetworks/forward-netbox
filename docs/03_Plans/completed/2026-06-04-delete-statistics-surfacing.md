# Delete Statistics Surfacing

## Goal

Make staged delete activity visible in ingestion statistics while a shard is running, instead of only relying on merge-time branch diff totals.

## Scope Completed

- Successful row deletes now increment persisted `ForwardIngestion.applied_change_count` and `deleted_change_count` during staging.
- Skipped and failed delete rows remain excluded from persisted delete totals.
- Ingestion list/detail annotations now use the larger of persisted counters and branch `ChangeDiff` counters, so running shards stay visible even when branch diffs lag.
- Regression coverage was added for delete-row counter persistence and branch-diff-lag UI annotation.

## Constraints

- Do not add extra Forward API or NQE calls.
- Do not change delete eligibility or dependency-skip behavior.
- Keep accounting updates lightweight during large delete shards.
- Do not expose customer-specific sync names, users, screenshots, or dataset identifiers in repo docs.

## Touched Surfaces

- `forward_netbox/utilities/sync_reporting.py`
- `forward_netbox/views.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_models.py`
- `docs/03_Plans/completed/2026-06-04-delete-statistics-surfacing.md`

## Approach

Persist successful delete totals from the existing delete loop in batches, using `F()` updates so large shards avoid per-row database writes and concurrent refreshes see monotonic totals. Keep skipped and failed delete rows out of `deleted_change_count`.

Render ingestion statistics as the larger of persisted counters and branch `ChangeDiff` counters. This preserves merge-time diff counts while making active or branchless shard progress visible in the UI.

## Rollback

Revert the `sync_reporting.py` delete-counter helper and the `views.py` `Greatest(...)` annotations. The system will return to merge/diff-only delete visibility, with row delete progress still present in job logs.

## Decision Log

- Chose persisted live delete counters instead of adding new UI-only log parsing because list/detail/API surfaces already consume `ForwardIngestion` counters.
- Chose batched counter updates at the heartbeat row interval to avoid writing the ingestion row for every deleted object.
- Chose `Greatest(persisted, branch_diff)` instead of replacing branch diff counts so existing merged/diff-derived totals stay authoritative when they are higher.

## Validation Evidence

- `invoke test-isolated --test-label forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_delete_model_rows_persists_successful_delete_statistics --project-name forward-netbox-test-delete-stats` passed.
- `invoke test-isolated --test-label "forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_delete_model_rows_records_row_failure_and_continues forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_delete_model_rows_records_dependency_skip_as_skipped_info forward_netbox.tests.test_models.ForwardIngestionSnapshotSummaryTest.test_annotate_statistics_uses_persisted_counts_when_branch_missing forward_netbox.tests.test_models.ForwardIngestionSnapshotSummaryTest.test_annotate_statistics_uses_persisted_counts_when_branch_diffs_lag" --project-name forward-netbox-test-delete-stats` had both delete tests pass; the first run used an incorrect model-test class label.
- `invoke test-isolated --test-label "forward_netbox.tests.test_models.ForwardIngestionSnapshotSummaryTest.test_annotate_statistics_uses_persisted_counts_when_branch_missing forward_netbox.tests.test_models.ForwardIngestionSnapshotSummaryTest.test_annotate_statistics_uses_persisted_counts_when_branch_diffs_lag" --project-name forward-netbox-test-delete-stats` passed.
- `invoke check` passed.
- `env PATH="$PWD/.venv/bin:$PATH" invoke lint` passed.
