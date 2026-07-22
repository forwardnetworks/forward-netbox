# 2026-05-23 Shard Fetch Partition Parallelism

## Goal

Reduce Branching stage runtime for shard-scoped workloads by parallelizing
partitioned Forward NQE fetches within a single model step while preserving the
existing row-shape and shard-filter semantics.

## Constraints

- Keep NQE as the only normalization layer.
- Keep NetBox Branching orchestration unchanged.
- Do not change row contracts or diff semantics.
- Keep deterministic row merge order across partitioned fetches.
- Reuse existing source-level fetch concurrency control; no new dependency.

## Touched Surfaces

- `forward_netbox/utilities/query_fetch_execution.py`
- `forward_netbox/tests/test_sync.py`

## Approach

1. Add a shared partition-fetch helper in `ForwardQueryFetcher`.
2. Use that helper for both:
   - full shard-scoped query execution (`run_nqe_query`)
   - shard-scoped diff execution (`run_nqe_diff`)
3. Use existing `query_fetch_concurrency` resolution for worker count.
4. Preserve deterministic result order by collecting concurrent partition
   results by original partition index before flattening.
5. Keep existing fallback behavior unchanged when scoped fetch fails.

## Validation

- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_partitions_large_column_filter_batches forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_partitions_large_column_filter_diff_batches forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_marks_full_fallback_when_shard_fetch_fails forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_reports_fetch_metadata_for_column_filter_scope`

## Rollback

- Revert `query_fetch_execution.py` partition helper usage and restore sequential
  per-partition loops in `_fetch_spec_rows`.

## Decision Log

- Chose intra-step partition parallelism instead of raising branch budgets or
  increasing shard sizes.
- Kept partition row ordering deterministic to avoid introducing hidden behavior
  changes in downstream apply/delete paths.
