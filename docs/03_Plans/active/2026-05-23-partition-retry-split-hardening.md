# 2026-05-23 Partition Retry Split Hardening

## Goal

Reduce shard-fetch fallback frequency by recovering from partition-scoped
column-filter query failures before escalating to full/model fallback.

## Constraints

- Preserve NQE row-shape contracts and local shard safety filtering.
- Keep deterministic row ordering across partitioned fetch merges.
- Preserve existing fallback behavior when retries cannot recover.
- Keep behavior native to current NetBox/Branching orchestration.

## Touched Surfaces

- `forward_netbox/utilities/query_fetch_execution.py`
- `forward_netbox/tests/test_sync.py`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- `docs/03_Plans/active/2026-05-23-partition-retry-split-hardening.md`

## Approach

1. Add deterministic split logic for `EQUALS_ANY` partition filters.
2. Wrap partition fetch execution with retry splitting for partition-scoped
   query failures.
3. Reuse the same retry path for full and diff shard fetches.
4. Add focused regression tests for full+diff recovery behavior.

## Implementation

- Added `_split_column_filter_partition()` for deterministic partition splitting.
- Updated `_fetch_partitioned_rows()` to:
  - catch partition-scoped query failures,
  - recursively split and retry recoverable partition batches,
  - preserve original partition-order merge behavior.
- This hardening applies to both full and diff shard fetches because both use
  `_fetch_partitioned_rows()`.

## Validation

- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardSyncRunnerTest`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_jobs forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest`
- `poetry run invoke harness-check`
- `poetry run invoke check`

## Rollback

- Revert split-retry wrapper in `_fetch_partitioned_rows()`.
- Restore prior behavior that escalates directly to full/model fallback on
  first partition failure.
- Keep partition chunking behavior unchanged.

## Decision Log

- The retry logic is intentionally bounded by partition decomposition (rather
  than global run retries) so recoverable partition failures avoid expensive
  whole-model fallback while preserving deterministic output behavior.
