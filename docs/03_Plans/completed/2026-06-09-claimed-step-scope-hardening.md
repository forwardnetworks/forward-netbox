# Claimed Step Scope Hardening

## Goal

Prevent shard execution drift during resume/retry by guaranteeing each plan step
executes using the in-memory claimed step metadata, and not a stale persisted plan
index lookup.

## Constraints

- Preserve existing branch execution semantics and status transitions.
- Do not alter user-visible branching behavior beyond removing stale index-based scope selection.
- Keep change compatible with existing sync plans and restart flows.

## Touched Surfaces

- `forward_netbox/jobs.py`
- `forward_netbox/utilities/multi_branch_executor.py`
- `forward_netbox/tests/test_jobs.py`
- `forward_netbox/tests/test_sync.py`

## Approach

1. Thread the claimed execution step object into `run_next_plan_item`.
2. Prefer a direct snapshot of the claimed step over persisted index resolution when
   building shard scope and plan metadata.
3. Keep persisted lookup as fallback only when no claimed step is available.
4. Add regression tests that verify shard scope comes from the claimed step and
   no stale persistence lookup is used in that path.

## Validation

- `invoke test-isolated --test-label=forward_netbox.tests.test_jobs.ForwardJobsTest`
- `invoke test-isolated --test-label=forward_netbox.tests.test_sync.ForwardMultiBranchExecutorAdaptiveSplitTest`
- `invoke harness-check`

## Rollback

- Revert the claimed-step threading in `run_next_plan_item` and restore index-only
  plan lookup semantics in `multi_branch_executor.py`.
- Remove the new regression assertions from the two test modules.

## Decision Log

- Chose claimed-step snapshotting over persisted-item lookup in the active claim path
  to avoid executing stale model/shard scope after step reuse.
