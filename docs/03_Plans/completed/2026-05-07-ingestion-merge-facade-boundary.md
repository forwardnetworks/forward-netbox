# Ingestion Merge Facade Boundary

## Goal
Move the remaining `ForwardIngestion` merge bookkeeping out of `forward_netbox/models.py` and into the existing ingestion merge utility module while keeping the model methods as thin wrappers.

## Constraints
- Keep `ForwardIngestion.enqueue_merge_job()`, `record_change_totals()`, `_cleanup_merged_branch()`, and `sync_merge()` as public model entrypoints.
- Preserve merge status transitions, change-count persistence, and branch cleanup behavior exactly.
- Do not change merge orchestration semantics or the review/auto-merge workflow.
- Keep the NetBox-native UI/API workflow unchanged.

## Touched Surfaces
- `forward_netbox/models.py`
- `forward_netbox/utilities/ingestion_merge.py`
- `forward_netbox/tests/test_ingestion_merge.py`
- `forward_netbox/tests/test_models.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Validation
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Approach
Extract the merge-specific helper logic into `forward_netbox/utilities/ingestion_merge.py` and keep the model methods as wrappers so the existing public flow remains stable.

## Decision Log
- Chosen: reuse the existing ingestion merge module instead of creating another utility file.
- Chosen: keep the cleanup helper inside the merge utility because branch deletion is part of merge completion, not a generic model concern.
- Rejected: leaving merge bookkeeping inline in `models.py` because the model is still carrying more orchestration than it should.

## Rollback
Restore the merge helper calls into `ForwardIngestion` if the split changes merge timing, branch cleanup, or change-count persistence.
