# Ingestion Merge Helper Extraction

## Goal
Move `ForwardIngestion.sync_merge` orchestration and branch-merge signal suppression into a dedicated utility module while preserving the current model API and merge semantics.

## Constraints
- Keep `ForwardIngestion.sync_merge()` as the public entrypoint.
- Preserve merge status transitions, branch cleanup, and branch-run-state updates.
- Do not alter the underlying `merge_branch()` behavior in this tranche.
- Keep the change NetBox-native and compatible with the current Branching workflow.

## Touched Surfaces
- `forward_netbox/models.py`
- `forward_netbox/utilities/ingestion_merge.py`
- `forward_netbox/tests/test_models.py`
- `forward_netbox/tests/test_ingestion_merge.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach
- Extract the `sync_merge` orchestration into `ingestion_merge.py`.
- Move branch merge signal suppression into the same utility module.
- Leave `ForwardIngestion.sync_merge()` as a delegating wrapper.
- Add focused regression tests for the helper and the wrapper behavior.
- Update architecture and debt notes so the merge lifecycle boundary is explicit.

## Validation
- `invoke harness-check`
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback
- Restore `forward_netbox/models.py` from version control and remove `ingestion_merge.py`.
- No data migration or persistent state change is expected.

## Decision Log
- `ForwardIngestion.sync_merge` is the other remaining overgrown lifecycle boundary in `models.py`.
- Keeping the wrapper avoids a public API break while making the merge orchestration testable in isolation.
- This tranche is intentionally limited to orchestration and signal suppression, not the low-level merge algorithm.
