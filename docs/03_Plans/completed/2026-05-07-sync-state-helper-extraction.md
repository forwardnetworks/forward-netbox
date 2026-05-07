# Sync State Helper Extraction

## Goal
Move Forward sync state and presentation helpers out of `forward_netbox/models.py` into a dedicated utility module while preserving the current model API.

## Constraints
- Keep `ForwardSync` and `ForwardIngestion` public methods/properties working through delegation.
- Preserve existing branch-run state serialization and sync status semantics.
- Do not change merge behavior or job orchestration in this tranche.
- Keep the change NetBox-native and compatible with the current Branching workflow.

## Touched Surfaces
- `forward_netbox/models.py`
- `forward_netbox/utilities/sync_state.py`
- `forward_netbox/tests/test_models.py`
- `forward_netbox/tests/test_sync_state.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach
- Extract branch-run state helpers, density helpers, and summary helpers into `sync_state.py`.
- Keep `ForwardSync` properties/methods as delegators so callers do not change.
- Add focused regression tests for the helper module and the model delegation surface.
- Update architecture and debt notes to reflect the new boundary.

## Validation
- `invoke harness-check`
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback
- Restore `forward_netbox/models.py` from version control and remove `sync_state.py`.
- No data migration or runtime configuration changes are expected.

## Decision Log
- `models.py` is still an overgrown boundary, but branch state helpers are the lowest-risk extraction with clear reuse.
- Keeping wrappers in `ForwardSync` avoids a public API break while shrinking the model module.
- This tranche deliberately stops short of splitting merge/job orchestration so behavior stays pinned.
