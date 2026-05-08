# Stale Shard Progress Visibility

## Goal

Make a killed or stalled long-running shard obvious in the sync activity display instead of presenting the last heartbeat as if it were still active.

## Constraints

- Preserve NetBox-native and Branching-native sync behavior.
- Do not auto-clean branches or mutate sync state based only on a stale timestamp.
- Keep the change limited to operator visibility unless a later plan proves a safe recovery action.
- Do not commit customer identifiers, network IDs, snapshot IDs, or dataset-specific records.

## Touched Surfaces

- `forward_netbox/utilities/sync_state.py`
- `forward_netbox/tests/test_sync_state.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach

1. Add a stale-progress threshold for branch-run heartbeat display.
2. When an active sync has a progress heartbeat older than that threshold, surface it as stale and include the last reported activity.
3. Keep fresh heartbeat behavior unchanged.
4. Add focused regression tests for fresh and stale progress activity.
5. Document that heartbeat visibility distinguishes an active long shard from a dead/stale worker.

## Validation

- `invoke lint`
- `invoke test`
- `invoke docs`

## Rollback

Revert the sync-state display helper and tests. No data migration or persisted state cleanup is required.

## Decision Log

- Do not automatically mark stale syncs failed in this tranche; a stale timestamp alone is not enough proof to mutate production state.
- Do not delete stale branches automatically; cleanup can hide useful failure evidence.
