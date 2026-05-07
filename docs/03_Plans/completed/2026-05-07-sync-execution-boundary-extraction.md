# Direct Sync Execution Boundary Extraction

## Goal

Move the legacy non-branch sync-stage loop out of `forward_netbox/utilities/sync.py` and into a dedicated execution module while preserving the existing query, apply, delete, and sync-mode behavior.

## Constraints

- Preserve the current direct-run behavior for tests and any legacy call sites.
- Keep the change NetBox-native and Branching-native.
- Do not alter row validation, diff fallback, delete ordering, or sync-mode selection.
- Keep customer data and snapshot identifiers out of committed examples.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_execution.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_synthetic_scenarios.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach

Move the direct sync-stage orchestration into `forward_netbox/utilities/sync_execution.py` and keep `ForwardSyncRunner.run()` as a compatibility wrapper. Reuse the existing row-validation, query resolution, and adapter behavior through the runner API so the refactor is mechanical rather than semantic.

## Validation

- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback

Restore the `ForwardSyncRunner.run()` body in `forward_netbox/utilities/sync.py` and remove the execution module if the extraction changes behavior.

## Decision Log

- Rejected keeping the direct sync loop inline in `sync.py` because the module is already a compatibility boundary for the larger 0.7.0 cleanup.
- Rejected changing the direct-run semantics to use a different fetch/apply flow because the goal is to keep behavior pinned while the code moves behind a smaller boundary.
