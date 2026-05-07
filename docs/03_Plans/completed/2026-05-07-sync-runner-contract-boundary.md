# Sync Runner Contract Boundary

## Goal

Thin `forward_netbox/utilities/sync.py` by moving the remaining runner contract and policy helpers into a dedicated helper module while preserving the current row-splitting and conflict-policy behavior.

## Constraints

- Preserve `ForwardSyncRunner` behavior.
- Keep diff row splitting, conflict policy selection, module-native inventory detection, and IP address skip reasoning unchanged.
- Do not change adapter semantics or introduce new user-visible flow.
- Keep the existing `ForwardSyncRunner` import path valid.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_runner_contracts.py`
- `forward_netbox/tests/test_sync.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach

Move the remaining policy/contract helpers out of `sync.py` into a dedicated runner-contract mixin module, then make `ForwardSyncRunner` inherit from it. Keep `run()` and the main orchestration boundary in place.

## Validation

- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback

Restore the contract helpers to `sync.py` if the boundary split changes row splitting, skip logic, or conflict policy selection.

## Decision Log

- Rejected: moving the core runner orchestration into a new module in this tranche | the execution boundary is already stable enough.
- Rejected: changing any contract behavior while extracting the helpers | this pass is structural only.
