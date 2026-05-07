# Sync Inventory And Module Boundary Extraction

## Goal

Extract the inventory-item and module adapter family from `forward_netbox/utilities/sync.py` into a dedicated module while preserving the existing runner-level API and behavior.

## Constraints

- Keep the public runner methods intact for existing tests and UI flows.
- Preserve module-native inventory cleanup, module bay creation, and skip semantics.
- Do not change NetBox-native branching behavior.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_inventory_module.py`
- `forward_netbox/tests/test_sync.py`
- `docs/03_Plans/technical-debt.md`
- `ARCHITECTURE.md`

## Approach

Move inventory-item and module apply/delete logic into `forward_netbox/utilities/sync_inventory_module.py` and keep `ForwardSyncRunner` methods as delegation shims. This continues the adapter boundary split without forcing a broader rewrite.

## Validation

- `invoke lint`
- `invoke test`
- `invoke ci`

## Rollback

Restore the inventory-item and module adapter methods inside `forward_netbox/utilities/sync.py` and remove `forward_netbox/utilities/sync_inventory_module.py`.

## Decision Log

- Chose the inventory/module family next because it is a stable, well-covered adapter surface and already has module-native cleanup behavior that benefits from isolation.
- Rejected a wider sync-module refactor in the same pass because the goal here is to preserve behavior while steadily shrinking the monolith.
