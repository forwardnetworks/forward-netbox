# Inventory Component Normalization

## Goal

Import Forward hardware components as NetBox-native `dcim.inventoryitem` rows only when the NQE data model exposes reliable inventory fields, and avoid duplicate generic inventory rows when the same component class is modeled as native `dcim.module`.

## Constraints

- Keep NQE as the component classifier and normalizer.
- Do not store customer identifiers, network IDs, snapshot IDs, or sample rows.
- Keep NetBox as the system of record for available model fields; do not invent side tables for lifecycle data.
- Preserve the existing inventory-item map as the default low-friction path.

## Touched Surfaces

- `forward_netbox/queries/forward_inventory_items.nqe`
- `forward_netbox/queries/forward_modules.nqe`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`
- User/reference docs generated or maintained for built-in query behavior

## Approach

Use the Forward `DevicePart` schema as the contract: name, part ID, serial number, part type, description, version ID, and optional lifecycle support data. Store only fields that map naturally to NetBox inventory items. Stop synthesizing serial and part IDs from fallback values. Exclude non-hardware application rows in NQE.

Emit part type and module-candidate hints from the inventory query. When `dcim.module` is enabled, the inventory adapter removes any matching generic inventory item for module-native component classes and skips applying those rows, so line cards, supervisors, fabric modules, and routing engines have one native destination.

Add role color normalization in NQE and persist native InventoryItem `label`/`asset_tag` fields when present. Keep lifecycle support dates out of inventory item descriptions unless a future native/custom-field contract is added.

## Validation

- `python manage.py test forward_netbox.tests.test_query_registry forward_netbox.tests.test_models.ForwardNQEMapModelTest forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_dcim_inventoryitem_sets_native_optional_fields forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_dcim_inventoryitem_cleans_module_backed_rows_when_modules_enabled forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_dcim_module_creates_module_when_module_bay_exists forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_dcim_module_creates_missing_module_bay_natively --keepdb --verbosity 2` passed.
- The updated built-in inventory query executed through the plugin query loader against the live smoke dataset and returned 75,851 aggregate inventory rows, 0 application rows, and 3,864 module-candidate rows.
- Every live inventory row passed the updated `dcim.inventoryitem` row-shape contract with coalesce fallbacks `device/name/part_id/serial`, `device/name/part_id`, and `device/name`.
- The updated module query executed against the same live smoke dataset and returned 3,864 module rows.
- `invoke ci` passed, including harness checks, sensitive-content scan, pre-commit, Docker build/startup checks, Django checks, plugin tests, Playwright UI harness, docs build, and package build.

## Rollback

Restore the previous inventory query fallback behavior, remove the module-candidate skip from the inventory adapter, and reset inventory item coalesce defaults to the strict four-field identity if the cleaner identity causes unacceptable churn.

## Decision Log

- Chosen: blank unknown serial/part ID values instead of fabricating them from names or roles, because fake identifiers create misleading NetBox inventory.
- Chosen: keep lifecycle data out of descriptions for now, because NetBox `InventoryItem` has no native lifecycle fields and description churn would be noisy.
- Rejected: split inventory and module behavior into separate user-selected inventory queries, because it adds an operator step and makes UI workflow easier to misconfigure.
