## Goal

Make the optional `dcim.module` query normalize Forward component output into the native NetBox module shape, using the live smoke dataset as validation evidence.

## Constraints

- Keep NQE as the source of truth for component classification.
- Keep Python limited to native NetBox lookup/apply behavior.
- Do not move SFP/transceiver rows into `dcim.module` unless the query can prove a native module-bay shape.
- Keep the inventory-item fallback valid for generic components.

## Touched Surfaces

- `forward_netbox/queries/forward_modules.nqe`
- `forward_netbox/tests/test_query_registry.py`
- `docs/02_Reference/model-mapping-matrix.md`
- `docs/02_Reference/built-in-nqe-maps.md`

## Approach

Use `DevicePartType` in NQE to keep the module query focused on chassis module classes. Include line cards, supervisors, and fabric modules. Leave chassis, fans, power supplies, stack artifacts, applications, motherboards, and transceivers to the inventory-item path.

## Validation

- Executed the shipped `forward_modules.nqe` query against the live smoke dataset through the plugin's public `/api/nqe` client path.
- Live smoke dataset component distribution sampled 76,498 component rows, including 36,719 transceiver rows.
- Shipped module query returned 3,864 module rows and no `Transceiver Slot` module-bay rows.
- Focused Django tests passed:
  - `forward_netbox.tests.test_query_registry`
  - `forward_netbox.tests.test_forms`
  - `forward_netbox.tests.test_models`
  - `forward_netbox.tests.test_sync`
- `invoke lint`

## Rollback

Remove the module classifier helper and restore the previous broad component query if a real customer dataset proves the narrower mapping misses required bay-aware modules.

## Decision Log

- Chosen: conservative module types only, because the live smoke dataset shows transceivers dominate the component stream and should not be imported as NetBox modules by default.
- Chosen: keep optics in the inventory-item path unless a customer-specific query maps them to known module bays.
