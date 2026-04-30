# NQE Device-Parallel Query Shape

## Goal

Keep shipped high-volume NQE maps in Forward's device-first query shape so eligible maps can use automatic per-device execution on large networks.

## Constraints

- Keep row shaping in NQE rather than adding plugin-side transforms.
- Preserve existing NetBox model fields and coalesce identities.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or sampled rows.
- Leave complex row-union queries alone unless row-count parity is proven.

## Touched Surfaces

- `forward_netbox/queries/forward_device_models_with_netbox_aliases.nqe`
- `forward_netbox/queries/forward_devices_with_netbox_aliases.nqe`
- `forward_netbox/queries/forward_device_feature_tags_with_rules.nqe`
- `forward_netbox/queries/forward_virtual_chassis.nqe`
- `forward_netbox/queries/forward_inventory_items.nqe`
- `forward_netbox/tests/test_query_registry.py`
- `docs/02_Reference/built-in-nqe-maps.md`
- `docs/02_Reference/device-type-alias-data-file.md`
- `docs/02_Reference/feature-tag-rules-data-file.md`

## Approach

1. Move optional data-file bindings after `foreach device in network.devices`.
2. Remove simple `foreach row in (...) select distinct row` wrappers from virtual chassis and inventory item queries.
3. Keep `select distinct` on the projected record so output identity remains unchanged.
4. Add query-registry regression coverage for the device-first data-file and wrapper-free shapes.
5. Document the device-first constraint for operators who copy the shipped queries into the Forward Org Repository.

## Validation

- Focused query registry tests for data-file and wrapper-free query shape passed.
- Full `forward_netbox.tests.test_query_registry` passed.
- `invoke harness-test` passed.
- `PATH=.build-venv/bin:$PATH invoke docs` passed.
- `invoke check` passed.
- `invoke test` passed.
- `invoke lint` passed.
- Live limited NQE smoke checks passed for the changed query files without printing source rows or private identifiers.

## Rollback

Revert the query files, docs, and query registry tests in this plan. No database migration or NetBox object cleanup is required.

## Decision Log

- Rejected: rewrite interface and IP address union queries in this pass.
  - Reason: those queries have more complex row-union and dedupe semantics and need separate row-count parity validation.
- Rejected: add plugin-side transforms to compensate for slow NQE shape.
  - Reason: query logic should remain native NQE and stay usable through raw query or query ID workflows.
