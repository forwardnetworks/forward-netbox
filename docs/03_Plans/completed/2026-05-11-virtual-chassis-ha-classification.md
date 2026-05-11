# Virtual Chassis HA Classification

## Goal

Stop the built-in NetBox virtual chassis map from treating Forward HA peer
relationships as NetBox `dcim.virtualchassis` memberships.

## Constraints

- Keep NQE as the source of truth for model selection and normalization.
- Keep NetBox-native `dcim.virtualchassis` support for custom queries that emit
  true virtual chassis rows.
- Do not commit customer identifiers, network IDs, snapshot IDs, or screenshots.
- Preserve the existing row guards so stale or custom maps fail clearly instead
  of aborting later models.

## Touched Surfaces

- `forward_netbox/queries/forward_virtual_chassis.nqe`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/utilities/sync_device.py`
- `docs/02_Reference/`
- `docs/01_User_Guide/troubleshooting.md`

## Approach

Forward exposes vPC, MLAG, and cluster peers as HA relationships between
control planes. Those are not the same thing as NetBox virtual chassis
membership. Forward exposes chassis internals through platform components, which
the plugin already maps into modules and inventory items.

Change the shipped virtual chassis query to a no-op query that still declares
the required NetBox fields. Keep Python-side validation for duplicate
`vc_position` and missing `vc_position` so custom or stale query-ID maps are
isolated with actionable errors.

## Validation

- Query registry tests prove the built-in map no longer references Forward HA
  fields.
- Sync tests prove duplicate virtual chassis positions fail the model preflight
  or row apply path without aborting later models.
- Run focused tests, lint, docs, and the repo harness gate.

Completed:

- `invoke test -- forward_netbox.tests.test_query_registry.QueryRegistryTest.test_virtual_chassis_query_does_not_map_ha_peers_by_default forward_netbox.tests.test_query_registry.QueryRegistryTest.test_wrapped_device_queries_keep_device_first_parallel_shape forward_netbox.tests.test_query_registry.QueryRegistryTest.test_built_in_maps_use_current_bundled_query_text forward_netbox.tests.test_sync.ForwardMultiBranchPlannerPreflightTest.test_duplicate_virtual_chassis_positions_skip_model_not_later_models forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_virtual_chassis_rejects_duplicate_position forward_netbox.tests.test_models.ForwardNQEMapTest.test_virtual_chassis_map_rejects_query_missing_position`
  - This expanded to the full plugin suite in the local NetBox harness: 288
    tests passed.
- `invoke lint`
- `invoke docs`
- `invoke harness-check`

## Rollback

Restore the previous `forward_virtual_chassis.nqe` and matching docs/tests. If
an environment used the no-op map and needs old behavior, pin a custom query
that emits `dcim.virtualchassis` rows explicitly.

## Decision Log

- Rejected mapping vPC or MLAG to NetBox virtual chassis: those are
  dual-control-plane HA constructs, not shared-control-plane stack membership.
- Rejected inventing virtual chassis membership in Python: the plugin boundary
  keeps source classification in NQE.
