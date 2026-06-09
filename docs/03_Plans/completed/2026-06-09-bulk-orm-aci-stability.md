# Bulk ORM No-Op Stats and ACI Platform Stability

## Goal

Reduce false-positive "updated" accounting on bulk-ORM-backed syncs and make
ACI platform classification stable when Forward exposes ACI command families
for Cisco NX-OS ACI devices.

## Constraints

- Keep the change behaviorally narrow.
- Preserve existing no-op semantics for row application.
- Keep ACI detection source-backed and generic.
- Avoid extra Forward API calls.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py`
- `forward_netbox/queries/netbox_utilities.nqe`
- `forward_netbox/queries/forward_devices.nqe`
- `forward_netbox/queries/forward_devices_with_netbox_aliases.nqe`
- `forward_netbox/queries/forward_platforms.nqe`
- `forward_netbox/queries/forward_aci_fabrics.nqe`
- `forward_netbox/tests/test_apply_engine.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`

## Approach

- Count unchanged bulk-ORM rows as `unchanged` instead of `applied`.
- Add a shared ACI detector that uses Forward command inventory plus the
  existing platform heuristic.
- Reuse that detector for native NetBox device/platform imports and ACI fabric
  discovery.

## Validation

- Added a bulk-ORM regression for unchanged platform rows.
- Added query-contract regressions for the shared ACI helper usage.
- Added a sync-level ACI platform repeat-run no-op regression.
- Added a tag-scope escaping regression for the query fetch path.
- Added a platform execution-summary regression for unchanged row counts.
- Ran targeted tests, `invoke lint`, and `invoke harness-check`.

## Rollback

- Restore the bulk-ORM statistics outcome and revert the shared ACI helper
  calls in the affected NQE queries.

## Decision Log

- Use a shared NQE helper instead of hard-coding ACI in multiple queries so
  the classification rule stays consistent across native NetBox and ACI
  discovery paths.
