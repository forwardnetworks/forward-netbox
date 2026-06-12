# 1.4.1.1 Merge And Interface Hardening

## Goal

Prevent `dcim.module` sync from creating `dcim.modulebay` side-effect changes
that can fail during Branching merge with `Save with update_fields did not
affect any rows`, and prevent `dcim.interface` LAG member rows from clearing
existing parent interface descriptions during repeat syncs.

## Constraints

- Preserve native NetBox and Branching behavior.
- Do not create module bays through raw SQL or hidden side channels.
- Keep missing module-bay handling non-blocking so the rest of the sync can
  merge.
- Keep module readiness as the operator path for missing bays.
- Keep cross-shard LAG parent creation available without letting member rows
  mutate existing parent descriptive or operational fields.

## Touched Surfaces

- `forward_netbox/utilities/sync_inventory_module.py`
- `forward_netbox/utilities/sync_runner_adapters.py`
- `forward_netbox/management/commands/forward_module_readiness.py`
- `forward_netbox/tests/test_sync.py`
- module import user and reference docs
- `forward_netbox/utilities/sync_interface.py`
- release compatibility docs

## Approach

1. Stop creating missing `ModuleBay` objects as a side effect of applying a
   `dcim.module` row.
2. Treat missing module bays as non-blocking skipped module rows with an
   aggregated warning.
3. Keep existing module bay and module upserts unchanged when the bay already
   exists.
4. Update readiness and docs so operators import generated module bays before
   enabling module sync.
5. Change LAG placeholder upserts to use separate create/update values so
   missing parents can still be created while existing parents keep their
   description, MTU, and speed.
6. Add a repeat-sync regression where a member row references an existing LAG
   with a description and the second apply remains a no-op.

## Validation

- `rtk .venv/bin/invoke test-isolated --test-label forward_netbox.tests.test_sync.ForwardSyncRunnerTest`
- `rtk .venv/bin/invoke test-isolated --test-label forward_netbox.tests.test_module_readiness`
- `rtk .venv/bin/invoke lint`

## Rollback

Revert this change to restore automatic module-bay creation during
`dcim.module` sync. If rolled back, large module imports may again emit
`dcim.modulebay` create changes during module shards.

## Decision Log

- Chosen: require pre-existing module bays and skip missing-bay module rows.
- Rejected: retrying failed Branching merges, because the failure is caused by
  the model contract emitting side-effect creates during merge.
- Rejected: raw or hidden module-bay creation, because it would bypass native
  Branching review.
