# LAG-cabled interface merge release

## Goal

A branch that converts a cabled interface into a LAG must merge cleanly. Before
this change the merge failed with
`ValidationError({'type': ['Link Aggregation Group (LAG) interfaces cannot have
a cable attached.']})`, leaving the branch `ready` for retry and the ingestion
recorded as `2 applied, 1 failed`.

## Root cause

`ObjectChange.get_merge_data()` only emits fields that differ from the change's
prechange snapshot.

`bulk_orm_apply_interface._apply_cabled_lag_row` deletes the interface's cable,
clears `cable_id`, calls `forget_lookup_object`, then re-applies the row through
the same bulk path. The re-apply resolved a freshly fetched interface and
snapshotted it *after* the cable was already gone, so both the prechange and
postchange sides recorded `cable: None`. The cable release cancelled out and
never reached the merge payload — the staged update carried only
`{'type': 'lag'}`.

Merge then applied `type=lag` to a main row that still had its cable attached,
which NetBox rejects.

## Constraints

- Preserve the protected `Interface.cable` FK release ordering already provided
  by `_add_destination_fk_release_dependencies`; a merge must not silently drop
  a legitimate delete.
- No change to merge ordering or branch semantics.
- Behavior for interfaces that keep their cable is unchanged.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py`

## Approach

`forward_netbox/utilities/apply_engine_bulk.py`

- Snapshot the interface in `_apply_cabled_lag_row` while the cable is still
  attached, before `cable.delete()`.
- Carry that snapshot into the recursive apply through a new
  `prechange_snapshots` keyword, so `forget_lookup_object` re-resolving the
  instance no longer discards it.
- `_snapshot_once` prefers a carried snapshot over taking a fresh one.

The staged update now contains both the LAG conversion and the cable release,
and merge applies them together.

## Rejected alternatives

- **Dependency edge in `bulk_merge`** ordering the interface update after the
  cable delete: produces `Exception: Cycle detected in dependency graph`.
  `_add_destination_fk_release_dependencies` already adds the opposite edge so
  the protected `Interface.cable` FK is released before the cable is removed,
  which is correct; inverting it is circular.
- **Snapshotting earlier without carrying the snapshot**: no effect, because the
  recursive apply re-resolves the interface and re-snapshots it.

## Validation

- `test_bulk_lag_cabled_parent_stages_and_production_merges_atomically`: passes.
- `Phase4BulkStageTest` + `ForwardSyncModelTest`: 96 tests, all pass.
- Diagnosis used a temporary dump of `get_merge_data()` at the merge apply site;
  the recorded ingestion issue is intentionally type-only
  (`safe_operation_failure`), so field-level errors are not visible in support
  evidence. That instrumentation was removed.

## Rollback

Revert `forward_netbox/utilities/apply_engine_bulk.py`. The change is confined
to `_apply_cabled_lag_row`, `_snapshot_once`, and the new optional
`prechange_snapshots` keyword on `bulk_orm_apply_interface`, which defaults to
`None` and leaves every other caller unchanged. Reverting restores the previous
behavior, including the merge failure this fixes.

## Decision Log

- Fix the staged payload rather than merge ordering: the update was incomplete,
  not misordered. Ordering was already correct and inverting it cycles.
- Carry the snapshot explicitly instead of removing `forget_lookup_object`:
  the lookup cache still needs to drop the stale cabled instance, so the
  snapshot is threaded through rather than relying on cache reuse.
- Keep the merge issue message type-only. The redaction hid the field-level
  error during diagnosis, but support evidence must not carry customer data;
  temporary instrumentation was used instead and removed.

## Completion Evidence

- `test_bulk_lag_cabled_parent_stages_and_production_merges_atomically`: passes.
- `Phase4BulkStageTest` + `ForwardSyncModelTest`: 96 tests, all pass.
