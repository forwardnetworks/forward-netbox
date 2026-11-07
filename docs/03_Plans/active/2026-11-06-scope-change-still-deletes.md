# A Device Entering or Leaving Scope No Longer Stops Deletes

## Goal

Rows that disappear from Forward (interfaces, addresses, MACs, cables, modules,
inventory items, FHRP groups, BGP and OSPF rows) are deleted from NetBox on the
next sync, even when the set of in-scope devices changed in the same run.

Before this change, a device-scoped query was sent the in-scope device names
(`forward_netbox_shard_keys`), and those names were part of the durable
workload-state parameter hash. Any device entering or leaving scope made every
device-scoped model take the `contract_reset` path: it upserts and deletes
nothing, and it replaces the stored state, so the missing rows were forgotten and
never deleted later. A customer's device scope shifts most runs, so stale rows
accumulated for months (reproduced live: BGP peers in NetBox 150,387 against
130,037 returned by Forward after one scope change).

## Constraints

- No device delete may result from this: `dcim.device` keeps its own path
  (ownership quarantine), so it is excluded.
- Rows of a device that left scope are not Forward's to judge this run: they must
  be neither deleted nor forgotten.
- States stored by earlier releases must stay comparable when the scope did not
  change, so upgrading does not cost another run that deletes nothing.
- Any other parameter change (tags, endpoint options, query) still resets.

## Touched Surfaces

- `forward_netbox/utilities/workload_state.py`: `_parameter_hash` drops the scope
  names for device-scoped child models; `_legacy_parameter_hash` keeps the old
  hash for compatibility; missing rows of devices this run did not fetch are
  carried forward instead of deleted; new summary `out_of_scope_carried_rows`.
- `forward_netbox/utilities/query_fetch_execution.py`: the durable-state log line
  reports `out_of_scope_carried`.
- `forward_netbox/tests/test_workload_state.py`: `ScopeChangeStillDeletesTest`.

## Approach

1. `_scope_device_names(model, workloads)` returns the union of the scope names
   across the model's full workloads, or `None` when the model is not a
   device-scoped child model or any workload fetched the whole network.
2. When it is not `None`, the parameter hash is computed with the scope names
   removed (and duplicate per-shard entries collapsed).
3. A stored state is compatible when its hash equals the new hash or the legacy
   hash.
4. On the compatible path, a previous row missing from this run's rows is
   deleted only if its device was fetched this run (`_row_device_names`), or it
   names no device. Otherwise it is carried into the new state with a blank row
   hash (`CARRIED_ROW_HASH`), so it is staged again when its device returns: the
   device may have been pruned with its rows while out of scope.
5. The carried count is in the durable-state log line and in the
   `durable_workload_state` diagnostic (`out_of_scope_carried_count`).

## Validation

- `invoke test-isolated --test-label forward_netbox.tests.test_workload_state`
  (40 tests, run on .104: OK). The two behaviour tests fail on the old code
  (mutation check), the legacy-state test errors there.
- Live on .104 against the customer-shaped network: sync an older snapshot with
  a different tag scope, then the latest; both runs must report `local_delta`
  for the device-scoped models and delete the rows Forward stopped returning.
- Full suite before release.

Known limits:

- A model with a map that does not declare `forward_netbox_shard_keys` (an
  older pinned org query, the CIMC/APIC inventory maps) still resets on a scope
  change, as before.
- Carried rows stay claims in this sync's state. Where a device moves from one
  sync to another, they keep the other sync from deleting that device's stale
  rows until this sync sees the device again. This errs toward keeping.

## Rollback

Revert the commit. States written by the new code carry the new hash; the old
code reads them as a parameter change and takes `contract_reset` once (no
deletes that run), then continues as before.

## Decision Log

- Rejected: deriving deletes from NetBox's current rows on a reset. It would
  delete rows people added by hand to synced devices; the sync deletes only what
  it can prove it staged.
- Rejected: carrying out-of-scope rows with their hash intact. Review found it
  leaves a device pruned while out of scope with no interfaces when it returns.
- Rows already forgotten by earlier releases are not cleaned up by this change.
  A cleanup needs its own evidence of ownership and is a separate item.
