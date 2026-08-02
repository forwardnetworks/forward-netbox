# Primary-IP reassignment convergence

## Goal

Allow an in-scope, Forward-owned IP address to move to its new interface when
its prior device still designates it as primary, so subsequent syncs converge.

## Contract

- A staged IP reassignment releases a matching `primary_ip4` or `primary_ip6`
  pointer on its prior device only when that device is owned by this exact sync
  and is in the current tag scope when one is configured.
- The branch merges that release before the IP reassignment.
- Out-of-scope, unowned, and otherwise unprovable primary pointers are left
  unchanged; NetBox's existing destination-rule skip remains intact.

## Constraints

- Preserve the native `netbox_branching` staging and merge path.
- Do not persist device names, addresses, or other customer identifiers in
  diagnostics or runtime state.
- Do not add Forward/NQE requests; scope evidence is already resolved for the
  run and is passed only in memory.
- Do not weaken validation or fail-closed ownership checks.

## Touched Surfaces

- `forward_netbox/utilities/branch_lifecycle.py`
- `forward_netbox/utilities/sync_ipam.py`
- `forward_netbox/utilities/apply_engine_bulk.py`
- `forward_netbox/utilities/bulk_merge.py`
- IPAM/merge integration tests and this plan.

## Approach

Pass the resolved device scope to each short-lived runner without adding it to
persisted model results. When an existing IP's assigned interface changes in a
branch, find any device primary pointers to that IP. Release only pointers on
devices backed by a `ForwardDeviceIdentity` for the same sync and, for a
tag-scoped run, in the current resolved scope. Stage the device release and IP
move in the same branch write transaction and emit both ObjectChanges.

The LAG/cable precedent was checked first. That fix could carry the cable
release in the same Interface ObjectChange because `cable` is a field on that
row. Here the conflicting `primary_ip*` value is a field on a different Device
row, so one IPAddress payload cannot encode the release. Add only the required
Device-release -> IP-update edge, protected by the existing acyclic edge guard.

## Validation

- Targeted IPAM/primary-IP integration and merge-rule tests, sequentially.
- `python scripts/check_harness.py`.
- `python -m invoke harness-test`.
- Inspect each command output for literal `Ran N tests`, `OK`, or `FAILED`.

## Rollback

Revert the change. Existing destination-rule handling will again record and
skip blocked reassignments; no migration or persisted cleanup is required.

## Decision Log

- Use `ForwardDeviceIdentity` plus current tag-scope membership as the minimum
  proof that this sync may clear the prior device's pointer.
- Do not clear an unowned or out-of-scope pointer and do not infer ownership
  from the IP row alone.
- Reuse the cycle-safe dependency helper because a Device release and an
  IPAddress reassignment are necessarily separate ObjectChanges.
- The original integration fixture assigned the source device's primary IP
  after provisioning its branch. Branch snapshots quite correctly retained the
  earlier null value, so its two failures neither proved an out-of-scope
  release nor exercised merge ordering. Seed that pointer before branch
  provisioning so the tests cover the stated contract.
- The destination Device and Interface had the same defect. Provision the
  branch only after all rows participating in the reassignment exist, or the
  adapter legitimately skips an unresolved destination before it ever reaches
  the primary-release helper.
- The remaining in-scope failure was real: the active branch routed
  `ForwardDeviceIdentity` through its provision-time copy, which omitted an
  identity written to the main control plane afterward. The ownership proof now
  explicitly reads main; the holder query and both staged mutations remain on
  the active branch.
