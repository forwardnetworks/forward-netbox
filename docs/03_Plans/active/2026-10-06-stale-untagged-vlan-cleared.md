# Fix: interfaces holding a VLAN from a previous site are refused on every write

## Goal

Stop a sync from recording over a hundred interface rejections on a deployment
where devices have moved between sites. An interface that still carries an
untagged VLAN from its device's old site is refused by NetBox
(`untagged-vlan-outside-device-site`) on every write, including a write that only
changes the MTU, because `full_clean` validates the whole object. Those rows were
recorded as skipped, so the interface never converged and the same rows reappeared
on every run.

## Constraints

- Forward is the source of truth for the VLAN. A VLAN is cleared only when the
  row does not supply a valid one for the device's site (no mode, VLAN ID 1, or a
  VLAN not imported for that site). A row that supplies a VLAN for the device's
  site still wins.
- A VLAN that belongs to the device's site, or to no site, is never touched.
- Only interfaces this sync is already writing are changed; an interface whose
  row is unchanged and which is not otherwise written stays as it is.
- No change to the existing clear on device writes, to the audit command, or to
  the disposition of rejections this does not prevent.

## Touched Surfaces

- `forward_netbox/utilities/sync_interface.py`
  (`stale_cross_site_untagged_vlan`, `apply_dcim_interface`)
- `forward_netbox/utilities/apply_engine_bulk.py` (`bulk_orm_apply_interface`)
- `forward_netbox/utilities/diagnostics.py` (one validation-rule slug,
  `name-not-unique-per-device`, for netbox-routing 0.5.0's OSPF instance rule)
- `forward_netbox/tests/test_stale_untagged_vlan_cleared.py` (new),
  `forward_netbox/tests/test_merge_rule_rejection.py`

## Approach

The plugin clears cross-site VLANs when it writes a device, but only for the
devices it wrote in that run. State left by an earlier version, a manual move or a
site-relabel merge is never revisited. The interface apply now checks, for an
existing interface whose row names no usable VLAN, whether its VLAN sits at
another site, and if so writes `untagged_vlan = None` with the rest of the row.
The lookup is one query per distinct VLAN per run, cached on the runner.

## Validation

- New tests cover the helper (other-site, same-site, global, none), both apply
  paths, a row that supplies a valid VLAN, and a valid VLAN left alone.
- Full `invoke ci` pre-push gate.

## Rollback

Revert this branch. No migration and no persisted state. Interfaces already
cleared stay cleared; the next sync writes the VLAN Forward reports for the
device's site.

## Decision Log

- **Clear in the interface apply, not only on device writes.** The rejected rows
  are pre-existing state from before this run, which the device-write clear cannot
  see.
- **Clear, don't skip.** Skipping left the interface unconverged on every run;
  Forward's own answer for that interface at that site is "no VLAN".
- **Named the OSPF unique-name rule** so a recurrence reads as a rule in a support
  bundle rather than as an unrecognised validation message.
