# Restrict baseline removals to models the plugin solely authors

## Goal

Stop a full sync deleting devices. This is a regression I introduced in 2.7.11
and shipped in 2.7.12.

## Constraints

- Device removal stays operator-gated. Scope Reconciliation -> Prune orphans
  exists, with a shrink guard and a "confirm in Forward before deleting
  anything" warning, because absence from a query result is not evidence a
  device is gone.
- The reconciliation must keep working for the rows it was built for, or the
  DLM leftovers return.
- Fail closed: a model not explicitly listed is not removed.

## Touched Surfaces

- `forward_netbox/utilities/full_removal_reconciliation.py` -
  `BASELINE_REMOVAL_MODELS` and the guard in `compute_full_removals`
- `forward_netbox/tests/test_full_removal_reconciliation.py`

## Approach

`compute_full_removals` applied to every model, with no exclusions. That was not
a considered decision - it was the absence of one. The consequence is that a
full sync stages deletion for any row in the promoted baseline absent from the
current result, INCLUDING `dcim.device`, subject only to the 30% limit.

A deployment on 2.7.12 showed it in a single run: 54 deletions applied, one
`dcim.device` `ProtectedError`, five `netbox_dlm.softwareversion`
protected-delete skips, and their untagged device count falling from 407 to 389.
Devices were being deleted unattended, on every full run, having never passed
the gate the product puts in front of exactly that.

The fix is an allowlist rather than a denylist, so a model added later is
excluded until someone decides otherwise. It contains only rows the plugin
solely authors and that are derived from a device which still exists:
interfaces, MACs, inventory items, modules, cables, IP addresses, FHRP groups,
the DLM per-device rows, and the routing rows.

Deliberately excluded, each for its own reason:

- `dcim.device`, `dcim.site` - operator-gated through the prune flow
- `dcim.devicetype`, `platform`, `manufacturer`, `devicerole` - shared
  catalogues where an empty row is not garbage and may be a Device Type Library
  import
- `ipam.prefix`, `vlan`, `vrf` - global IPAM, never pruned by device scope
- `netbox_dlm.softwareversion` - a catalogue with children, which is precisely
  what the five protected-delete skips were

## Validation

Five tests pinning what must NOT be removed - device, site, a catalogue with
children, global IPAM - and one pinning that a derived row still is, so the fix
cannot quietly disable the feature it is narrowing.

## Rollback

Revert. Full runs resume deleting devices, which is the defect.

## Decision Log

- **Allowlist, not denylist.** The failure here was an implicit "everything",
  and a denylist reproduces that for every model added later.
- **`netbox_dlm.softwareversion` excluded even though it is DLM.** It is a
  catalogue with children; the protected-delete skips prove the deletes were
  never going to succeed, and attempting them is noise on every run.
- **Do not simply lower the threshold.** No percentage makes deleting a device
  without operator review correct.

## Open

- Devices already removed at that deployment are not recoverable from here. The
  recovery run recreated them as new rows; anything referencing them by id did
  not come back.
