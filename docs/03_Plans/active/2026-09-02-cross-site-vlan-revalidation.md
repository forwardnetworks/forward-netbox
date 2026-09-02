# Revalidate a device's interfaces when the plugin writes the device

## Goal

Close the mechanism behind the cross-site untagged-VLAN refusals at the point
the state is made, instead of finding it afterwards with the audit command.

## Why

A customer's post-2.7.2 interface failures were NetBox refusing interfaces
whose untagged VLAN belonged to a different site than their device - state the
plugin itself had left behind. Neither `bulk_update` nor `save()` runs
`Interface.clean()`, so a device written with a new site keeps its old site's
VLANs, and every later interface sync is refused on them. 2.7.x added
`forward_interface_vlan_audit` to find the rows; both its plan and the
interface-validation plan recorded "detecting it at the point it is created
is not addressed here."

## Constraints

- Clear only when this sync manages `dcim.interface`. Otherwise the
  interfaces are someone else's and the state is reported, not changed.
- Only devices the run actually WRITES are revalidated. An unchanged device's
  interfaces are left for the audit; touching them would be a side effect
  nothing asked for.
- Job-log and issue text carries device primary keys, never names.

## Touched Surfaces

- `forward_netbox/utilities/interface_vlan_audit.py` -
  `cross_site_untagged_vlan_interfaces`, `clear_cross_site_untagged_vlans`,
  `owned_only` on the audit.
- `forward_netbox/utilities/apply_engine_bulk.py` - one call after the device
  `bulk_update`, inside the same transaction.
- `forward_netbox/utilities/sync_device.py` - one call after an existing
  device is upserted.
- `forward_interface_vlan_audit --owned-only`.

## Approach

The code no longer matches the August diagnosis in one respect, and the plan
records it: since 2.7.11 the bulk device path matches an existing device on
`(name, site)`, so it cannot move a device between sites at all - a device
that changes site in Forward is created anew at the new site. The row path
still can, under an operator-configured name-only coalesce, and an operator
can move a device by hand. So the revalidation is keyed on "this run wrote
the device", however it came to match, rather than on a detected move. One
query per batch on the bulk path; one per device on the row path.

## Validation

`test_cross_site_vlan_revalidation.py`: only the other site's VLAN is cleared
(same-site and global survive); a sync not managing interfaces reports and
leaves them; a clean device costs no warning; keys not names; the bulk path
clears after a write and leaves an unchanged device alone; the row path clears
and respects the interface gate; `--owned-only` narrows the audit. Adjacent:
`test_interface_vlan_audit`, `test_apply_engine`, `test_device_scope_tagging`.
Full Django suite.

## Rollback

Revert. The audit still finds the rows on request.

## Decision Log

- **Clear, not refuse.** Refusing the device write would leave the device
  stale to protect interfaces NetBox will refuse anyway. Clearing restores a
  writable state, and the next interface apply writes the VLAN Forward
  actually reports for the current site - which is authoritative for a
  device this sync manages.
- **Gated on managing `dcim.interface`**, because on a deployment where the
  plugin syncs devices but not interfaces the VLANs are an operator's data.

## Open

- Nothing.
