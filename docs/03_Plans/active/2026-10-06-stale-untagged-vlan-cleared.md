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

## Also in this change: the support bundle answers this cycle's questions

A customer's report could not be matched to its bundle in five ways, each now
closed with counts and versions only (no names):

- `environment.dependency_versions`: `scrapli`, `scrapli-netconf`, `forward-sdk`,
  `httpx`, `psycopg`, `django`, `pyzipper`, `cryptography`. A new `scrapli` release
  stopped NetBox starting and the bundle could not say which version was installed.
- `scope_reconciliation.age_hours` and `older_than_last_sync`: the stored report
  was three days older than the sync it sat beside.
- `primary_ip.without_primary_ip_split` (uncovered, with a `Mgmt_` tag, with an
  interface IP, with none) and `netbox_devices_without_primary_ip`, so the figure
  on the device list can be reconciled with the sync's own.
- `latest_ingestion_issues.summary`: counts by model, exception and matched rule
  over every issue, not only the capped rows returned.
- `diagnostics.routing_name_collisions`: OSPF instances sharing a (device, name),
  the pair netbox-routing 0.5.0 refuses.

Touched: `forward_netbox/views.py`,
`forward_netbox/utilities/bundle_diagnostics.py`,
`forward_netbox/tests/test_bundle_triage_diagnostics.py`.

## Also: devices sitting at a different site than Forward puts them

A customer saw 320 devices at one fallback site while Forward places only a small
share of its scoped devices there. The stored scope report could not say why: it
recorded Forward's site only for devices whose names are duplicated, to keep the
refresh cheap. The refresh now always reads the sync's device map (one more
Forward call per Refresh Scope Reconciliation) and records `site_mismatch`: how
many devices were compared, how many sit at a different NetBox site than the map
puts them, which site pairs account for them, and how many names the map places
nowhere or at several sites. Primary keys and counts only; the per-device site map
is still recorded for duplicated names only. Touched:
`forward_netbox/utilities/scope_reconciliation.py` (`_site_mismatch_summary`),
`forward_netbox/tests/test_scope_reconciliation_site_aware.py` (the test that pinned
"no Forward call without duplicates" now pins the opposite, on purpose).
