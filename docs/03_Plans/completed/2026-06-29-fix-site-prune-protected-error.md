# Fix site prune ProtectedError (empty-site prune never ran)

**Date:** 2026-06-29

## Goal
Make "Prune orphans" reliably delete empty orphan sites. On a real field sync it
errored in ~2s with an empty Error field and `data: null`; the Scope
Reconciliation preview correctly showed 152 empty orphan sites, but none were
deleted.

Root cause: `prune_orphan_sites` (shipped 2.1.3) treated a site as "empty" if it
had zero devices AND zero racks, then deleted all candidates in one
`transaction.atomic()` batch. But a NetBox Site is `PROTECT`-ed by more than
devices/racks — `dcim.PowerPanel`, `ipam.VLAN`, and `virtualization.VirtualMachine`
also PROTECT. A candidate still holding a VLAN/VM/power panel raised
`ProtectedError`, and the single atomic block rolled the whole prune back → job
errored, zero sites deleted, `data: null`. Separately `ipam.Prefix`,
`dcim.Location`, `virtualization.Cluster`, `wireless.WirelessLAN`, and
circuit/cable terminations are `CASCADE` — deleting such a site would have
silently destroyed those children (latent data loss).

## Constraints
- Only delete a site that is **truly empty** — nothing references it via any
  reverse relation. A site with any remaining object is kept.
- Preview and prune must use the same emptiness rule so the count matches what is
  deleted.
- No deletion when the Forward scope returned 0 devices or no location data
  (existing guards retained).

## Touched Surfaces
- `forward_netbox/utilities/scope_reconciliation.py` — `_occupied_site_ids()`;
  preview (`compute_scope_reconciliation`) + `prune_orphan_sites` use it; per-site
  delete with a `ProtectedError` guard.
- `forward_netbox/jobs.py` — `prune_forward_orphans` records the exception into
  `job.data` (`{"error", "error_type"}`) before terminating, so a failure is
  visible in the UI instead of an empty Error + null data.
- `forward_netbox/tests/test_scope_module_ui.py` —
  `test_prune_keeps_sites_with_non_device_objects`.

## Approach
`_occupied_site_ids()` unions the site foreign key of every reverse relation
(skipping many-to-many) so a site counts as occupied if anything points to it.
The prune lists candidates not in `forward_site_slugs` and not occupied, then
deletes them one at a time, catching `ProtectedError` (reported as `skipped`) so
an uncovered relation can't abort the run.

## Validation
`test_scope_module_ui` (11 tests, incl. the new regression) green on NetBox
4.6.3. The new test crashes with `ProtectedError` under the old batch code and
passes with the fix. Lint/harness/sensitive. Prefix preservation asserted (no
cascade delete).

## Rollback
Revert the three surfaces; `pip install forward-netbox==2.1.4`.

## Decision Log
- "Truly empty" (no references at all) over "no devices/racks": matches the
  stated intent ("only delete if truly empty"), and avoids both the PROTECT crash
  and the CASCADE data loss in one rule.
- Per-site delete + `ProtectedError` catch as defense in depth: the occupancy
  union should already exclude protected sites, but a relation added in a future
  NetBox version (or a GenericForeignKey not in `related_objects`) must not
  re-break the whole prune.

## Bundled changes
- Fix: "Prune orphans" now reliably removes empty orphan sites. A site is pruned
  only when nothing references it (no devices, racks, VLANs, prefixes, VMs, power
  panels, locations, …), so the prune no longer aborts with a NetBox
  ProtectedError on sites that still hold a VLAN/VM/power panel, and never
  cascade-deletes a child object. Prune-job failures are now surfaced in the
  job's Data panel. Drop-in from `2.1.4`.
