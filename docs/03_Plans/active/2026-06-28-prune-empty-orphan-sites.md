# Prune empty orphan sites

**Date:** 2026-06-28

## Goal

Extend the "Prune orphans" job to also remove NetBox sites that are no longer
present in the Forward location result AND have zero devices and zero racks.
Previously only devices were pruned; sites imported by Forward would accumulate
indefinitely after their devices were removed from scope.

## Constraints

- Only delete sites that are **truly empty** (zero devices, zero racks) at prune
  time. Sites with any remaining infrastructure are left alone regardless of
  Forward scope.
- No deletion if Forward returned no location data (guard: `forward_site_slugs`
  empty → skip site prune).
- No deletion if Forward returned 0 devices (reuses the existing
  `EmptyForwardScopeError` guard inherited from device prune).
- Site prune runs **after** device prune in the same job so sites that became
  empty due to device deletion are caught in the same pass.

## Touched Surfaces

- `forward_netbox/utilities/scope_reconciliation.py` — add `location` field to
  the reconciliation NQE query; collect `forward_site_slugs`; add preview counts
  (`netbox_empty_orphan_site_count`, `empty_orphan_site_sample`) to report; add
  `prune_orphan_sites` function.
- `forward_netbox/jobs.py` — wire `prune_orphan_sites` after `prune_orphan_devices`
  in `prune_forward_orphans`; persist `pruned_site_count` in job data.
- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`
  — add "Empty orphan sites" row to the preview table; update the confirm dialog
  to mention sites; enable Prune button when either devices or sites are queued.
- `forward_netbox/tests/test_scope_module_ui.py` — add
  `test_prune_also_removes_empty_orphan_sites` (device+site pruned together;
  site with rack preserved).

## Approach

The existing `compute_scope_reconciliation` device query is extended to also
select `device.locationName` (lowercased). After the row loop, slugify each
location name into `forward_site_slugs`. For the preview, query NetBox for sites
not in that slug set and currently empty (no devices, no racks). For the actual
prune, `prune_orphan_sites` re-queries current DB state (post-device-prune) so
sites that became empty in the same job pass are also removed.

## Validation

Unit: `test_prune_also_removes_empty_orphan_sites` — orphan device pruned, its
now-empty site deleted; in-scope site kept; site with rack kept (not "truly
empty"). Full suite (903). Syntax check. Sensitive gate.

## Rollback

Revert the four surfaces. Any deleted sites would need to be re-imported via a
fresh Forward sync.

## Decision Log

- Re-query empty sites at prune time (not from cached report pks) so the
  post-device-prune state is always used — avoids a two-pass job.
- Guard on `forward_site_slugs` empty rather than `tagged_names` alone: if
  Forward has devices but none have a `locationName`, we have no basis to prune
  any sites.
- Button enabled when either device OR site orphan count > 0.
