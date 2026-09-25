# Scope reconciliation is site-aware, not just name-aware

## Goal

`compute_scope_reconciliation`'s scope-membership test (`out_of_scope`,
`owned_untagged`) collapsed devices by bare NetBox device name. A device
whose site Forward relabeled looked identical to one that never moved: both
carry the same name, so both read as "in scope" as long as SOME device with
that name still existed anywhere. This is why 243 of the customer's 248
site-relabel duplicate pairs were invisible to every report, orphan prune
and uncovered cleanup - the stale copy at the old site was silently treated
as covered because a device named the same thing existed at the new site.
Items 1 and 2 stop new duplicates and repair existing ones; this item closes
the last gap - the report itself must be able to see a stale copy once one
exists (a merge that failed, a customer who hasn't run the merge yet, or any
future case the merge doesn't cover).

## Constraints

- Endpoint-derived names carry no site (that NQE row has no location field)
  and must keep the exact name-only test they had before - no regression
  there.
- `_absence_census`/`_absence_kinds`/`_absence_details` stay name-keyed:
  that machinery is inherently about a Forward NAME's collection status, not
  a NetBox device, and converting it would be a much larger, riskier change
  for no behavioral gain here.
- `reconcile_source_device_tag_claims` (the tag-maintenance call boundary)
  stays name-based - it is the plugin's own name<->pk resolution boundary
  (handles ambiguity, renames, stale-binding retirement) and correctly-scoped
  input sets are what fixes its callers, not a signature change.
- The site-aware fix necessarily makes a currently-stranded, unmerged
  site-relabel pair's older device correctly out-of-scope/absent - exactly
  what both prunes delete. That device must stay untouched until the
  operator runs `merge_site_relabel_duplicates` (item 2), not get silently
  deleted by an existing prune button the moment this ships.

## Touched Surfaces

- `forward_netbox/utilities/scope_reconciliation.py`:
  `compute_scope_reconciliation` (the `out_of_scope`/`out_of_scope_pks`
  computation, and the `netbox_names`/site map it's built from),
  `_unmanaged_device_summary` (the `owned_untagged` computation),
  `_require_survivable_scope_shrink`, `_require_survivable_uncovered_shrink`,
  `prune_orphan_devices`, `prune_uncovered_devices`, and a new
  `_exclude_site_relabel_pending_pks` helper.
- Tests: `test_scope_reconciliation_site_aware.py` (new).

## Approach

1. **Track each Forward row's own site**, not just the set of all sites seen.
   The existing per-row loop already slugifies `location` into
   `forward_site_slugs` (every slug seen, for the empty-orphan-site
   calculation); it now also records `row_site_slug_by_name[name] = slug` -
   one extra dict write, no new query.
2. **Read NetBox devices as `pk -> (name, site slug)`**, not `{names}`. One
   query (`select_related("site")`), reused by both the `out_of_scope` and
   `owned_untagged` computations below instead of two separate `Device.objects`
   scans.
3. **The membership test becomes**: a managed device (by pk) is in scope
   only when Forward tags its name AND (Forward reported no site for that
   name at all - the endpoint fallback - OR the device's own current site
   matches the site Forward reports for that name now). Applied at both
   `compute_scope_reconciliation`'s `out_of_scope_pks` loop (replacing the
   old `(previously_managed_names & netbox_names) - tagged_names` set
   arithmetic) and `_unmanaged_device_summary`'s `untagged` filter
   (replacing `name not in tagged_names`).
4. **Every downstream re-derivation-by-name is fixed to test the pk-set
   first.** `out_of_scope_absent_pks` (the prune-eligibility gate) now
   intersects against `out_of_scope_pks` before testing the absence-kind
   name, so it can never re-widen back to a live device that happens to
   share a name with a genuinely out-of-scope one. Both shrink guards now
   count pks (`_out_of_scope_pks`, the `absent_pks` list already computed at
   the call site) instead of `len()` of a name-set, which underdcounted by
   however many such name collisions existed.
5. **Prune excludes any pk in an unmerged, identity-provable site-relabel
   pair.** `_exclude_site_relabel_pending_pks` calls the already-DB-local
   `site_relabel_pairs(sync)` (item 2) and subtracts every `older_pk`/
   `newer_pk` from what either prune is about to delete. A same-name group
   with no identity proof (held, not a pair) is unaffected - it was already
   correctly excluded from `pairs`, so nothing here widens what a customer's
   existing `SITE_RELABEL_HOLD_*` reasons already refuse to guess at.

## Validation

- `test_scope_reconciliation_site_aware.py`: the customer's exact shape (a
  claimed stale-site device reads as out of scope, its live-site same-name
  sibling does not, symmetrically in both directions); `owned_untagged` is
  site-aware the same way; the prune-eligibility re-derivation never catches
  the live sibling (an end-to-end `prune_orphan_devices` run deletes only
  the stale pk); an unmerged, identity-provable pair is excluded from prune
  entirely; the endpoint name-only fallback is unchanged.
- Full existing regression run across every test touching this module (221
  tests: `test_absence_quarantine`, `test_absent_configuration_detail`,
  `test_device_scope_reconciliation_audit_command`, `test_jobs`,
  `test_scope_audit_full`, `test_scope_module_ui`,
  `test_scope_reconciliation_view`, `test_scope_shrink_guard`,
  `test_uncovered_absence_and_trend`, `test_uncovered_device_cleanup`) plus
  item 2's own suite - 240 tests total, all green, confirming the fix is
  additive (no prior name-only behavior broke) rather than a rewrite that
  merely didn't happen to regress anything tested.
- Full `invoke ci` before push.

## Rollback

Single change set, no migration. Revert restores the previous name-only
membership test; items 1 and 2 are unaffected and independent (item 2's
`site_relabel_pairs` detection was already pk-native and unaffected by this
change either way).

## Decision Log

- **A site-aware membership test, not a full pk-native rewrite of the whole
  report.** The fuller conversion this item's plan entry originally
  described (every internal set replaced with pk-keyed structures
  throughout) touches `workload_state.py`'s device sweep and the tag-claim
  reconciliation boundary for no additional correctness gain once traced
  through: `workload_state.py`'s `device_id_by_name` is keyed by
  `ForwardDeviceIdentity.source_device_key`, which is unique per sync by a
  DB constraint - not a raw `Device.name` collapse - so it was never
  actually vulnerable to this bug. And `reconcile_source_device_tag_claims`
  is itself the name<->pk resolution boundary; feeding it pks would only
  require converting them back to names at the call site. Scoping the fix to
  where the bug actually lives kept the blast radius proportionate to a
  change already touching two prune paths and two shrink guards.
- **The endpoint fallback is a fallback, not a gap.** Endpoint-derived scope
  has no site to check by design (Forward's endpoint NQE row carries no
  location), so the site-aware test degrades to the exact name-only
  behavior it always had whenever `row_site_slug_by_name` has no entry for
  a name - never a stricter or looser test than before for that case.
