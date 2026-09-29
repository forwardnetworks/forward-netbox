# Scope by name, relabel pairs by the device map's site

## Goal

A customer upgraded to 2.9.9 and the scope report called all 3,484 in-scope
devices out of scope, while the site-relabel repair built for their 221
duplicate pairs held every one of them and merged nothing. The cause is the
same in both cases: 2.9.9 decided which site a device belongs at from a slug
it derived itself, instead of from the sync's own device map. Put scope back
on the name. Answer "which copy of a duplicated name is current" from the
device map, so the repair can act on the pairs this customer actually has.

## Constraints

- Nothing that tags, streaks or prunes may depend on a site answer that could
  be wrong. Scope membership goes back to the 2.9.8 rule (the name is in
  Forward's tag-scope result), still keyed by device pk as 2.9.9 made it.
- The site answer is the device map's own: its `site`/`site_slug` columns for
  this sync, from whatever variant or pinned query the sync runs, resolved to
  a NetBox `Site` the way the apply resolves one (slug, then name). It is
  never re-derived from `locationName`.
- The repair always keeps the older device, per the owner's earlier decision,
  and never deletes a copy that carries operator or other-plugin objects.
- Persisted and exported data stays pk-only.
- The extra Forward work is read-only and runs only when NetBox holds
  duplicated device names. It is `ForwardQueryFetcher.fetch_workloads`, the
  same path the apply-identity audit uses, pinned to the reconciliation's own
  snapshot, so a clean estate (or this customer's, once merged) pays nothing
  for it. A failure yields no site answer, never a wrong one.

## Touched Surfaces

- `forward_netbox/utilities/scope_reconciliation.py`:
  - `compute_scope_reconciliation` is name-only again.
  - New `_forward_device_sites` and `_duplicated_device_forward_sites`.
  - New report keys: `forward_site_id_by_device_pk`,
    `forward_site_ambiguous_device_ids`, `forward_site_source`, and the
    internal `_forward_site_pks`.
  - Empty-orphan-site detection and pruning prefer the device map's sites.
  - `site_relabel_pairs` is rewritten, with new hold reasons and remedies.
    `site_relabel_held_by_reason` is new.
  - `merge_site_relabel_duplicates` targets Forward's site.
  - `_exclude_site_relabel_pending_pks` also excludes held groups.
- `forward_netbox/utilities/bundle_diagnostics.py`: `_site_relabel_pairs`
  adds the held-reason breakdown and the report timestamp.
- Tests: `test_scope_reconciliation_site_aware.py` and
  `test_site_relabel_pairs_merge.py`, both rewritten.

## Approach

1. **Scope.** A previously managed device is out of scope when its name is not
   in Forward's tag-scope result. Same for the owned-untagged split.
2. **Forward's site.**
   - Last Forward call of the reconciliation: fetch the sync's `dcim.device`
     workloads with `validate_rows=False`, on the reconciliation's snapshot.
     Resolve each row's site by slug, then name.
   - Group by casefolded name. For every NetBox device whose casefolded name
     is duplicated, record the single site the map places that name at.
   - A name the map places at two sites is recorded as ambiguous, not guessed.
   - The source's availability, row count and error class are reported.
3. **Pairs.** Proved from the latest stored report. A pair qualifies when:
   - exactly two devices share the name at different sites;
   - the report is newer than both devices;
   - the map places the name at exactly one of the two sites;
   - this sync's identity for the name (casefolded) binds older, newer or
     neither;
   - the newer copy has no non-Forward protecting references.

   Each pair carries the target site, the action (`move_older` or
   `delete_newer`) and which copy the identity bound.
4. **Merge.**
   - Delete the newer copy.
   - Move the older copy only when it is not already at Forward's site.
   - Bind the identity to the older copy under Forward's own spelling of the
     key.
   - Re-verify that neither device moved since detection.
5. **Prune.** Both prunes leave every member of a mergeable or held group
   alone.

## Validation

- `test_scope_reconciliation_site_aware.py`:
  - The customer's slug shape (`atl_colo.1` → NetBox slug `atl-colo-1`) keeps every
    device in scope.
  - Neither copy of a pair is out of scope.
  - Slug-then-name resolution; case-insensitive names; only duplicated names
    recorded; two-site names ambiguous.
  - A failed fetch leaves scope untouched and reports the error.
  - Empty orphan sites come from the map.
  - An unmerged pair survives the orphan prune.
- `test_site_relabel_pairs_merge.py`:
  - The pair qualifies whether identity binds older (the customer's 218),
    neither (their 3) or newer.
  - When Forward's site is the older copy's, the merge only deletes.
  - Holds: no report, stale report, unknown site, ambiguous, a third site,
    identity on a third device, 3+ devices (all members listed), protecting
    references. Remedies are given.
  - Merge: primary IP kept, identity moved to the older copy under Forward's
    spelling; held and unrelated devices untouched.
- Regression: every scope-reconciliation, uncovered, absence, audit-command
  and bundle test module, then the full `invoke ci`.

## Rollback

No migration and no model change. Reverting restores 2.9.9's site test, and
with it the false out-of-scope result, so a revert must come with a note to
operators not to act on scope counts.

## Decision Log

- **The site answer stays out of scope membership.** The approved plan put a
  corrected site test back into scope membership behind a plausibility
  guard. Dropped: tags, absence streaks and both prunes would still hang off
  a site answer whenever the device-map fetch succeeded, and "stale copy of a
  duplicated name" is already the repair's job. Scope by name, pairs by site.
  No extra guard is needed on paths that no longer read sites.
- **Held pairs are excluded from the prunes too.** 2.9.9 excluded only
  mergeable pairs, so a pair held for want of proof was still prunable. That
  inverts the point of holding it.
- **The device map is fetched only when duplicates exist.** Its answer is only
  used to tell the copies of a duplicated name apart; fetching it after every
  sync on an estate with none would cost several Forward calls for nothing.
  Empty-orphan-site detection then keeps its pre-2.9.9 slug behaviour.
- **The report must be newer than both devices.** A copy created after the
  last reconciliation has no device-map answer yet. That is a refresh, not a
  guess.
