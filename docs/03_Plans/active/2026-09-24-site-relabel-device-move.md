# Stop creating a duplicate device when Forward relabels a site

## Goal

A customer's sync began piling up duplicate devices: 248 name-pairs (496
rows), 207 created in a single day. Root cause: when Forward renames a
location (`cdl0_dc00-roseland nj (njrsl)` -> `dc00-cdl-roseland nj`, confirmed
independently by the customer's own Forward contact - the old name no longer
exists as a location), the device row Forward reports for that name now
carries a different site. Both apply paths match an existing device on
`(name, site)` only, so the lookup misses under the new site and creates a
second device instead of moving the first. The old copy is stranded: it still
holds the device's primary IP (which the new copy then cannot acquire - the
customer's exact "carry no identity from this sync" skip messages), and
`scope_reconciliation`'s name-only identity (a separate, already-diagnosed
bug) never flags it as stale because a device with that name still exists
somewhere.

This is item 1 of the 2.9.9 release; item 2 (a GUI action to repair the ~243
pairs already stranded on the customer's estate) and item 3 (scope
reconciliation keyed by device, not name) follow in later PRs and must not
land before the customer has run the repair action, or the stale copies
would become silently prunable.

## Constraints

- Never guess. A device is moved only when this sync can PROVE which one
  Forward means: bound via `ForwardDeviceIdentity` (this sync's own name ->
  device record, read on `using("default")` the same way
  `sync_ipam._release_plan` already does), or - absent a binding - the SINGLE
  unbound same-name device at another site. Two or more candidates, or a
  candidate bound to a different sync, is held (recorded as an issue) and
  never guessed at, matching the "hold, don't guess" rule used elsewhere in
  this codebase (device-name ambiguity, ownership reconciliation).
- Both apply paths (bulk ORM and per-row) are fixed together, deliberately -
  this repo has shipped the same class of bug twice before by fixing only one
  branch.
- No new create when a move is possible; no move when it cannot be proven
  safe.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` (`bulk_orm_apply_device`) -
  when no same-site match exists, a batch-prefetched
  `_relabel_move_candidate(name, site_pk)` looks across
  `existing_by_folded_name` (already indexed across all sites for the
  case-collision fix) for an other-site device, using `ForwardDeviceIdentity`
  bindings fetched once per batch. A move falls through into the existing
  update path (which already handles the `site` field diff, `full_clean`, and
  `clear_cross_site_untagged_vlans` via the shared `update_objects` bulk
  path) instead of creating.
- `forward_netbox/utilities/sync_device.py` (`apply_dcim_device`) - a new
  `_relabel_move_device(runner, name, site)`, mirroring the existing
  `_case_variant_device` helper and reusing its `["id"]` coalesce injection,
  raises `ForwardSearchError` (the established ambiguous-match signal) rather
  than guessing.
- Tests: `forward_netbox/tests/test_device_site_relabel_identity.py` (new).

## Approach

1. Batch-fetch, per apply call, every device sharing a row's (folded) name
   across ALL sites - the bulk path already builds this index for the
   case-collision fix (2.9.8); reused here rather than a second query.
2. Batch-fetch this sync's `ForwardDeviceIdentity` bindings for the names in
   the batch, and which of the candidate devices are bound to ANY sync at
   all, in two queries total (not per-row).
3. When a row's name has no same-site match: resolve the fallback candidate.
   - Bound to this sync at that name -> that device, always (highest
     confidence: this sync itself asserted the binding).
   - No binding, and exactly one same-name device at another site, unbound to
     any sync -> that device.
   - Anything else (2+ same-name candidates; bound to a different sync; the
     identity binding points at a device not among the candidates) -> hold:
     record an ingestion issue naming the reason, create nothing.
4. A move sets `existing`/`matched_existing` to the candidate device and
   falls through into the SAME update logic every other device update already
   uses - `site` is already in `update_field_names`/`defaults`, so the pk,
   history, journal, cables and (critically) the primary IP all stay with the
   moved device. No new code path for "what happens after a move."

## Validation

- New tests cover: a bound identity moves even with another unbound candidate
  present; the single unbound candidate moves; two unbound candidates are
  held (nothing created, nothing moved); a candidate bound to a different
  sync is untouched; the row path raises `ForwardSearchError` rather than
  guessing; the device's primary IP survives the move with no release
  needed; an unrelated new device at a genuinely new site is still created
  normally.
- Regression run: `test_device_name_case_identity`,
  `test_cross_site_vlan_revalidation`, `test_bulk_merge`,
  `test_device_scope_tagging` (122 tests) - all pass unmodified, confirming
  no interaction with the case-collision fix or cross-site VLAN cleanup this
  change shares code with.
- Full `invoke ci` before push.

## Rollback

Single change set, no migration, no model change. Revert restores the
previous (name, site)-only matching; the customer's existing duplicates are
unaffected either way (item 2 is a separate, later change).

## Decision Log

- **Batch, not per-row, identity lookups.** A per-row query for 496+ affected
  devices in one sync would be a real cost at fleet scale; the fix reuses the
  bulk path's existing folded-name index and adds two batch queries total.
- **The bulk and per-row paths share no new helper function, by design.**
  They already differ in shape (batch dict lookups vs. `runner._get_unique_or_raise`
  reads) and mirror their EXISTING sibling helpers
  (`_case_variant_device`/inline bulk case-matching) rather than introducing
  a third shared abstraction neither path's structure naturally fits.
- **Ordering with item 2/3 is load-bearing, not cosmetic.** Landing item 3
  (scope reconciliation by device) before item 2 (the GUI repair action) would
  make the ~243 already-stranded duplicates silently prunable before an
  operator has chosen to merge them - sequenced explicitly in the 2.9.9 plan.
