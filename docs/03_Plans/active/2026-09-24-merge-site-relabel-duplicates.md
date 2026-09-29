# GUI repair for existing site-relabel duplicate pairs

## Goal

The site-relabel apply-path fix (item 1, `apply_engine_bulk.py`'s
`_relabel_move_candidate`) stops a sync from creating a second device on a
site relabel going forward, but the customer's estate already has ~248 such
pairs (496 rows) from before the fix. Each pair strands an older device at a
stale site, still holding the device's primary IP, invisible to scope
reconciliation because a device with that name still exists. Give the
operator a single GUI action that repairs the existing pairs: keep the older
device, move it to the newer copy's site, delete the newer copy.

## Constraints

- Never guess. A pair is merged only when it can be PROVEN which device
  Forward means - the same "hold, don't guess" rule the apply-side fix
  itself follows. Ambiguity is held with a reason, not resolved by
  assumption.
- No live Forward call: the newer device's own site already IS Forward's
  current answer for that name (it was created there by a real ingestion),
  so detection is entirely DB-local and safe to compute inline.
- The pair list is recomputed inside the job, never trusted from the button
  click, matching every other prune action in this module.
- Exports (the bundle section) carry pks and reason tokens only.

## Touched Surfaces

- `forward_netbox/utilities/scope_reconciliation.py` - `site_relabel_pairs(sync)`
  (detection) and `merge_site_relabel_duplicates(sync)` (the repair), reusing
  `_delete_prunable_devices` for the delete half.
- `forward_netbox/jobs.py` - `MergeSiteRelabelDuplicatesJob` +
  `_merge_site_relabel_duplicates_work`.
- `forward_netbox/utilities/sync_facade.py` - `BUTTON_JOB_SPECS` entry.
- `forward_netbox/views.py` - `ForwardSyncMergeSiteRelabelDuplicatesView`,
  `_site_relabel_pairs_payload`.
- `forward_netbox/api/views.py` - `merge_site_relabel_duplicates` action.
- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`
  - new card.
- `forward_netbox/utilities/bundle_diagnostics.py` - `site_relabel_pairs` section.
- Tests: `test_site_relabel_pairs_merge.py` (new), `test_button_jobs.py`
  (parity entry).

## Approach

1. **Detection is DB-local.** Group devices by casefolded name with a raw
   count >= 2 (any group size, so 3+ devices correctly falls into the
   "ambiguous, hold" branch rather than being invisible to the scan). For a
   group of exactly 2 at different sites, the older (by `created`, tie-broken
   by pk) is the repair candidate only when: the newer device is bound to
   this sync's `ForwardDeviceIdentity` for that name (proof Forward's current
   answer is the newer copy), the older is not, and the newer carries no
   *operator* protecting reference. Forward's own provenance FKs
   (`ForwardDeviceIdentity`, `ForwardDeviceTagClaim`,
   `ForwardVirtualParentClaim`) report as protecting too - for a plain
   delete, they are - but are explicitly excluded from that check: they are
   exactly what proves the pair safe, and `_delete_prunable_devices` already
   releases them before deleting.
2. **The merge reuses the prune's delete machinery.** Each pair runs in its
   own `atomic()`: `_delete_prunable_devices(sync, [newer.pk])` (ownership
   release, `ProtectedError` handling, all shared with every other prune),
   then `older.site = newer.site` + `full_clean()` +
   `clear_cross_site_untagged_vlans`, then bind `ForwardDeviceIdentity` to
   the older pk. One pair's failure does not stop the rest.
3. **A fraction guard, not a hard limit.** More than half the estate's
   devices appearing in candidate pairs refuses outright
   (`SiteRelabelPairFractionGuardError`) - almost certainly a detection bug
   rather than a real backlog. A floor (`SAMPLE_LIMIT` devices touched)
   keeps this from firing on a small estate.
4. **Button, job, view, API, template and bundle section** all mirror the
   existing `prune_uncovered` action end to end - same permission
   (`dcim.delete_device`), same `enqueue_button_job` overlap guard, same
   job-data shape (counts + pk lists, never names).

## Validation

- `test_site_relabel_pairs_merge.py`: the happy path (bound newer device
  merges, older survives at the new site with its primary IP, identity
  rebinds); every negative-space case (no binding, binding to the older
  device, binding to a third device, binding to a different sync, 3+ devices
  sharing a name, a real protecting reference via a mocked
  `describe_protecting_references`, an out-of-list pk, two independent pairs
  both merging).
- `test_button_jobs.py`: the `WORK_FUNCTIONS` parity entry, so the runner
  is proven to actually call its work function.
- `test_scope_reconciliation_view.py`, `test_bundle_diagnostics_parity.py`:
  unmodified, run for regression (page still renders, bundle still builds).
- Full `invoke ci` before push.

## Rollback

Single change set, no migration. Revert removes the button, job and bundle
section; the apply-path fix (item 1) is unaffected and independent.

## Decision Log

- **Detection needs no live Forward call.** The newer device's site already
  answers "what does Forward currently say," because a real ingestion put it
  there - unlike `compute_scope_reconciliation`, which issues two live
  `fetch_all` NQE queries and is deliberately never computed on a page
  render. This is why the card's count can be computed inline.
- **Keep the older device (owner decision).** Preserves its pk, journal,
  change history, and any manually-attached objects the newer sync-created
  copy never accumulated.
- **Forward's own provenance FKs are excluded from the "manual objects"
  check, but not skipped entirely.** `describe_protecting_references`
  correctly reports them as protecting for a bare `Device.delete()` - that
  is real, current behavior (`release_on_operator_delete` protects on every
  engine path). They are excluded here specifically because the merge uses
  `_delete_prunable_devices`, which releases them first; a caller that did a
  plain delete would need to keep the check unfiltered.
- **Two `_n` bugs caught only by tests, not code review**: the initial
  detection query filtered `_n=2` (exact), which made a 3-device group
  invisible to the scan entirely rather than landing in the "ambiguous,
  hold" branch already written for it; and the fraction guard fired on every
  small-fixture test until it got the same floor pattern
  `SCOPE_SHRINK_REFUSAL_FLOOR` already uses.
