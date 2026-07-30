# Protected-Delete Blocker

## Goal

Stop a delete the database will certainly refuse from failing a row, and so
permanently blocking baseline promotion.

This is the blocker behind the diff-enablement NO-GO for `dcim.device`,
`dcim.interface` and `ipam.ipaddress`. It was believed fixed by 2.6.5's
protected-delete ordering. It was not.

## Contract

- A delete whose PROTECT/RESTRICT reference survives the run is **skipped and
  reported**, never scheduled to fail.
- A delete whose referencing row is deleted or updated in the same run is left
  alone — ordering resolves both.
- Blocked deletes appear in `result_metadata` as schema identifiers and counts
  only, never referencing row contents.
- No delete that would have succeeded is skipped.

## Constraints

- Do not delete the protecting row to clear the path: it is operator-owned data
  the sync has no mandate over.
- Keep `netbox_branching` in the ingestion path.
- Persisted diagnostics stay schema-level.

## Touched Surfaces

- `forward_netbox/utilities/bulk_merge.py`
- `forward_netbox/tests/test_protecting_reference_deletes.py`
- This plan.

## Approach

**Root cause: an unwired function.** `protecting_reference_blocked_deletes` was
implemented and fully unit-tested with **zero production call sites**
(`grep` across the package finds only its definition and its tests). So the
delete was still scheduled, still raised `ProtectedError` at apply time, and
still failed the row — and per the merge-failure dead end a single failed row
permanently blocks baseline promotion. One operator-owned object could wedge a
sync's convergence bookkeeping indefinitely. Same shape as 2.6.6's density
learning, which also shipped with no call sites.

2.6.5's `_add_protected_child_delete_dependencies` did not cover this. It orders
child-before-parent when **both** are deleted, and its own docstring says a
surviving reference is deliberately left alone "so the parent delete still fails
strictly". Nothing acted on survivors.

**Fix.** `_skip_protecting_reference_blocked_deletes` runs before ordering,
marks blocked deletes `SKIP`, logs what holds each one, and returns the set for
`result_metadata`.

**Disposition: skip, not fail.** The plugin cannot resolve these. Of the three
PROTECT references into `dcim.Device` — `dcim.VirtualChassis.master`,
`dcim.VirtualDeviceContext.device`, `virtualization.VirtualMachine.device` —
only virtualchassis is a synced model; the other two are operator data.
Deleting them to clear the path would destroy records the operator owns. So the
delete intent stays exact and visible as an issue, the row stays in drift where
an operator can act, and the merge is not wedged by a condition it cannot fix.

**The predicate had to be tightened.** The first version treated "not deleted in
this run" as "surviving", which skipped a delete that actually succeeds: the
check reads destination state *before* ordering applies anything, so a referrer
whose UPDATE is about to release the FK still looks like it protects. That is
precisely the case `_add_destination_fk_release_dependencies` exists for, and
`test_dlm_protected_version_delete_follows_destination_fk_reassignment` caught
it. A reference now blocks only when the referencing row is **not changing at
all** — deleted (ordering handles it) or updated (release-ordering handles it)
both clear the block.

The asymmetry is deliberate: wrongly skipping loses a real delete *silently* and
diverges from Forward, while wrongly allowing one restores the pre-existing
`ProtectedError`, which is reported. When uncertain, allow.

## Validation

- `test_protecting_reference_deletes`, `test_bulk_merge`, `test_set_based_merge`,
  `test_ingestion_merge`, `test_accepted_merge_failures`: **160 tests, OK**,
  including the DLM release-reassignment case.
- `scripts/tests`: 247 OK.
- **Negative control:** with the call site replaced by `{}`,
  `test_the_merge_calls_it` fails. That test asserts the *call site*, not just
  the helper — a helper-only test would pass again if a refactor dropped the
  call, which is exactly how this shipped broken.

## Rollback

Revert the commit. The change is additive: one new function, one call, one
metadata key. Reverting restores the previous behaviour, in which these deletes
fail at apply time.

## Decision Log

- 2026-07-30: Skip rather than fail, for the same reason the MAC ambiguity case
  skips — this is a destination-side condition the sync did not cause and cannot
  fix, and failing it blocks baseline promotion for the whole ingestion.
- 2026-07-30: An updated referrer does not block. Conservative in the safe
  direction; see the asymmetry above.
- 2026-07-30: **Corrects an earlier claim in this tranche** that diff enablement
  was unblocked by 2.6.5. It was not. Whether this change is *sufficient* to
  enable Device/Interface/IPv4 is still a live-proof question — the NO-GO was
  "exact workload intent is insufficient when apply convergence fails", and this
  converts an apply failure into a reported non-convergence. That is necessary,
  not proven sufficient.

## Evidence

- `protecting_reference_blocked_deletes` call-site search before this change:
  definition plus `forward_netbox/tests/test_protecting_reference_deletes.py`
  only.
- The prior diff-delete-correctness proof recorded the physical gate as: one
  `dcim.device` `ProtectedError` and two protected-dependency skips each for
  `dcim.interface` and `ipam.ipaddress`. The note describing that incident as
  "two surviving BGP peers" does not survive checking — no BGP model holds a
  PROTECT reference into `dcim.Device`. The actual protecting relation should be
  re-derived from run evidence before planning the live proof.
