# The unattended device sweep gets the same evidence bar as the operator path

## Goal

A sync may delete a device on its own only on the same evidence Prune orphans
requires from an operator: a quarantined absence, and a sane aggregate.

## Why

The same act - delete a device because it is absent from Forward's result -
existed at two different standards. The operator path (Prune orphans) requires
an absence QUARANTINE (>=3 consecutive confirmed absent runs AND >=72 hours,
operator-tunable per source, fail-closed for a device with no absence record), a
25% shrink refusal, a confirm dialog, and an absence classification. The
unattended ownership sweep in `apply_durable_workload_deltas` deleted on the
FIRST absence, gated only by ownership claims - and the scope claim is released
by the same first observation of absence, so the gate evaporated exactly one run
before the delete fired.

The quarantine model's own docstring states the principle the sweep violated: a
device disabled in Forward is indistinguishable from one that was deleted, so
deleting on the first absence destroys devices that are merely in a maintenance
window. The sequence - disabled in Forward -> claim released on run 1 -> deleted
on run 2 - is the long-standing "disabled in Forward deletes from NetBox"
mechanism. A deployment's ten devices survived it only because
`netbox_routing.bgppeer` rows held PROTECT references; a device with no
protecting child was removed silently, since a successful delete files no
ingestion issue.

## Constraints

- The sweep itself is intended behaviour and stays: a device this sync created
  and Forward genuinely dropped must eventually go without an operator visit.
  Option A (remove the sweep) was rejected for stranding those devices forever;
  option C (demote to tagging) for making the sweep redundant with the tag job.
- Fail closed everywhere: no absence record means zero confirmed absences.
- Zeroed thresholds remain an explicit operator "no quarantine" and are
  honoured, exactly as on the prune path - one knob, not two.
- The hold-backs must be observable, not silent.

## Touched Surfaces

- `forward_netbox/utilities/workload_state.py` - `_owned_device_rows` also
  returns the name->pk map; the sweep filters through
  `partition_quarantined_orphans` and the shrink ceiling; two new summary
  fields.
- `forward_netbox/tests/test_workload_state.py` - the first-absence test now
  asserts the new policy; two new tests pin the negative space.

## Approach

The sweep's absent identities are joined to their Device pks and filtered
through `partition_quarantined_orphans` - the same machinery, thresholds and
source parameters as the prune. On top, the prune's aggregate refusal: past
`SCOPE_SHRINK_REFUSAL_FLOOR` devices and `SCOPE_SHRINK_REFUSAL_RATIO` of
previously-managed, the sweep deletes NOTHING, because a query fault absents a
fleet just as convincingly as a decommission absents one device and only the
aggregate can tell them apart. Unattended, there is no operator to ask, so the
answer is to delete nothing while the streaks keep advancing.

Held rows are reported as `ownership_quarantine_held_rows` and
`ownership_shrink_held_rows` in the durable-state summary.

The absence streaks are maintained for every sync by the post-sync scope-tags
job and lag this fetch by one run - the conservative direction.

## Validation

- `test_owned_device_absent_past_quarantine_is_deleted` - the intended
  behaviour, now with evidence.
- `test_owned_device_first_absence_is_held_by_the_quarantine` - the policy
  change itself: one absence deletes nothing, fail-closed.
- `test_a_mass_absence_is_a_fault_and_deletes_nothing` - every device past
  quarantine individually, held in aggregate.
- `test_current_scope_claim_protects_owned_device_from_delete` - updated to
  clear the quarantine so the claim guard is what it exercises.
- Full Django suite green (recorded in the release authorization when cut).

## Rollback

Revert. The sweep returns to first-absence deletion. Reverting cannot delete
anything by itself - this change only ever removes rows from a delete set.

## Decision Log

- **Option B over A and C.** Keeps the intended behaviour at the operator
  path's evidence bar; A strands devices, C duplicates the tag job.
- **Reuse `partition_quarantined_orphans` verbatim** rather than a parallel
  implementation: same thresholds, same source parameters, same fail-closed
  semantics, one place to tune.
- **Hold, don't raise, on the shrink ceiling.** The prune raises because an
  operator is present to read the refusal. Unattended, an exception fails the
  run; holding the deletes lets the rest of the sync proceed and the streaks
  keep advancing, and the summary says what was held.
- **Guard order: quarantine, then claims, then references.** Cheapest and most
  conservative first. The claim test is pinned past the quarantine so it still
  exercises the claim guard.

## Open

- The `netbox_dlm.softwareversion` catalogue sweep still enumerates the entire
  table, including operator-created rows the sync never touched. Quarantine is
  device-shaped and does not apply; the refused-delete guard (#273) makes its
  failures honest. Scoping it to plugin-provenanced rows is its own question.
- `DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT = 10` cap and the mislabelled
  dependency-skip summary wording remain from the previous plan's Open list.
