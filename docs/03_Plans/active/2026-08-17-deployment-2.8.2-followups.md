# Three follow-ups from a 2.8.2 deployment sync

## Goal

Stop a protected delete on the device path failing the row, give the
tag-scope prune the removal allowlist the other two producers have, and make
the drift report say how much it measured.

## Why

A deployment's first 2.8.2 sync confirmed both of the previous release's fixes
in the field - every skip named its NetBox row, and the sites, device types and
VRFs the diff path used to delete were gone - and surfaced three more.

**The device delete failed instead of skipping.** `dcim.device row processing
failed (ProtectedError).` `dcim.device` is deleted through the branch collector
rather than through `delete_by_coalesce`, so its `ProtectedError` never reached
the conversion every other delete path gets and landed in the generic handler.
The word "failed" is the problem: a single failed row blocks baseline promotion
permanently, and the drift report then reads "Not measured" for the whole
deployment. Declining to delete a device is minor. Wedging the convergence
bookkeeping over it is not, and the row named neither what held the device nor
which device it was.

**The prune was a third ungated delete producer.** Six
`netbox_dlm.softwareversion` protected-delete skips survived both allowlists,
because rows dropped by device-tag scope become deletes whenever
`device_tag_prune_out_of_scope` is on, with no model policy consulted.

**The drift report used one string for three situations.** "Not measured" meant
a payload older than the measurement feature, or a preview that compared
nothing, or ordinary partial coverage - and only the first is fixed by
re-running the preview. Two rounds of support guessing followed, over data the
report already computed in 2.8.1 and did not display.

## Constraints

- Prune orphans must keep removing devices and sites. That is the feature, and
  an operator enables it deliberately behind a shrink guard and a warning.
- A skip must stay a skip. Converting the ProtectedError must not make the row
  invisible; it is still recorded, still counted, still shown.
- The drift wording must not claim measurement it does not have. The point is
  to distinguish the cases, not to make the report look more complete.

## Touched Surfaces

- `forward_netbox/utilities/sync_primitives.py` - `protected_delete_skip()`
- `forward_netbox/utilities/sync_device.py` - the branch delete path
- `forward_netbox/utilities/full_removal_reconciliation.py` -
  `PRUNE_REMOVAL_MODELS`, `prune_removals_allowed`
- `forward_netbox/utilities/query_fetch_execution.py` - the prune delete rows
- `forward_netbox/utilities/drift_report.py` and the drift report template

## Approach

The ProtectedError conversion moves out of `delete_by_coalesce` into a shared
`protected_delete_skip()` that both paths call, so the two cannot drift apart
again. This is the sibling-branch rule: a guard on one path and not the other
is the same defect as no guard, for whichever path lacks it.

The prune allowlist is the diff allowlist plus exactly `dcim.device` and
`dcim.site`, written as an equality in the test so a fourth model cannot be
added quietly. Three producers now read one policy file.

The drift report shows `measured / total`, names the uncompared models, and
when nothing was measured says whether the payload predates measurement -
detected by the absence of `comparison_coverage`, which every preview has
written since 2.8.1, so it dates the payload rather than describing it.

## Validation

- `forward_netbox/tests/test_protected_delete_is_a_skip.py`
- `forward_netbox/tests/test_drift_coverage_is_explained.py`
- `forward_netbox/tests/test_diff_removal_allowlist.py`, extended to the prune
  producer and to a parity check across all three
- Full suite: 2203 tests, OK with 4 skipped.

## Rollback

Revert. The device delete fails again, the prune deletes catalogues again, and
the drift report goes back to one word for three situations.

## Decision Log

- **Shared helper rather than a second copy of the conversion.** The defect was
  two delete paths with one guard between them; adding a second guard would
  reproduce it the next time a third path appears.
- **The prune keeps device and site.** Refusing them would break Prune orphans,
  which is the one delete an operator explicitly asked for.
- **Name the defect case in the UI.** When a preview compared nothing, telling
  the operator to re-run it wastes their time - that is the one situation
  re-running does not fix, so the report says so and asks for the payload.

## Open

- Why a deployment's fresh preview measured nothing is NOT diagnosed. The
  report change makes it self-identifying, and `comparison_coverage` from the
  preview JSON distinguishes a stale payload from a real comparison failure.
  Do not guess further without it.
- Three names for one feature: the button says "Preview Dependencies", the
  schedule table says "Dependency preview", the page header says "Dependency
  Dry Run". Not changed here; it is cosmetic and this diff is already three
  concerns.
