# Device cleanup honesty: a manual delete works, the badges match the buttons, and inventory items are measured

## Goal

A customer's first week on 2.9.5 surfaced four operator-visible gaps on the
Scope Reconciliation page and the drift report, all traced from the same
support thread:

1. A manual delete of an uncovered device from the device list (bulk or
   single) refuses with NetBox's raw `ProtectedError` naming the plugin's
   own `ForwardDeviceIdentity` / `ForwardDeviceTagClaim` rows - no cause, no
   remedy - even though those are exactly the records the plugin's own prune
   already knows how to release. After this change, a person deleting a
   device on `main` releases those rows for every sync that held them; every
   engine path (the sync, the merge, the prune, the fast baseline) keeps the
   PROTECT it relies on.
2. The orphan card's quarantine badge ("N in quarantine / M prune-eligible")
   counted every out-of-scope device, not only the ones the prune actually
   deletes (which is `absent` only - a device still in Forward under other
   tags is never touched). A customer's panel read 97 prune-eligible / 10 in
   quarantine, but the button only deletes the ones genuinely gone from
   Forward. After this change the badge and the button agree, and a new
   `out_of_scope_not_prunable` count names the rest.
3. The uncovered card's "Filter devices by tag" link opens
   `/dcim/devices/?tag=forward-uncovered`, a tag shared by every sync across
   every run, not this sync's current count - a customer read that global
   list's size against their sync's card, which said something much
   smaller, and bulk-deleted from the mismatched list. The card now says the
   tag list is shared and points at the sync-scoped "List all N" as the
   current count.
4. `dcim.inventoryitem`'s largest deployment-facing map read "Not measured"
   on every run whenever `dcim.module` was enabled and any batch contained a
   module-native row (the apply deletes those rather than upserting them,
   and the comparison had no slot for a delete). One customer's estate has
   58,606 such rows. The comparison now counts a module-native row as a
   delete instead of declining the whole model.

## Constraints

- Every engine write path (sync apply, merge, prune, fast baseline, branch
  staging) must keep exactly its current `PROTECT` behavior on
  `ForwardDeviceIdentity.device` / `ForwardDeviceTagClaim.device` - this
  change only widens what a **person** deleting on `main` can do.
- `protecting_relations()` (the merge's held-back-delete predictor) must
  keep seeing both models as protecting, or a merge delete it used to hold
  back would be attempted and fail.
- `ForwardVirtualParentClaim` is not touched: a parent with claimed virtual
  children is a real dependency, not bookkeeping about one device.
- No change to `prune_orphan_devices`' own cause gate (already correct,
  see `OrphanPruneGatesOnCauseTest`) - only the report's badge.
- No new per-run NQE execution anywhere in this change.
- No customer names, identifiers, or scan-matching hostnames in committed
  content, commit messages, or PR bodies.

## Touched Surfaces

- `forward_netbox/models.py` - `release_on_operator_delete`, a module-level
  `on_delete` callable used on both FKs.
- `forward_netbox/migrations/0055_operator_delete_releases_ownership.py` -
  `AlterField` on both FKs, no data change.
- `forward_netbox/utilities/bulk_merge.py` - `protecting_relations()` also
  matches a callable's `.protects` attribute.
- `forward_netbox/template_content.py`,
  `forward_netbox/templates/forward_netbox/inc/device_ownership_panel.html` -
  wording updated for the new delete behavior; the gated prune offer is
  unchanged.
- `forward_netbox/utilities/scope_reconciliation.py` - the orphan
  quarantine summary computed over absent pks only; a new
  `out_of_scope_not_prunable` count.
- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html` -
  the uncovered card explains the shared tag list.
- `forward_netbox/utilities/drift_comparison.py`,
  `forward_netbox/views.py` - module-native `dcim.inventoryitem` rows are
  counted as deletes (`_compare_adapter_rows`'s new `deletes` key), folded
  into the model result's `delete_count` by `_dependency_model_result_summary`.
- Tests: `test_operator_delete_releases_ownership.py` (new),
  `test_absence_quarantine.py`, `test_scope_module_ui.py`,
  `test_inventoryitem_drift_comparison.py`,
  `test_dependency_preview_summary.py`.

## Approach

**Item 1.** `release_on_operator_delete(collector, field, sub_objs, using)`
reads `netbox.context.current_request` and `netbox_branching.contextvars.active_branch`:
when the current context carries a real `django.http.HttpRequest` (not the
`NetBoxFakeRequest` every engine path attributes its writes to) and no
branch is active, it delegates to `models.CASCADE`; otherwise to
`models.PROTECT`, unchanged. The function is marked `.protects = True` so
`protecting_relations()` still reports it as a protecting relation for the
merge's own prediction, even though its runtime behavior now depends on
context. `ForwardVirtualParentClaim` is untouched.

**Item 2.** `compute_scope_reconciliation` resolves which out-of-scope
devices the current Forward census classifies as `absent` (the same
classification `prune_orphan_devices` already gates its own deletes on),
and computes `out_of_scope_quarantine` over only those pks - mirroring the
uncovered card's `owned_quarantine`, which already had this rule.
`out_of_scope_not_prunable` carries the rest of the count for the template.

**Item 3.** No model change: the uncovered card gains one sentence
explaining that the tag-filtered device list is shared across syncs and
runs, and that "List all N" is this sync's own current count.

**Item 4.** `_compare_adapter_rows` gains a `deletes` counter; the
`uncomparable_outcomes` branch increments it instead of declining the whole
model. `_compare_dcim_inventoryitem` is otherwise unchanged.
`_dependency_model_result_summary` folds `comparison["deletes"]` into the
`delete_count` it already reports, which `drift_report.py`'s
`compute_drift_report` already reads as the model's removal count - no
change needed there.

## Validation

- `invoke ci` under the release-gate environment.
- New/updated tests listed above cover: a real-request delete releasing
  every sync's rows; no-request, fake-request, and active-branch deletes
  still raising `ProtectedError`; a virtual-parent claim still protecting;
  `protecting_relations(Device)` still listing both models; the device
  list's real bulk-delete view succeeding; the orphan badge matching the
  button on a mixed absent/present-untagged set; the uncovered card
  rendering the new sentence with "List all" before the tag link; a
  module-native `dcim.inventoryitem` row being counted as a delete instead
  of declining its batch.

## Rollback

Every change here is additive or narrows an existing over-broad count; no
data migration beyond the two `AlterField`s (reversible - `AlterField` back
to `models.PROTECT` restores the pre-2.9.6 behavior exactly). The prune's
own delete path is untouched.

## Decision Log

- **PROTECT for every engine path, release only for a person on `main`.**
  The discriminator had to be *a real `HttpRequest`*, not "any request
  object": the merge and the fast baseline both attribute their writes to a
  `NetBoxFakeRequest`, and treating that as an operator would have cascaded
  exactly the delete the merge's held-back guard relies on holding.
- **A person deleting inside an active branch still gets PROTECT.** These
  ownership tables are not branch-aware; a cascade there could delete
  claims on `main` while the device delete itself stayed staged.
- **A manual delete releases every sync's rows for that device, not only
  one.** The operator is deleting the device; bookkeeping about it must not
  hold it hostage for a second sync either, the same rule
  `ForwardDeviceAbsence` already follows with `CASCADE`.
- **Item 2 mirrors the uncovered card's existing rule** rather than
  inventing a new one.
- **Item 4 counts module-native rows as deletes** rather than adding a
  fourth "declined" comparison state: the report already has a delete
  accounting separate from `creates + updates`.
