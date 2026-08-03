# Reconciliation Cascades With Its Ingestion

## Goal

Make an ingestion held only by ownership reconciliation rows deletable, so the
refusal message stops instructing an action the product does not offer.

## Contract

- An ingestion whose only remaining references are `ForwardOwnershipReconciliation`
  rows deletes, and those rows go with it.
- The ingestion whose reconciliation currently proves ownership complete for its
  sync is refused, as an expected warning, naming a step the operator can take.
- The baseline ingestion is still refused, with the baseline message.
- Device identities, device tag claims and virtual parent claims still protect.

## Constraints

- Schema change on live customer data; the migration must be reversible.
- Baseline protection must not be weakened.
- Ownership state must not be silently lost. A wrong refusal is a support
  question; a wrong delete is unrecoverable.

## Touched Surfaces

- `forward_netbox/models.py` - `ForwardOwnershipReconciliation.ingestion`
  overrides the mixin field to CASCADE
- `forward_netbox/migrations/0047_ownership_reconciliation_cascade.py` - new,
  `fake_on_branch = True` like every ownership control-plane migration
- `forward_netbox/views.py` - new `_ingestion_holds_current_ownership`, checked
  first in `_ingestion_delete_refusal_detail`
- `forward_netbox/utilities/bulk_merge.py` - corrected docstring
- `forward_netbox/tests/test_ingestion_delete.py`,
  `forward_netbox/tests/test_protecting_relations.py`
- This plan.

## Approach

2.7.0 made the refusal honest: it named every PROTECT reference instead of only
the baseline. That exposed the real defect. The message named
`ForwardOwnershipReconciliation`, and nothing in the product deletes those rows -
no UI, no API (the ingestion viewset is read-only), no management command. The
operator was told to remove records they could not reach.

**Correction to the diagnosis this work started from.** Reconciliation rows do
not accrue per ingestion. `Meta.constraints` makes them unique per
`(sync, domain)` and every writer is `update_or_create`, so a sync holds at most
three, and they re-point. Verified in `utilities/ownership.py`: `_mark_reconciled`,
`_mark_ownership_pending_locked`, and the virtual-parent path all rewrite
`ingestion_id` in place. The three sibling provenance models re-point too
(`finalize_device_identities_locked`, `reconcile_source_device_tag_claims`,
`reconcile_virtual_parent_claims`), so the general shape is "the newest ingestion
holds everything".

What actually pins an *old* ingestion is narrower and worse. A row re-points only
when its own domain reconciles again. A domain that stops running - scope tags
switched off, virtual parents disabled - freezes its row on the ingestion where
it last ran and pins that ingestion forever. And `required_ownership_domains`
treats the existence of a row as proof the domain is still required, so the same
frozen row also holds ownership permanently incomplete. One row, two dead ends,
and no way to remove it. Cascading clears both at once.

**Why only this one of the four.** `ForwardDeviceIdentity`, `ForwardDeviceTagClaim`
and `ForwardVirtualParentClaim` each describe a live NetBox object that outlives
any single ingestion, and their provenance is the record of which ingestion last
asserted it. A reconciliation row describes no NetBox object: it says only "this
sync finished this domain at this ingestion", which is a statement about nothing
once the ingestion is gone, and which nothing can repair afterwards.

**The risk, and where it is handled.** With CASCADE alone, a sync with no virtual
parents could have its ingestion held *only* by reconciliation rows - and
deleting it would discard the proof that ownership converged, regressing the sync
to Incomplete. The database cannot express "protect only the newest", so
`_ingestion_holds_current_ownership` refuses that one explicitly, defined as
`ownership_generation_complete(sync, ingestion.pk)` rather than as anything
invented here: a COMPLETED row at this generation for every domain
`required_ownership_domains` names. `required_ownership_domains` always appends
STATUS_TAGS, so the required set is never empty and the check can never refuse
every ingestion; one row per `(sync, domain)` means at most one ingestion per
sync satisfies it. The refusal is a wait, not a wall - it lifts as soon as a
newer ingestion reconciles, which is what the message tells the operator to do,
and a test asserts that actually happens.

Deleting an ingestion that holds a *stale* row is safe by the same definition:
`_domain_is_current` already requires the reconciled generation to equal the
latest baseline generation, so such a sync is not current before the delete
either. It cannot regress something already false, and it releases the frozen
row that was keeping the domain required.

An unprovable check refuses. `_ingestion_holds_current_ownership` returns True if
it raises, inverting the house rule for diagnostics, because here a failed check
that permits the delete destroys evidence no reverse migration can restore.

## What This Deliberately Does Not Do

- **The three sibling PROTECT relations stay.** They are the reason the customer's
  newest ingestion is usually undeletable anyway (one identity per synced device),
  and this change does not make that ingestion deletable. It makes the *stale*
  ones deletable and replaces an impossible instruction with a possible one.
- **No new way to delete reconciliation rows by hand.** Adding a UI or command for
  ownership evidence is a larger decision about who may discard convergence state.
  Cascading with the parent needs no new authority.
- **`ForwardContributorBaseline` is untouched**, so the baseline ingestion remains
  undeletable and reports the baseline message even when it also holds current
  ownership evidence - the baseline branch is checked first.
- **A PENDING reconciliation row does not protect.** Only COMPLETED evidence is
  worth refusing for; a pending row would otherwise pin exactly as before. If an
  operator deletes an ingestion while its post-merge ownership work is still in
  flight, that work's marker goes with it and the next sync re-creates it. Not
  defended further: such an ingestion is in practice still held by its device
  identities.

## Validation

- `forward_netbox.tests.test_ingestion_delete` and
  `forward_netbox.tests.test_protecting_relations`: the four bar cases, plus one
  asserting the refusal lifts once a newer ingestion reconciles, plus one pinning
  the CASCADE/PROTECT split across the four provenance models so a future edit to
  the shared mixin cannot take the other three with it.
- `forward_netbox.tests.test_ownership` for the ownership control plane.
- `invoke harness-check`.

## Rollback

Revert, then reverse migration 0047 to restore PROTECT. Rows cascaded away while
0047 was applied are not recoverable, which is the intent of applying it; nothing
else in the schema changes.

## Decision Log

- 2026-08-03: Verified the starting diagnosis before acting on it and corrected
  it. The rows do re-point; the pin is a domain that stops reconciling. The fix
  is unchanged, the reasoning for it is not.
- 2026-08-03: Put the newest-completed guard at the delete path rather than in
  the model, because it is a policy about one row that the database has no way
  to name.
- 2026-08-03: Left the three sibling PROTECT relations alone. Only one of the
  four models is meaningless without its ingestion.

## Open

- The newest ingestion of an actively syncing sync remains undeletable via
  `ForwardDeviceIdentity` (one row per synced device). That is intended -
  identities are live-object provenance - but the refusal for it reads as an
  error rather than as the expected "this is the current ingestion". Worth
  revisiting if the customer reports it.
