# Hidden PROTECT Relations

## Goal

Make a refused ingestion delete say why, instead of failing after rendering
several hundred rows the operator cannot act on.

## Contract

- An ingestion held by any PROTECT reference reports that reference and its
  count before the delete is attempted.
- Expected protection stays a warning; unexpected protection stays an error.
- No delete that previously succeeded is refused.

## Constraints

- Diagnostics must never raise; a failed check must not mask the failure.
- The merge path's delete prediction is not changed by this work.

## Touched Surfaces

- `forward_netbox/utilities/bulk_merge.py` - new `protecting_relations`,
  used by `describe_protecting_references`
- `forward_netbox/views.py` - corrected docstring
- `forward_netbox/tests/test_protecting_relations.py` - new
- This plan.

## Approach

A customer could not delete three ingestions, and each new sync added another.

`ForwardIngestionProvenanceMixin` declares its ingestion FK as `PROTECT` with
`related_name="+"`. Django treats such relations as *hidden* and omits them from
`_meta.related_objects`, which is what the protection check iterated. Confirmed
against the live schema: of five PROTECT relations pointing at an ingestion,
the check could see exactly one.

    VISIBLE  ForwardContributorBaseline
    HIDDEN   ForwardDeviceIdentity, ForwardDeviceTagClaim,
             ForwardVirtualParentClaim, ForwardOwnershipReconciliation

So an ingestion held only by device identities reported no refusal, the delete
view fell through to NetBox's confirmation page, rendered one dependent row per
synced device, and then failed with `ProtectedError` on confirm. Every ingestion
owns one identity per device, so every ingestion was undeletable this way -
which is why the count grew with each sync rather than staying at one.

`protecting_relations` asks for hidden relations explicitly, so protection is
read from what the database will actually enforce.

**Scope.** The same blind spot exists in `protecting_reference_blocked_deletes`,
which predicts which merge deletes a PROTECT reference will reject. It is
deliberately left alone. Widening it was tried and measured: it regressed
`test_owned_device_delete_preserves_provenance_until_atomic_merge` (the device
survived) and two concurrency tests, because `ForwardDeviceIdentity.device` is
also a hidden PROTECT, and the merge removes that identity in the same atomic
transaction. Treating it as a static blocker skips a delete that would have
succeeded - the failure mode that function's own docstring names as the worse
one, because it loses a real delete silently. Teaching the predictor which
referencing rows this plugin clears in-transaction is a separate change.

## Validation

- Tests pin relation discovery itself rather than one caller, because the defect
  was in discovery and had two callers.
- One test asserts `related_objects` alone would still miss these, so if a future
  Django exposes hidden relations the helper is revisited rather than kept
  forever out of habit.
- `test_bulk_merge` + new tests: 96 tests OK. The same run with the merge
  predictor also widened was 3 failures, which is the measurement above.

## Rollback

Revert. Ingestions become undeletable again with no explanation.

## Decision Log

- 2026-08-01: Fixed discovery, not the two call sites, because the same omission
  had already reached two of them.
- 2026-08-01: Left the merge predictor untouched after measuring a regression.
  Shipping the half that is proven beats shipping both halves on reasoning.

## Open

- The merge predictor blind spot: a merge delete blocked by a hidden PROTECT is
  never predicted, so it is scheduled and fails at apply time, and a failed row
  blocks baseline promotion. Deferred with the reproduction recorded above.
