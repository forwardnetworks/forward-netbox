# Stop a provenance stamp pinning its ingestion

## Goal

Make an ingestion deletable once nothing but an old provenance stamp holds it,
without releasing ownership of any live device.

## Contract

- Ownership must survive. The sync, device, source key and `snapshot_id` on
  each evidence row are untouched; only the pointer to a deleted run goes.
- No device changes hands. Nothing is pruned, released, or re-adopted.
- `ForwardOwnershipReconciliation` keeps CASCADE. It is a child record of the
  ingestion, not evidence held against it.

## Constraints

- The evidence rows that pin an ingestion are NOT stale - the devices still
  exist and are still owned. Only the stamp is old. Any fix that prunes them
  releases ownership of live devices that are merely out of scope, and devices
  leave scope for benign reasons: a Forward-side tag edit is enough.
- `exclude(ingestion__sync_id=F("sync_id"))` compiles to a LEFT OUTER JOIN
  guarded by `ingestion.sync_id IS NOT NULL`. A null stamp satisfies the
  negation, so the cross-sync mismatch counters must require a stamp before
  comparing it. `exclude(ingestion_id=generation)` behaves the OPPOSITE way and
  correctly counts a null stamp as stale - verify the compiled SQL, do not
  reason from the Python.
- `related_name="+"` makes these relations invisible to
  `_meta.related_objects`; `protecting_relations` reads them explicitly.

## Touched Surfaces

- `forward_netbox/models.py`, `forward_netbox/migrations/0051_...py`
- `forward_netbox/utilities/ownership.py`, `forward_netbox/utilities/bulk_merge.py`
- `forward_netbox/tests/test_ingestion_delete.py`,
  `test_protecting_relations.py`, `test_ownership.py`

## Approach

Change the shared `ForwardIngestionProvenanceMixin.ingestion` from PROTECT to
SET_NULL (nullable, blank), and guard the two cross-sync mismatch counters so a
null stamp is not mistaken for a stamp pointing at another sync.

## Validation

- `invoke test-isolated` - full plugin suite, 1993 tests, OK (4 skipped)
- `invoke makemigrations` reports no model drift
- The new counter test was confirmed to FAIL without the guard (`1 != 0`) before
  being accepted as a regression test

## Rollback

Revert both. The reverse migration restores PROTECT and non-null, and fails if
any row already has a null stamp - which is the expected state once an
ingestion has been deleted, so reversing means deciding what those rows should
point at.

## Decision Log

- **The stamp is provenance, not a dependency.** Reframing it that way is what
  made this tractable after three wrong models. The question is not "is this
  evidence still valid" (it is) but "should a record of ownership block the
  deletion of the run that recorded it".
- **Rejected: re-point every row on each merge.** It would assert the current
  run saw devices it never saw, and would permanently zero the `stale_claims`
  signal.
- **Rejected: prune evidence for departed devices (#46).** The dangerous
  option, for the reason in Constraints.
- **A subagent sweep found the counter defect.** It contradicted the premise I
  gave it and was right; the compiled SQL settled it. Worth repeating for any
  nullable-FK change.

## Open

- #46 is now optional rather than blocking. Pruning genuinely dead identities
  may still be worth doing, but it is no longer what stands between an operator
  and deleting an old ingestion.
