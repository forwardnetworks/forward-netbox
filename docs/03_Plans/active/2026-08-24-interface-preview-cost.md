# The interface preview stops validating rows it never writes

## Goal

Cut the first-sync cost of the `dcim.interface` drift comparison - the
largest compared model on the reporting deployment at 357,274 rows - without
moving a count.

## Why

Measured after the same treatment worked for `dcim.macaddress`. At 16,000
first-sync rows the comparison cost 29,139 ms and 9,000 queries. Converged, it
already cost 0.088 ms/row and 36 queries, so the exposure is a first sync or a
badly drifted estate, not the reporting deployment's steady state - it measures
~21 s there and needs nothing.

## Constraints

- The APPLY path must be byte-identical; every change sits behind `if preview`.
- Counts must not move, except the one divergence named below.
- The object must still be constructed and must still flow into LAG
  resolution, which reads it.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` - `bulk_orm_apply_interface`
- `forward_netbox/tests/test_drift_comparison.py` -
  `InterfacePreviewCostContractTest`
- `forward_netbox/tests/test_preview_primes_its_lookup_caches.py` - a canary
  re-aimed, and a documented belief corrected

## Approach

`_validate_interface` runs `full_clean`, which a preview does not need. It is
skipped on both the create and the update branch. Unlike the macaddress fix
there is NO sentinel: the `Interface` is still constructed and still flows into
LAG resolution, because construction turned out to be cheap.

The update branch additionally skips `_snapshot_once` and the `setattr` loop,
which exist to stage a write. Honest scope on that one, having tried to justify
it harder and failed: it is not fixing a laundering bug like `_ensure_fhrp_vip`
had - duplicate rows for one interface are deduplicated upstream, so no second
row reads the mutated object - and it is not measurable, 36 queries either way.
It stands on being work a preview provably does not need, nothing more.

### A documented belief, corrected by measurement

`test_preview_primes_its_lookup_caches` stated that a create-path row "is
instantiated, and NetBox charges two queries per instance for content type and
custom field defaults", and that "the only way to avoid them is to stop routing
the preview through the real apply".

The middle step is wrong. The per-row charge came from `full_clean`, not from
`Interface(**defaults)`. The preview still instantiates, still resolves, still
classifies through the same code, and the queries went flat anyway.
Instantiation is cheap; validating is not.

That module's canary asserted the create path costs MORE QUERIES for more rows,
as proof the preview still went through the real apply. That proxy is dead - and
asserting a dead proxy would have forced the cost back to keep a test green. It
now asserts the property it was really protecting: every row is still classified
individually, checked on the counts, which is also what an operator reads.

### The divergence

Same shape as macaddress, and taken for the same reason: a row `full_clean`
would reject counts as a create rather than failed. Overstates drift, never
understates it. Pinned by
`test_an_invalid_interface_row_counts_as_a_create_under_preview`.

## Validation

16,000 rows, same box:

| | ms/row | queries | wall |
| --- | --- | --- | --- |
| first sync, before | 1.821 | 9,000 | 29,139 ms |
| first sync, after | 0.092 | 36 | 1,467 ms |
| converged, before | 0.088 | 36 | 1,408 ms |
| converged, after | 0.079 | 36 | 1,263 ms |

~20x on a first sync; converged unchanged, as expected. Extrapolated to
357,274 rows: ~10.7 min becomes ~33 s on a first sync.

Negative control: restoring `_validate_interface` under preview puts it back to
28,883 ms / 9,000 queries, so the win is attributable to that call and nothing
else.

481 tests green, including all of `test_sync` and `test_apply_engine`.

## Rollback

Revert. The comparison validates every row again, correct and ~20x slower on
create-heavy runs.

## Decision Log

- **No sentinel here.** macaddress needed one because constructing the model
  was the cost; here it is not, and the object is read by LAG resolution.
- **Re-aim the canary rather than delete or satisfy it.** It was protecting a
  real property through a proxy that this change invalidated. Deleting it would
  drop the protection; satisfying it would restore the cost to keep a test
  green.
- **Say plainly that the snapshot skip is unproven.** It is defensible as dead
  work and nothing stronger; claiming a bug fix it does not deliver is how the
  retracted macaddress claim happened.

## Open

- The reporting deployment's unexplained 270 s macaddress cost is unaffected by
  this and still open; see `2026-08-24-macaddress-preview-cost.md`.
- Three adapter models remain uncompared: lifecycle (netbox-dlm), peering,
  routing.
