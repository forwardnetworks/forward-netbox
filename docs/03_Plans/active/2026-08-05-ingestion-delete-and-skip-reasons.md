# Collect spent baselines, and say what a skip was waiting for

## Goal

Three customer reports in one day, all housekeeping rather than sync failure:
three ingestions that will not delete, and six issue rows reading exactly
`netbox_dlm.softwareversion row processing failed (ForwardDependencySkipError).`
Make the first deletable and the second legible.

## Contract

- A superseded contributor baseline goes with its ingestion. The live one never
  does, by any path, including `queryset.delete()`.
- An ingestion with a running job is refused, as a sync already is.
- A skipped row says "skipped", and names the model it was waiting for when the
  raiser knows it.
- No collected value is persisted. Model labels only.

## Constraints

- `PROTECT` cannot express "keep the live generation, collect the spent one",
  and the collection has to happen before the ingestion row goes, so it cannot
  be done in the delete view either — NetBox's view owns that transaction. The
  guarantee therefore moves to a `pre_delete` receiver, which is the pattern
  `ForwardSync` already uses and the only one that also covers querysets.
- The view's refusal and the receiver's refusal must test the SAME condition, or
  the UI refuses deletes the database allows or offers ones it will abort.
- `log_level` is not a usable signal for skip-versus-fail: the row-oriented
  handler records a dependency skip at `info` while the bulk engines record
  theirs at the default. Keying on it would word one condition two ways.
- No customer identifiers.

## Touched Surfaces

- `forward_netbox/models.py`, `migrations/0050_contributor_baseline_cascade.py`
- `forward_netbox/signals.py` — `refuse_ingestion_delete_with_live_baseline`
- `forward_netbox/views.py` — `_ingestion_holds_live_baseline`, the refusal
- `forward_netbox/exceptions.py` — `dependency` on `ForwardDataError`
- `forward_netbox/utilities/sync_dlm.py`, `sync_primitives.py` — raisers
- `forward_netbox/utilities/sync_reporting.py` — message composition
- tests: `test_ingestion_delete.py`, `test_protecting_relations.py`,
  `test_dlm_integration.py`, `test_sync.py`

## Approach

**Baselines.** The FK becomes CASCADE so a spent generation is collected, and a
`pre_delete` receiver keeps the live one. The receiver also refuses while a job
is running — `ForwardIngestion` carries `JobsMixin` exactly as `ForwardSync`
does, so its `Job` rows would otherwise cascade through the SQL collector and
bypass `Job.delete()`'s RQ-cancel override.

**Skips.** `ForwardDependencySkipError` gains a `dependency` attribute holding a
model label. The raiser's own message names the platform or device, which is why
it cannot be persisted; the label can be. On the delete path the safe answer is
inverted — nothing is missing, the object is still referenced — so that raiser
reports the protecting child models, read from `ProtectedError.protected_objects`.

Raisers not yet taught to name a dependency record exactly what they record
today. There are about thirty; six are covered here, chosen because they are the
two groups a customer is currently reading.

## Validation

A spent baseline is collected with its ingestion; a live one survives a
`queryset.delete()` and raises; the refusal names the live case and says what to
do. The `protecting_relations` suite asserts the baseline is no longer reported
as protecting — and that discovery still finds *something*, because the baseline
was the last visible protecting relation and every remaining one is hidden
behind `related_name="+"`. Full plugin suite over the lot.

## Rollback

Revert, including migration `0050`. The reverse restores PROTECT and can fail
only if an ingestion was deleted while this was applied and took its spent
baseline with it — which is the point of applying it.

## Decision Log

- **CASCADE plus a receiver, not a delete-view helper.** The first attempt kept
  PROTECT and deleted the husk in the view. Rejected: PROTECT forces the
  baseline to go first, and making "baseline deleted, ingestion deleted" atomic
  inside NetBox's delete view needs response-sniffing to know whether the second
  half happened. A half-applied version destroys the record and keeps the row.
- **The wording is keyed on the exception, not the log level.** A test asserting
  `outcome="skipped"` two lines below a message reading "failed" is what made
  the inconsistency obvious.
- **Six raisers, not thirty.** Normalising all of them means replacing the
  context dict at the delete-path site and auditing twenty-odd others; the two
  groups a customer is reading are worth shipping now, and a raiser that says
  nothing still behaves exactly as it does today.

## Open

- The remaining ~24 dependency-skip raisers still record only the exception
  class. The vocabulary already exists in `record_aggregated_skip_warning`
  (`missing-device`, `missing-interface`, ...) and never reaches the database.
- `dcim.site`, `ipam.vrf` and `dcim.devicetype` skips come from pruning, not
  from a missing parent — they mean children still reference the object. They
  now name those children, but they are still filed under a name that reads
  like the opposite. `health_summary_blocks` already calls them "protected
  dependency skips"; the issue list does not.
- Device identities for departed source keys are the other reason an ingestion
  will not delete, and are untouched (#46).
