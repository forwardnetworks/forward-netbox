# A refused delete is retried, not recorded as done

## Goal

Stop the durable workload state from promoting a delete the apply refused, so
the row is retried on the next run instead of silently diverging.

## Why

The state is staged BEFORE the branch applies and promoted at merge, so it
records the deletes the DELTA computed rather than the ones that happened.
`newly_explicit_deletes` treats a previous `delete` entry as settled, so a
refused delete was skipped on every subsequent run: the row stayed in NetBox,
the plugin believed it was gone, nothing retried, and the report went quiet.

2.8.9 closed the PROTECT half by never staging a delete a surviving child
holds. Its plan recorded the rest as open: "a delete that fails for a reason
OTHER than protection is still tombstoned optimistically."

## Constraints

- Identities only, never rows. The canonical identity is derived from the
  model's coalesce fields, which is what the state is keyed by already.
- An ordinary run must do no extra work: no decode/encode when nothing was
  refused.
- A refusal must never remove an UPSERT entry for the same identity - the run
  may have written the row and refused a delete of something else keyed alike.

## Touched Surfaces

- `sync_reporting.py` - `record_refused_delete` at every non-success path in
  `delete_model_rows` (falsy return, dependency skip, search/query error,
  validation/integrity error, and the generic catch);
  `persist_refused_delete_identities`.
- `single_branch_executor.py` - the refusals travel on the ingestion's
  `snapshot_info`, saved with the model results.
- `workload_state.py` - `refused_delete_identities`,
  `_without_refused_deletes`, applied in `promote_workload_states_locked`.

## Approach

Dropping the entry, not marking it. An identity absent from the promoted
state is exactly the state the delta reads as "not yet deleted", so the next
run recomputes the delete and tries again - no new action type, no new field,
and the existing `newly_explicit_deletes` logic does the retry unchanged.

The refusals travel on `snapshot_info` because promotion happens in a
different transaction at merge time, in `ingestion_merge`, which has the
ingestion and not the runner.

## Validation

`test_refused_deletes_are_retried.py`: a refused delete is absent from the
promoted state while a successful one stays; an upsert entry is never dropped;
an ordinary run promotes byte-identical; nothing is persisted when nothing was
refused; the recorded identity IS the state key; an unkeyable row is recorded
harmlessly. Adjacent: `test_workload_state`, `test_sync`, `test_dlm_integration`.
Full Django suite.

## Rollback

Revert. Refused deletes are tombstoned again.

## Decision Log

- **Drop the entry rather than record a failed action.** A third action would
  need every reader of the state taught about it; absence already means
  exactly what is wanted.
- **Every non-success path, not just the exceptions.** A handler returning
  falsy is a refusal too - that is the protected-delete shape - and it was the
  one most likely to be silent.

## Open

- Nothing.
