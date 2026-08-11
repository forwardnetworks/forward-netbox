# Reconcile removals on a full execution

## Goal

Stop a full run from leaving behind every row a map wrote before it was
re-pointed at a different query. This is the root cause behind the customer's
duplicate DLM hardware notices, and it applies to every model.

## Constraints

- A narrowed query looks exactly like real churn. Removal is destructive and a
  refused run is not, so the comparison carries its own limit that no drift
  policy can widen.
- Only a whole-model fetch may speak for the whole model; a shard holds part of
  it by construction.
- An empty current result must never remove anything.
- Inability to prove what was written must mean "remove nothing".
- No extra Forward call: the evidence is already local.

## Touched Surfaces

- `forward_netbox/utilities/full_removal_reconciliation.py` (new)
- `forward_netbox/utilities/query_fetch_execution.py` - full-mode return and
  `_full_run_removals`
- `forward_netbox/tests/test_full_removal_reconciliation.py` (new)

## Approach

Removals previously reached NetBox one way only: a Forward NQE diff, which
reports what the CURRENT query stopped returning. A full run computed none
(`delete_rows` was empty outside device-tag scope pruning). So re-pointing a map
- switching the device-type maps to their alias-aware variants, for instance -
orphaned every row the previous query wrote, permanently, because the baseline
relation holding them is declared incompatible (`map_set_changed`) and never
consulted again.

The proof of what the plugin wrote is already local and already checksummed: the
promoted contributor baseline persists every row each contract returned. The
comparison reads all relations for the MODEL rather than the current contract
key, which is the entire point - a re-pointed map writes under a new contract
key, and the previous relation is exactly what nothing else will ever read.

Identity is the model's coalesce key, built in this module rather than borrowed
from `row_shard_key`. That function is for bucketing work and is unsafe here in
two distinct ways: it falls back to a whole-row key when no coalesce set is
complete, turning any field change into "absent"; and for device-scoped models
it returns `device:<name>`, which every row of that device shares. Both are
correct for sharding and wrong for deciding what to delete.

## Validation

`forward_netbox/tests/test_full_removal_reconciliation.py`, weighted toward what
must NOT be removed: an empty result, a missing baseline, a row whose identity
is incomplete, and a removal set large enough to look like a narrowed query. The
customer's case - the same hardware under a part number and under the Device
Type Library slug - is pinned directly.

## Rollback

Revert. Full runs stop reconciling removals and leftovers accumulate again;
nothing else changes, because the diff path is untouched.

## Decision Log

- **A second limit rather than reusing the validation row-shrink guard.** That
  guard is the right first line but skips comparison entirely when the
  operator's scope configuration changed - precisely the situation that
  produces a large, legitimate-looking removal set - and a drift policy can
  relax it. This one cannot be widened by configuration.
- **Refuse per model, never fail the run.** Not removing is always safe. Taking
  a whole sync down over an advisory comparison would trade a cosmetic problem
  for an outage.
- **Compare by model, not by contract key.** Comparing within a contract would
  reproduce the bug: the orphaned rows live under the old key.
- **No opt-in setting.** A row the plugin wrote and no longer sees is stale by
  definition; making that opt-in would leave the default behaviour wrong and
  every existing deployment carrying the bug.

## Open

- The comparison is bounded by what one full execution returns, so a model that
  only ever runs diff (`diff_eligible` with a live baseline) will not reconcile
  until it next runs full. That is the correct conservative order - a diff run
  has no authority over rows it did not fetch - but it means the leftovers clear
  on the next full execution rather than immediately.
