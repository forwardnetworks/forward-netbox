# A Completed Run Says Which Models It Changed

## Goal

The change explainer reports created, updated and deleted counts per model for a
completed run, so a support bundle can answer "what did this run change?"

Before this, a merged run's branch is deprovisioned, so the explainer reported
`branch_missing` for every completed run. A customer's second sync created 6,326
rows and the bundle could not say which models held them.

## Constraints

- No customer names, network ids, snapshot ids or hostnames in the repo.
- The branch path is unchanged: a run that still has its branch is explained from
  the branch as before.
- The result is unavailable, never zero, when the change log holds nothing for the
  run, and the reason says why: `fast_baseline_no_change_log` (a fast baseline
  writes no change log by design), `branch_missing` (the run never merged),
  `no_change_log_for_run` (a merged ordinary run, which looks like a purge), or
  `change_log_query_skipped` (the guarded count timed out).

## Touched Surfaces

- `utilities/change_explainability.py`: `_change_log_summary` reads counts from
  NetBox's change log under the ingestion's `change_request_id` when there is no
  branch. Counts only; the field detail stays empty and is marked
  `field_detail_available: false`.
- `templates/forward_netbox/partials/change_explainability.html`: message for the
  `no_change_log_for_run` reason.
- `tests/test_change_explainability_change_log.py`.

## Approach

1. No branch: group `core.ObjectChange` rows by object type and action for the
   ingestion's request id, in the database, and return the same keys the branch
   path returns.
2. No rows: return `available: false` with reason `no_change_log_for_run`.
3. The ingestion page and the support bundle both call this function, so both pick
   it up. The bundle's existing label index already names the card.

## Validation

- New tests: counts by model and action, another run's changes excluded, empty
  change log unavailable, no request id stays `branch_missing`.
- Existing `test_change_explainability_full_counts` and `test_log_export` pass.
- Live, on the test host stack: the baseline run (963,392 changes, no branch)
  reports its per-model counts in 0.2 seconds, matching the run's own totals.

## Rollback

Revert the commit. Nothing is migrated.

## Decision Log

- Rejected: keeping the branch alive after merge. Branch deprovisioning is how
  Branching keeps the schema count down.
- The counts are what the merge wrote. They are not identical to the branch's
  ChangeDiff counts: they can include merge side effects, such as objects created
  along with a parent, and field detail is not available.
- The count is guarded with the same time limit as the other change-log reads.
- The change log is the source because the same request id already proves
  ownership for the stale-row cleanup and the platform restore.
