# Unsatisfiable merge row accounting

## Goal

A row NetBox refuses on its own validation rule must stop blocking convergence
end to end, not only inside the merge orchestrator. The 2.6.12 skip disposition
already lets such a row attest the merge; the row is still persisted as a failed
change, and every readiness surface downstream keys off that counter, so the
customer-visible state is unchanged from a hard block.

## Contract

- A destination-rule rejection (`ValidationError` at merge) is recorded as an
  ingestion issue, counted as a skipped change, and does not appear in
  `failed_change_count`.
- Baseline promotion, drift evidence, and the ingestion health check treat a
  run whose only exceptions are skipped rows as complete-with-exceptions, never
  as failed.
- Integrity and every other exception remain failures: they still raise
  `ForwardPartialMergeError`, still leave the branch `READY`, still block
  baseline promotion, and still require an explicit operator acceptance.
- The persisted counters keep summing to the branch's logical change total, so
  a partial-merge retry cannot trip the cumulative-evidence guard.
- Skipped rows stay visible: as ingestion issues, in per-model statistics, in
  the drift evidence, and in the health message.

## Constraints

- Highest-risk area (staging/merge). No new or reordered dependency edges in
  the merge graph.
- `netbox_branching` stays in the ingestion path.
- Do not weaken `release_owned_primary_ip_claims`' fail-closed guards. Releasing
  a primary-IP pointer this sync cannot prove it owns is worse than the failure.
- Persisted diagnostics stay free of customer data.
- Do not weaken or delete existing tests.

## Touched Surfaces

- `forward_netbox/models.py`, `forward_netbox/migrations/0047_*`
- `forward_netbox/utilities/merge.py`
- `forward_netbox/utilities/ingestion_merge.py`
- `forward_netbox/utilities/drift_report.py`
- `forward_netbox/utilities/health_checks.py`
- `forward_netbox/templates/forward_netbox/forwardsync_drift_report.html`
- `forward_netbox/tests/test_merge_rule_rejection.py`
- This plan.

## Approach

**The mechanism.** `merge_branch` counts destination-rule rejections separately
(`progress_unsatisfiable`) and subtracts them before deciding whether to raise
`ForwardPartialMergeError`, so the branch attests and post-merge bookkeeping
runs. But the line above that decision,
`ingestion.record_change_totals(applied=..., failed=failed, ...)`, persists the
*raw* failure count, unsatisfiable rows included. Everything that reads
readiness afterwards reads that field:

- `drift_report.build_latest_sync_evidence` - `if counters["failed"] ... status
  = "failed"`, which is why drift reads as a failed run rather than a measured
  one.
- `health_checks.ingestion_check_status` - `if ingestion.failed_change_count:
  return "fail"`.
- `health_summary_blocks.throughput_summary` - "remains incomplete".
- `ForwardIngestion.can_accept_merge_failures` - offers an acceptance action for
  rows that are already skipped.

So the 2.6.12 disposition does match this exception, and it does what it was
written to do; the defect is that the skip never reaches the persisted
counters. One row that can never be satisfied still presents as a failed sync
forever.

**The fix.** Persist the two dispositions separately. `record_change_totals`
gains a `skipped` counter backed by a new `skipped_change_count` field;
`merge_branch` records `failed=retryable_failed, skipped=unsatisfiable` - the
same split it already computes for the raise decision, now written down. The
per-model statistics increment `skipped` rather than `failed` for those rows.

The counters must keep summing to the logical branch total, because
`get_unmerged_changes()` returns every change while the branch is `READY`: a
partial-merge retry recomputes the same total and refuses to run if it differs
from `applied + failed` recorded previously. That guard therefore has to become
`applied + failed + skipped`, or a mixed run (one retryable failure plus one
unsatisfiable row) would raise on retry - a new dead end in place of the old one.

**Why this cannot create a dependency cycle.** The change touches counters,
persisted evidence and presentation only. It adds no edge to the collapsed
change graph, does not reorder `_order_collapsed_changes_fast`, does not change
which rows are staged or in what order they merge, and does not alter the
apply/skip decision for any row. Both `bulk_merge_changes` and the per-row
fallback see identical inputs and produce identical outcomes; only the
bookkeeping after the last row differs.

**Why not fix the release path instead.** The rejection is structural for the
cases `release_owned_primary_ip_claims` deliberately declines: the previous
holder out of scope, owned by another sync, unowned, or added to main after
branch provisioning. On update the bulk path runs `clean_fields()` and not
`IPAddress.clean()`, so the branch stages the move and NetBox applies its rule
at merge against main. Every guard that declines to release is fail-closed by
design, and widening any of them would clear a primary-IP pointer this sync
cannot prove it owns. The correct behaviour for those rows is exactly what the
disposition says: record, skip, keep converging. This change makes that true
downstream; it does not touch the release path.

## Validation

- `test_merge_rule_rejection` (extended): a validation rejection is recorded as
  an issue, counted as skipped, does not raise, attests the merge and promotes
  the baseline; an `IntegrityError` on the same path still raises
  `ForwardPartialMergeError`, still counts as failed, and leaves the baseline
  unpromoted; a mixed run keeps the counters summing to the logical total.
- `test_primary_ip_integration`, `test_bulk_merge`, `test_sync`,
  `test_accepted_merge_failures`, `test_ingestion_merge`, `test_health`,
  `test_models`, `test_migrations`, `test_log_export`, `test_issue_diagnosis`,
  `test_fast_baseline`.
- `python -m invoke harness-check`, `python -m invoke harness-test`.
- Inspect each command output for the literal `Ran N tests` and `OK`.

Evidence, isolated runtime (`forward-netbox-agent-pip-iso`, NetBox 4.6.6,
netbox_branching 1.1.2):

- `test_primary_ip_integration test_bulk_merge test_sync`:
  `Ran 413 tests in 490.241s` / `OK`.
- `test_merge_rule_rejection test_accepted_merge_failures test_ingestion_merge
  test_health test_models test_migrations test_log_export test_issue_diagnosis
  test_fast_baseline`: `Ran 270 tests in 138.128s` / `OK`.
- `invoke harness-test`: `Ran 263 tests in 1.334s` / `OK`.
- `invoke harness-check`: `Harness check passed.`
- `manage.py makemigrations --check --dry-run forward_netbox`:
  `No changes detected in app 'forward_netbox'`.

Diagnosis evidence, before the fix, from a probe driving `sync_merge` with one
injected merge-row exception:

- `ValidationError`: `merge_applied_at` set, `baseline_ready` True,
  `failed_change_count` 1, drift evidence status `failed`. The skip disposition
  fires; the counter still says the run failed.
- `IntegrityError`: raises `ForwardPartialMergeError`, `merge_applied_at` None,
  `baseline_ready` False, drift evidence status `failed`. Unchanged by this work.

## Rollback

Revert the commits. The added field is additive with a zero default and is only
written by the merge path; nothing reads it when absent. Older ingestions keep
`skipped_change_count = 0`, which is exactly how they were recorded.

## Decision Log

- 2026-08-03: A separate persisted counter rather than folding skipped rows into
  `applied`. A row that was not applied must never present as applied - that is
  the 2.6.4 defect this codebase already paid for once.
- 2026-08-03: A new field rather than a `snapshot_info` key. The retry guard
  compares against the persisted counters, so the split has to live where those
  counters live or the sum silently stops matching the branch total.
- 2026-08-03: The health check reports a skipped-only run as `warn` via the
  existing blocking-issue path, not `pass`. The rows are real exceptions and
  stay visible; they simply do not block promotion any more.
- 2026-08-03: No change to `release_owned_primary_ip_claims`. Its guards are
  fail-closed on purpose and a skipped pointer release is the safe outcome.
- 2026-08-03: The published `2.6.9` does not contain the skip disposition at
  all. It landed in the commit tagged `2.6.10`, which was never published, and
  reached PyPI only as `2.6.12`; the release note quoting it is 2.6.12's. On
  `2.6.9` this failure is still a hard block with no attestation, so an
  installation reporting these symptoms must be version-checked before its
  behaviour is read as this defect.
