# Fetch a Device-Scoped Query in Parts When Forward Stops It at Its Time Limit

## Goal

A device-scoped full query that Forward stops at its own NQE time limit (outcome
`TIMED_OUT`, reason `nqe-timed-out`) no longer fails the model. The plugin fetches
it again in halves of its device list, up to four levels deep (16 parts), and
merges the parts into the result the single query would have returned.

A customer's cable query ran past the limit as one execution over ~3,500 devices,
so cables were not refreshed that run. The source timeout does not govern this
limit, so raising it does not help.

## Constraints

- Only a full fetch whose rows are a function of the device list is split: a diff
  fetch with a baseline, a contributor (Tier 3) fetch, and a fetch that already
  carries a shard are not.
- The removal comparison (`_full_run_removals`) must run once, on the merged
  result: a part holds part of the model, and comparing it alone would read the
  rest as removed.
- The workload is recorded against the whole device list, so durable state sees
  the same scope as an unsplit run.
- A `TIMED_OUT` outcome is still not retried as is.
- IP addresses and MAC addresses are never split (`TIME_LIMIT_SPLIT_REFUSED_MODELS`):
  their queries pick one winner per address across devices, so each half would
  pick its own and the merge could hand a shared address to the wrong device.

## Touched Surfaces

- `forward_netbox/utilities/query_fetch_execution.py`:
  `_fetch_spec_rows_splitting_on_time_limit`, `_time_limit_split_keys`,
  `TIME_LIMIT_SPLIT_MAX_DEPTH`, `_fetch_spec_rows(skip_full_run_removals=...)`.
- `forward_netbox/tests/test_time_limit_split_fetch.py`.
- Scope Reconciliation label "Carry no include tag" renamed "No include tag
  (uncovered + not managed)" (template, reconciliation strip, bundle label index
  and UI figures, Health message, troubleshooting doc): a customer read it as the
  uncovered count.

## Approach

`_execute_workload_job` calls `_fetch_spec_rows_splitting_on_time_limit`. On a
`ForwardClientError` with `outcome == "TIMED_OUT"` and a splittable contract, it
logs a warning, fetches each half with a copy of the contract whose
`forward_netbox_shard_keys` is that half (recursing on a half that times out
again), keeps each distinct row once, sums `matched_type_rows_skipped`, records
the whole list in `query_parameters`, and runs the removal comparison once.

## Validation

- `invoke test-isolated --test-label forward_netbox.tests.test_time_limit_split_fetch`
- Full suite before release.
- Live: not reproducible on demand (needs Forward to time out); the halving path
  is covered by unit tests only. Say so in the release notes.

Known limits:

- Each level that times out costs Forward's full time limit before the next
  level runs; at the maximum depth that is up to 15 timed-out executions before
  the 16 final parts. A deadline set by the workload fetch budget still applies.
- A transient error in one part restarts the whole workload from the top, which
  pays the first time limit again.
- A map with a diff baseline is not split even when its diff was blocked and the
  full path ran; its incremental runs still fail on the time limit.

## Rollback

Revert the commit; a timed-out query fails its model as before.

## Decision Log

- Rejected: raising the source timeout. It does not govern Forward's NQE limit.
- Rejected after review: splitting IP and MAC queries (see Constraints).
- Rejected: splitting on any failure. Only the time limit is helped by a smaller
  device list; other failures would fail each part the same way.
