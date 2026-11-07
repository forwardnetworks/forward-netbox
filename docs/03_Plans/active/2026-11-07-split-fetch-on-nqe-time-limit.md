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

- Only the FULL execution of a query whose rows are a function of the device list
  is split. The error `_fetch_spec_rows` raises from its full call is marked; an
  error from a diff fetch, from a contributor (Tier 3) fetch, or from a fetch that
  already carries a shard is never split. A diff that fails (timed out, circuit
  open, unverified before-scope, full-run forced) falls back to the full call
  inside `_fetch_spec_rows` as before, and it is that full attempt that splits if
  it times out, even for a diff-eligible contract with a baseline. Parts are
  fetched with `force_full_path`, so none runs as a diff.
- The removal comparison (`_full_run_removals`) must run once, on the merged
  result: a part holds part of the model, and comparing it alone would read the
  rest as removed.
- The workload is recorded against the whole device list, so durable state sees
  the same scope as an unsplit run.
- A `TIMED_OUT` outcome is still not retried as is, at any level.
- A transient error (see `_is_transient_fetch_error`) in one part retries that
  part alone, with the workload's attempts and backoff
  (`_workload_fetch_retry_config`) inside the workload `deadline`; the parts
  already fetched are kept. A part that used its retries marks the error so
  `_execute_workload_job` does not restart the whole workload. A permanent error
  in a part fails the model.
- Cost is capped per workload: the split stops, and the model fails with the
  original time-limit error, when the workload `deadline` has less than
  `TIME_LIMIT_SPLIT_MIN_REMAINING_SECONDS` left or after
  `TIME_LIMIT_SPLIT_MAX_TIMED_OUT_EXECUTIONS` (15) time-limited executions. The
  executions spent are logged.
- IP addresses and MAC addresses are never split (`TIME_LIMIT_SPLIT_REFUSED_MODELS`):
  their queries pick one winner per address across devices, so each half would
  pick its own and the merge could hand a shared address to the wrong device.

## Touched Surfaces

- `forward_netbox/utilities/query_fetch_execution.py`:
  `_fetch_spec_rows_splitting_on_time_limit`, `_fetch_split_part`,
  `_time_limit_split_keys`, `_time_limit_split_stop_reason`,
  `TIME_LIMIT_SPLIT_MAX_DEPTH`, `TIME_LIMIT_SPLIT_MAX_TIMED_OUT_EXECUTIONS`,
  `TIME_LIMIT_SPLIT_MIN_REMAINING_SECONDS`,
  `_fetch_spec_rows(skip_full_run_removals=..., force_full_path=...)`, the retry
  loop in `_execute_workload_job`.
- `forward_netbox/tests/test_time_limit_split_fetch.py`.
- Scope Reconciliation label "Carry no include tag" renamed "No include tag
  (uncovered + not managed)" (template, reconciliation strip, bundle label index
  and UI figures, Health message, troubleshooting doc): a customer read it as the
  uncovered count.

## Approach

`_execute_workload_job` calls `_fetch_spec_rows_splitting_on_time_limit`. On a
`ForwardClientError` with `outcome == "TIMED_OUT"` and a splittable contract, it
logs a warning, fetches each half with a copy of the contract whose
`forward_netbox_shard_keys` is that half (`_fetch_split_part`: full path only,
recursing on a half that times out again, retrying a half that hits a transient
error), keeps each distinct row once, sums `matched_type_rows_skipped`, records
the whole list in `query_parameters` and the failed full call's fetch metadata
(so a diff fallback still reads as one), and runs the removal comparison once.

## Validation

- `invoke test-isolated --test-label forward_netbox.tests.test_time_limit_split_fetch`
- Full suite before release.
- Live: not reproducible on demand (needs Forward to time out); the halving path
  is covered by unit tests only. Say so in the release notes. The tests run the
  real `_fetch_spec_rows` and `_execute_workload_job` with only Forward replaced.

Known limits:

- Each level that times out costs Forward's full time limit before the next
  level runs; at the maximum depth that is up to 15 timed-out executions before
  the 16 final parts. Without a workload fetch budget (`deadline` unset, the
  default) the only bounds are the depth and the execution cap, which describe
  the same tree; set `workload_fetch_timeout_seconds` to bound the wall clock.
  The execution cap is a backstop that matters if the depth is raised.
- A diff fetch or contributor fetch that itself exceeds Forward's limit is still
  not split; it falls back to full (diff) or fails (contributor) as before.

## Rollback

Revert the commit; a timed-out query fails its model as before.

## Decision Log

- Rejected: raising the source timeout. It does not govern Forward's NQE limit.
- Rejected after review: splitting IP and MAC queries (see Constraints).
- Rejected: splitting on any failure. Only the time limit is helped by a smaller
  device list; other failures would fail each part the same way.
