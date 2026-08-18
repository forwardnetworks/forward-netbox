# Measure drift against the fetch, not against the plan

## Goal

Stop a model that is perfectly in sync from being reported as the most
uncertain thing on the page.

## Why

A deployment's drift report read `Models compared 2 / 32`. Every unmeasured
model showed change candidates exactly equal to its Forward row count -
357224/357224 for interfaces, 121978/121978 for MAC addresses, 39353/39353 for
IP addresses. That equality is not drift. It is the signature of the
upper-bound fallback, which is what the report prints when it has no comparison
at all: `estimated_changes = row_count + delete_count`.

The preview built its comparison by iterating `workloads`, which is the PLAN.
The plan legitimately omits any model with nothing to stage -
`apply_durable_workload_deltas` ends with

    candidate = replacements.get(position, workload)
    if candidate.estimated_changes:
        normalized.append(candidate)

and `estimated_changes` is `len(upsert_rows) + len(delete_rows)`. A model
unchanged since the last run has both empty, so it is dropped. Correct for a
plan. Wrong for a measurement, because the comparison was reading the same
list: the model had no key in `comparison_by_model`, `.get()` returned `None`,
and the report fell back to estimating every fetched row.

The result was an exact inversion. **The models in perfect sync were the ones
displayed as maximally uncertain**, and the more converged a deployment was the
worse its report looked. A first run, with everything to do, measured fine.

It also explains the two models that did report measured. `dcim.device` kept a
non-empty delta and genuinely compared to zero. `netbox_dlm.softwareversion`
had a delete-only delta - non-zero `estimated_changes`, empty `upsert_rows` -
which reached the empty-row-list shortcut removed earlier today in the
`empty comparison is not a measurement` change. The two are the same defect
seen from opposite ends: one invented a measurement from no rows, the other
threw the rows away before measuring.

## Constraints

- The plan must keep dropping zero-change workloads. Staging an item with
  nothing in it is the bug that filter exists to prevent, and this change must
  not touch it.
- An empty delta is NOT evidence of agreement with NetBox. The delta is
  computed against this plugin's own record of what Forward last returned, so
  it says Forward has not changed and nothing else. Reporting zero drift from
  it would be the same unearned confidence just removed from the empty-row
  shortcut.
- The sync path must not pay for this. Holding every fetched row alive is a
  memory regression precisely at the scale where drift measurement matters.

## Touched Surfaces

- `forward_netbox/utilities/query_fetch_execution.py` - capture the normalised
  rows before the durable delta narrows them, behind a keyword-only opt-in
- `forward_netbox/views.py` - the preview opts in and measures against them
- `forward_netbox/tests/test_converged_models_are_measured.py` - new
- `.pre-commit-config.yaml` - the new test is docstring-first

## Approach

The rows exist already: they are on the workloads immediately after
`normalize_dependency_workloads` and immediately before
`apply_durable_workload_deltas`. Capture them there, keyed by model, and have
the preview compare against that map instead of against the plan.

The capture is `capture_comparison_rows=False` by default and keyword-only.
Seven call sites reach `fetch_workloads`; only the dependency preview opts in.
The sync path - `single_branch_executor` - keeps today's behaviour, where the
rows the delta discards become garbage immediately.

## Validation

`forward_netbox/tests/test_converged_models_are_measured.py` pins that a
zero-change workload is still falsy for the plan filter, that the captured map
retains a model the plan drops, that building from the plan loses it, and that
the capture is off by default with the preview as the only caller that opts in.

Full Django suite, because this changes the fetch path every sync runs.

## Rollback

Revert. Converged models return to reporting `Not measured` with an estimate of
every fetched row.

## Decision Log

- **Capture before the delta rather than un-drop after it.** The dropped
  workload has no rows left to measure; recovering them means going back to
  where they still existed. Changing the plan filter instead would stage empty
  items.
- **Opt-in rather than always-on.** The preview is a background job that can
  afford the rows; the sync is the path that must not regress. Defaulting to
  on would have made every sync pay for a measurement only the preview reads.
- **Do not infer zero drift from an empty delta.** It would have been a one-line
  change to report converged models as in sync without comparing anything, and
  it would have been the same defect this repository keeps finding.

## Open

- Cost at scale is unmeasured. Comparing roughly 530k rows on a converged
  estate is work the preview did not previously do, and while it is the same
  classification a sync performs, the wall-clock effect on a large deployment's
  preview has not been observed.
