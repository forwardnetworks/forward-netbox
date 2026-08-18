# The preview reports what its measurement cost

## Goal

Replace an estimate nobody can make with a number the deployment reports.

## Why

`2.8.4` made the drift preview compare models it previously skipped. On a
converged estate that is the difference between doing nothing and classifying
every fetched row - roughly half a million of them on the deployment that
prompted the change.

The release shipped with that cost recorded as unknown, and it stayed unknown
because nobody here has an estate that size. Two things are worth separating:

**It cannot fail.** Every Forward job is enqueued through `job_queue.py` with
`job_timeout = max(effective_forward_job_timeout(), ...)`, and
`MINIMUM_FORWARD_JOB_TIMEOUT_SECONDS` is 7200 regardless of what the deployment
sets `RQ_DEFAULT_TIMEOUT` to. The whole 1M-row merge projection runs under
1800s. The comparison would have to be four times the cost of a full merge to
threaten the job.

**It might still be slow.** A preview that takes several minutes where it used
to take seconds is a worse experience even when it completes, and the operator
has no way to tell a slow comparison from a hung one.

Asking someone to time it by hand is the wrong instrument. The preview already
runs the work; it can time itself.

## Constraints

- A payload written before this existed must report `None`, not `0`. A zero
  reads as "instant" for a preview that never measured, which is the same class
  of false confidence this feature has already produced twice.
- Only compared models contribute. Time spent on a model with no comparison is
  not the cost of a measurement, and rows handed to one were never read, so
  including either makes the per-row figure look better than it is.
- Name the slowest model, not just a total. One slow model and thirty even ones
  produce the same total and call for different responses.

## Touched Surfaces

- `forward_netbox/views.py` - per-model timing, totals in `comparison_coverage`
- `forward_netbox/utilities/drift_report.py` - `comparison_runtime_ms`,
  `comparison_rows_compared`, `slowest_compared_model`
- `forward_netbox/templates/forward_netbox/forwardsync_drift_report.html`
- `forward_netbox/tests/test_preview_reports_its_cost.py` (new)

## Approach

Each `compare_model_rows` call is timed. The per-model figure rides on the model
result so a slow model can be named; the totals ride in `comparison_coverage`
beside the measured/total counts that describe the same run.

The report renders the cost only when the payload carries it, so an older
preview shows nothing rather than a misleading zero.

## Validation

`forward_netbox/tests/test_preview_reports_its_cost.py`: the cost round-trips
from the payload, a payload without it reports `None` rather than zero, the
slowest compared model is named, an uncompared model is never named as slowest
even when it carries the largest time, and no timings at all reports `None`.

Full Django suite, because this touches the dependency preview path.

## Rollback

Revert. The cost returns to being unmeasured, and the next question about it
gets another estimate.

## Decision Log

- **Instrument rather than benchmark locally.** A synthetic 500k-row estate
  would measure this hardware, not the deployment's. The number that settles
  the question comes from the fabric that has the scale.
- **`None`, never `0`, for a payload that did not report.** Same reason the
  empty-row shortcut was removed: a confident number for something never
  measured is worse than an absent one.
- **Exclude uncompared models from both the time and the row count.** Otherwise
  the cost-per-row improves precisely as coverage gets worse.

## Open

- The number itself. This makes it observable; it does not make it good.
