# Bidirectional Drift Report

## Goal

Give operators a read-only per-model "NetBox vs Forward" drift view — how far each
model has diverged from Forward ground truth — without applying changes.

## Constraints

- No new heavy dry-run: reuse the dependency-preview job's cached payload.
- Read-only; no live Forward call on render.
- No customer data in repo/tests.

## Touched Surfaces

- `forward_netbox/utilities/drift_report.py` — `compute_drift_report(payload)`
  distilling the dependency dry-run's per-model results into a drift table.
- `forward_netbox/views.py` — `ForwardSyncDriftReportView` (GET) renders the
  latest completed dependency-preview job's cached payload as drift; sync-detail
  Drift Report button URL.
- `forward_netbox/templates/forward_netbox/forwardsync_drift_report.html` and the
  Drift Report button on `forwardsync.html`.
- Tests in `test_health`.

## Approach

The dependency dry-run already computes, per model, `estimated_changes` (rows
Forward would create/update in NetBox) and `delete_count` (NetBox rows Forward no
longer has). That is exactly the bidirectional drift. `compute_drift_report`
reshapes those results into a sorted table (model, forward_rows, pending_changes,
pending_removes, drift, in_sync) with summary counts. The drift view reads the
latest completed dependency-preview job's cached payload (same source as View Last
Preview) — so no second heavy run — and renders the table. If no preview has run,
it points the operator to Preview Dependencies.

## Validation

- `forward_netbox.tests.test_health` — `compute_drift_report` summary math and
  the drift view rendering from a cached preview job.
- Full suite; local CI mirror.

## Rollback

Drop the utility, view, template, and button. No data/schema impact.

## Decision Log

- Reuse the dependency-preview payload instead of a dedicated drift dry-run: the
  data is identical, so a second job would double the cost; drift is a different
  presentation of the same dry-run, not new computation.
- Per-model granularity (not per-row): keeps the report cheap and legible;
  per-row divergence is available via the existing change/branch review.
