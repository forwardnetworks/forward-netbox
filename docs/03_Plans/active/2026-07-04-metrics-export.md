# Prometheus metrics export

**Date:** 2026-07-04

## Goal
Expose Forward sync/job health as Prometheus metrics so an enterprise Grafana/
Datadog stack can graph and alert on it (assessment: rich telemetry was trapped in
`job.data` / the health page).

## Constraints
- Read-only; cheap DB queries only; no new runtime dependency.
- Standard Prometheus text-exposition format.

## Touched Surfaces
- `forward_netbox/management/commands/forward_metrics.py` — emits
  `forward_sources_total`, `forward_syncs_total`, `forward_ingestions_total`,
  `forward_jobs{status=...}`, `forward_stuck_jobs`, and
  `forward_last_completed_job_{timestamp,age}_seconds`.
- `forward_netbox/tests/test_jobs.py` — asserts the exposition shape.
- `docs/01_User_Guide/operations.md` — documents wiring it to a textfile collector
  or scrape sidecar.

## Approach
Derive status/time metrics from the reliable core `Job` fields (filtered to the
plugin's content types) plus plugin model counts; reuse `job_has_live_execution`
for the wedged-job gauge. A command (not an HTTP endpoint) keeps it dependency-free
and consumable by a node_exporter textfile collector or a scrape sidecar.

## Validation
Command test asserts the exposition format; full suite green; lint/harness.

## Rollback
Revert; additive command + test + docs only.

## Decision Log
- A management command over a `/metrics` HTTP view: no new URL/auth surface, and it
  composes with existing scheduling; a scrape endpoint can wrap it later.

## Bundled changes
- `forward_metrics` Prometheus exporter command + docs.
