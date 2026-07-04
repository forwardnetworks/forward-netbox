# Stuck-job alert command

**Date:** 2026-07-04

## Goal
Close the observability gap flagged by the assessment: a stuck-job DETECTOR exists
(`job_liveness.job_has_live_execution`, 180s heartbeat) but there was no autonomous
way to be alerted to a wedged sync — equivalent to `forward_collection_gap_alert`
but for jobs.

## Constraints
- Read-only detector; no change to job execution.
- Reuse the existing liveness predicate.

## Touched Surfaces
- `forward_netbox/management/commands/forward_stuck_job_alert.py` — scans
  forward_netbox `Job` rows in PENDING/RUNNING whose `job_has_live_execution` is
  False (DB-active but no live RQ execution / stale heartbeat), reports JSON,
  warns, and exits non-zero with `--fail-on-stuck`.
- `forward_netbox/tests/test_jobs.py` — flags a dead active job; ignores a live one.
- `docs/01_User_Guide/operations.md` — documents scheduling it.

## Approach
Mirror `forward_collection_gap_alert`'s command shape; filter Jobs by the
`forward_netbox` app's content types and the active statuses, then apply the
existing liveness predicate.

## Validation
Full suite 947 green; two command tests; lint/harness.

## Rollback
Revert; additive command + tests + docs only.

## Decision Log
- Reuse `job_has_live_execution` (conservative — returns live when RQ can't be
  inspected) so the alert does not false-positive when RQ introspection is
  unavailable.

## Bundled changes
- `forward_stuck_job_alert` management command + tests + operations docs.
