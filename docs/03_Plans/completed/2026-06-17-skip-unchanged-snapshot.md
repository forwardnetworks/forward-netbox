# Skip Scheduled Runs on an Unchanged Snapshot

## Goal

Cut Forward API load for scheduled syncs: when a scheduled run would target the
same snapshot as the last successful baseline, skip query execution entirely
(no-op) instead of re-fetching unchanged data for every model.

## Constraints

- Opt-in per sync (`skip_unchanged_snapshot` parameter, default off) so existing
  syncs see zero behavior change.
- Manual/adhoc runs must always execute (operators can force a re-sync).
- Must short-circuit at the orchestration boundary, before any execution run,
  branch, or ingestion is created — so it never leaves the execution-ledger
  state machine inconsistent.
- Completion state (sync status, source status, last_synced, job data) must match
  a normal clean completion.

## Touched Surfaces

- `forward_netbox/utilities/sync_orchestration.py` — `should_skip_unchanged_snapshot`
  helper + no-op short-circuit in `run_forward_sync` (new `adhoc` param) that
  marks COMPLETED and runs `_finalize_forward_sync`.
- `forward_netbox/models.py`, `forward_netbox/jobs.py` — thread `adhoc` from the
  job into `sync.sync` → `run_forward_sync`.
- `forward_netbox/utilities/model_validation.py` — allow + normalize the new key.
- `forward_netbox/forms.py` — opt-in toggle in the Execution fieldset.
- Tests in `test_sync_orchestration.py`; docs in `configuration.md`.

## Approach

`should_skip_unchanged_snapshot` returns the resolved snapshot id when: not
adhoc, the opt-in flag is set, an eligible baseline ingestion exists, and the
sync's resolved snapshot equals the baseline snapshot. Any resolution error
returns None (run normally). `run_forward_sync` checks it after the in-progress
guards and before `_prepare_forward_sync`; on a hit it logs a no-op success,
sets COMPLETED, finalizes, and returns. The scheduler `finally` block still
reschedules the interval job, so the schedule stays alive.

## Validation

- `invoke test --test-label forward_netbox.tests.test_sync_orchestration`
  (helper matrix: matching baseline / adhoc / flag off / no baseline / advanced /
  resolution failure; plus run_forward_sync no-op path skips the executor).
- Regression: `test_forms`, `test_models`, `test_sync`, `test_ingestion_merge`.
- `invoke lint`, `invoke harness-check`.

## Rollback

Revert the listed modules. The flag is off by default, so reverting only removes
an opt-in capability; no data or migration impact.

## Decision Log

- Opt-in (default off) chosen over default-on (the original roadmap framing) to
  protect production syncs already running on this release line; can be promoted
  to default later once proven.
- Short-circuit at `run_forward_sync` (not inside the executor) so no execution
  run/branch/ingestion is created for a no-op — avoids ledger-state risk.
- Drift caveat: if NetBox data changed out-of-band on an unchanged snapshot, a
  skipped run will not re-apply; operators force a re-sync via a manual run.
