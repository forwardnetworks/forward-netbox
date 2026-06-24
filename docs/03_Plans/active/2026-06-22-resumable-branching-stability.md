# Resumable-Branching Stability Hardening (2026-06-22)

## Goal

Stop the resumable multi-branch sync from crashing or corrupting NetBox on
transient failures and at scale, after Blake's v1.7.2 sync hard-crashed at shard
6/163 ("Unable to resolve execution shard for claimed index 7"). Stability is
paramount: a sync that crashes loudly is safer than one that half-merges or
double-applies silently.

## Constraints

- No customer identifiers / network IDs / raw rows in the diff, tests, or docs.
- Changes touch the highest-blast-radius code (executor, merge, jobs, ledger);
  every change must keep the full suite green and preserve resume/merge
  idempotency.
- Behavioural changes are additive or strictly safer defaults; no new migration.

## Bundled changes

1. **Atomic post-merge ledger bookkeeping** — `sync_merge_ingestion` now commits
   the baseline-ready flag, plan-item state, and ledger step-merged mark in one
   `transaction.atomic()` so a crash can't leave the ledger half-marked and
   double-apply a merged shard on resume.
2. **Bounded stage-job retries (no infinite requeue)** — `STAGE_DB_RETRY_LIMIT`
   caps transient DB-connection-pressure retries (was unbounded → infinite
   requeue on a large fabric); after the cap the run fails for operator review.
3. **Graceful degrade on resume desync** — `ForwardShardResolutionError`
   (subclass of core `SyncError`) replaces the hard crash when a resumed shard's
   claimed index can't be resolved; the stage runner retries it up to
   `STAGE_SHARD_RESOLUTION_RETRY_LIMIT` (re-running the plan build, which usually
   resolves once the transient condition clears) before failing cleanly.
4. **Transient workload-fetch retry** — `_run_workload_job` retries a transient
   NQE fetch (connectivity / timeout / 429 / 5xx; never a `ForwardQueryError`)
   so a transient query blip no longer fails a shard and truncates the rebuilt
   plan. Tunable via `workload_fetch_retry_attempts` / `_backoff_seconds`.
5. **`next_step_index` monotonic clamp** — `update_run_from_branch_state` clamps
   with `max()` so stale branch-run state can't regress the index below the
   merged floor and re-execute an already-merged shard.
6. **Locked enqueue** — `enqueue_branch_stage_job` runs its in-flight guard +
   enqueue + run update under `select_for_update` so concurrent enqueues can't
   double-queue the same shard.
7. **Orphaned-branch cleanup** — a non-budget apply-phase exception now
   detaches + deletes its branch (best-effort) instead of leaving an orphaned,
   provisioned branch behind.
8. **Startup dependency/version guard** — `AppConfig.ready()` logs (never
   raises) a warning when `netbox_branching` is missing and records resolved
   versions, so dependency drift surfaces at startup not mid-sync.
9. **fetch_all in-memory row ceiling** — `run_nqe_query(fetch_all=True)` aborts
   with an actionable error once accumulated rows exceed
   `nqe_fetch_all_max_rows` (default 2M); the page-count ceiling alone permitted
   ~50M rows before firing, enough to OOM the worker on a giant unsharded result.
10. **Per-row bulk-apply isolation** — when a `bulk_create`/`bulk_update` hits a
    DB constraint error (rolling the whole batch back), the bulk apply retries
    the batch row-by-row so good rows apply and the offending row(s) become
    ingestion issues — one bad row no longer fails the whole shard. Applied to
    the simple-models path and the three highest-volume models
    (`dcim.macaddress`, `dcim.interface`, `ipam.ipaddress`) via the shared
    `_isolate_bulk_objects` helper. (Tree models can adopt it next.)
11. **Terminal step status surfaces over STAGED** — `_run_status_from_steps`
    now resolves a run to TIMEOUT/FAILED before WAITING, so a timed-out or
    failed merge whose later shard was pre-staged no longer leaves the run in a
    WAITING-vs-FAILED limbo that no recovery loop requeues (the merge-timeout
    wedge).

## Touched Surfaces

- `forward_netbox/__init__.py` — startup dependency/version guard.
- `forward_netbox/exceptions.py` — `ForwardShardResolutionError`.
- `forward_netbox/jobs.py` — retry ceilings + graceful shard-resolution retry.
- `forward_netbox/utilities/ingestion_merge.py` — atomic merge bookkeeping.
- `forward_netbox/utilities/execution_ledger_run_store.py` — next_step_index clamp.
- `forward_netbox/utilities/multi_branch_executor.py` — raise the new exception.
- `forward_netbox/utilities/multi_branch_lifecycle.py` — apply-failure branch cleanup.
- `forward_netbox/utilities/query_fetch_execution.py` — transient fetch retry.
- `forward_netbox/utilities/resumable_branching.py` — locked enqueue.
- `forward_netbox/utilities/forward_api_impl.py` — fetch_all row ceiling.
- `forward_netbox/utilities/apply_engine_bulk.py` — per-row isolation fallback.
- `forward_netbox/tests/test_stability_hardening.py`, `test_apply_engine.py`,
  `test_forward_api.py` — new regression tests.

## Approach

Lock in the crash + data-corruption-bookkeeping + recovery-retry cluster first
(highest stability value, well-scoped). Each change is additive or a strictly
safer default, validated against the full suite.

## Validation

- Full plugin suite: 1191 tests, 0 failures, 26 skipped (running container).
- New `test_stability_hardening` (8 tests): transient classifier, fetch
  retry/permanent/exhaust behaviour, shard-resolution error contract.
- `pre-commit` (reorder/black/flake8) clean.

## Rollback

Revert this commit. Each change is independent; the new exception falls back to
`SyncError` handling, the retry ceilings only bound existing retries, and the
atomic block / clamp / lock only tighten existing behaviour.

## Decision Log

- **Cluster scope** — the merge *atomic boundary* (per-change merge), the
  intricate state-machine races (reopen-reset, merge-timeout non-terminal
  strand), and the scale/speed items (chunk `__in`, bound `fetch_all`, per-row
  isolation, ORM N+1) are deferred to a focused follow-up after re-analysis;
  rushing them risks the stability this change protects. Tracked in the
  backlog memory `stability-speed-roadmap.md`.
- **Shared retry counter** — DB-pressure and shard-resolution retries share the
  per-shard `retry_count` with separate ceilings; both are "retry this shard".
