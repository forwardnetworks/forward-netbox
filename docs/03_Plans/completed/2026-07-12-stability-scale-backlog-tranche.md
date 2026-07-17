# Stability + Scale Backlog Tranche (post-2.5.7, release TBD)

## Goal

Clear the standing stability/speed and automation-parity backlog in one
branch (`feat/backlog-tranche`, off the held CVE-tab work), gated by a
10-agent read-only recon that pinned exact code sites and feasibility. NOT
released — held with the CVE tab pending design-partner feedback.

## Constraints

- No release, no push until feedback lands (standing directive).
- Stability is paramount: every behavioral change is default-off or
  default-identical; the risky plan/executor changes are opt-in.
- No NQE query changes anywhere in the tranche (no ADP republish needed).

## Touched Surfaces

Eight implementations + four documented closures. Per item:

1. **PATCH-intent immediacy + transactional persist** — `api/views.py`
   (perform_update/perform_create hooks, intent-key-only), `sync_facade.py`
   (`standing_schedule_intent`, `select_for_update` persist), `sync_state.py`
   (display echo of intent keys).
2. **Button JobRunner parity** — `jobs.py` shared work fns + shims +
   PruneOrphansJob/TagDeleteEligibleIpamJob/CreateModuleBaysJob (fixed
   Meta.names == BUTTON_JOB_SPECS suffixes; instance-scoped guard).
3. **Sync self-reschedule pinning suite** — `tests/test_sync_recurrence_
   pinning.py`; recon verdict KEEP-AS-IS (core JobRunner cannot replicate the
   completion-time anchor / model-field source of truth / name-scoped guard).
4. **Event-queue hygiene** — `sync_events.py` (snapshot/restore),
   `sync_reporting.py` + `apply_engine_bulk.py` per-row loops drop a failed
   isolated row's events; threshold increment moved inside the row atomic.
5. **Bulk per-row isolation + `__in` chunking** — `apply_engine_bulk.py`:
   tree-model + virtualchassis row isolation; all 19 unbounded lookups
   chunked at 500 (device+interface pair-chunked; net_host OR chunked);
   tree-model prefetch kept single-query (test-pinned, lowest volume).
6. **Stuck-run recovery** — `utilities/stuck_recovery.py` +
   `management/commands/forward_stuck_job_recover.py`; classify + advisory-
   locked recover of MERGING/SYNCING syncs wedged by a dead worker.
7. **Per-workload wall-clock fetch budget** — `exceptions.py`,
   `query_fetch_execution.py`, `forward_api_impl.py`; opt-in
   `workload_fetch_timeout_seconds` cooperative deadline + circuit breaker.
8. **Shard-key bucket-packing** — `branch_budget.py` (`split_workload`,
   `build_branch_plan` optional kwarg), `single_branch_executor.py` +
   `views.py` opt-in gate + always-warn, `multi_branch_lifecycle.py`
   stats-reset fix.

Allowlists/validators for the new sync/source params in `model_validation.py`.

## Approach

Recon workflow (10 agents) → implement smallest-blast-radius first, each
item gated + committed separately; the mechanical batches (5, 7, 8)
delegated to codex with the recon sketch and verified locally (codex's
sandbox can't reach docker). Every item's behavior is identical unless an
operator opts in via a source/sync parameter.

## Validation

Per-item targeted suites green; final full plugin suite + pre-commit +
harness + sensitive gate before the tranche is considered done. Key
regression pins held: build_branch_plan single-item default, tree-model
single-query prefetch, sync-reschedule semantics, button name couplings.

## Rollback

Revert the per-item commits (independent). No migrations. Every new
parameter is inert when unset; reverting leaves stored params ignored.

## Decision Log — closures (recon-confirmed, NOT implemented)

- **resume-from-persisted-plan rearchitecture: OBSOLETE.** Every named target
  (`multi_branch_executor._load_execution_context`,
  `resumable_branching.get_plan_items`, `_persisted_plan_item`) was deleted
  in the 2.0 single-branch rewrite (commit fb75ea9). The 163-shard/163-replan
  crash it addressed was eliminated structurally, not left open. Nothing to
  build.
- **reopen-COMPLETED-run step reset: OBSOLETE.** The target
  `execution_ledger_reconciliation.py` and the ForwardExecutionRun/Step
  models were dropped (migration 0028); the reopen path no longer exists.
- **NQE query consolidation (49 -> ~42): DO-NOT-DO** (prior investigation,
  2026-06-25). Family merges (ipv4/ipv6) can't dedup — NQE can't parameterize
  field-access paths; variant merges would risk the three hottest
  org-published queries for marginal tidiness. Keep 49.
- **sync-loop -> core JobRunner convergence: KEEP-AS-IS**, locked by the
  item-3 pinning suite (deletes ~65 lines but would require rebuilding the
  intent/guard/reconcile apparatus and loses the completion-time cadence).
- **ACI "1 created + 1 deleted every sync" churn: BLOCKED on field data.**
  Needs the created/deleted object from a sync's ChangeDiff BEFORE merge;
  `forward_apply_identity_audit` (read-only, shipped 2.2.1) is the tool for
  the affected operator to name it. Not reproducible without that network.

## Bundled changes

Stability + scale hardening + automation parity, all opt-in or
default-identical: immediate PATCH-intent reconcile; JobRunner parity for the
three remaining button jobs; per-row isolation + chunked lookups + event-
queue hygiene in the bulk apply path; stuck-worker recovery command; opt-in
per-workload fetch budget; opt-in shard-key bucket-packing with an
always-on oversized-workload warning; plus a latent stats-reset bug fix.
Python-only; no query change.
