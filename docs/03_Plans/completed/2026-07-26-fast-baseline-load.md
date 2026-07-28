# Fast Baseline Load

## Goal

Reduce the first, empty-destination Forward baseline from hours to a bounded
bulk-load window while preserving the final NetBox inventory and every durable
Forward state record required by later diff, prune, ownership, health, and
recovery behavior. Measure the path at an anonymized 1.17-million-change shape
and answer the achievable time and explicit safety cost with paired evidence.

## Constraints

- Work only in `/tmp/forward-netbox-merge` on `feat/fast-baseline-load` and
  preserve the dirty profiling/set-based starting tree.
- Do not write `/tmp/forward-netbox-publish-261` or
  `/tmp/forward-netbox-copysql`; do not mutate production, GitHub, releases,
  versions, commits, or tags.
- Keep `netbox_branching` as the only ongoing/incremental sync path. Any
  baseline exception is explicit, disabled by default, exact-version pinned,
  model allowlisted, and fail-closed before target mutation.
- Admit only a first full snapshot whose in-scope target and owned side tables
  are empty, with no baseline/current workload state, no prior successful
  ingestion, no competing branch, no deletes, and no runtime/schema hooks
  outside the proved contract. Recheck eligibility under a database lock.
- A failed preflight uses the current single-branch path. A fault after direct
  target mutation must roll back the whole baseline transaction; it must never
  fall back after a partially committed direct load.
- Preserve validation, model results, ingestion issues/statistics, workload
  states, contributor baselines, device identities, ownership pending records,
  catch-up state, and generation-guarded post-sync work wherever the current
  successful path produces them.
- Omit branch review artifacts only when they have no downstream baseline
  consumer: source branch rows, source ObjectChanges/ChangeDiffs,
  destination-per-row ObjectChanges, AppliedChanges, BranchEvents, and branch
  rollback. Record the selected baseline engine and aggregate counts durably so
  the omission is explicit and inspectable.
- Use a distinct Compose project. Run long tests and benchmarks detached with
  fsync'd logs, status, checkpoints, resource samples, and machine-readable
  results.

## Touched Surfaces

- Eligibility, direct-load orchestration, bulk model specifications, and
  transaction/lock handling in focused new utility modules.
- `single_branch_executor.py`, ingestion finalization/bookkeeping, and minimal
  persisted/configuration surfaces needed to opt in and attest the engine.
- Model-family apply helpers only where their existing normalization and
  dependency resolution can be reused without per-row signals/audit.
- Focused selection, rollback, paired-equivalence, lifecycle, and downstream
  baseline tests.
- Generated customer-shaped fixture, detached benchmark runner, PostgreSQL/WAL/RSS/
  statement/rate instrumentation, and isolated remedy experiments.
- Architecture, configuration/operations guidance, and a reference evidence
  report with machine-readable artifacts.

## Approach

1. Inventory the successful current-path database contract: target tables,
   NetBox side tables, plugin statistics/issues, workload/contributor state,
   identity/ownership rows, audit lineage, branch evidence, and cleanup state.
2. Define a versioned eligibility decision that checks explicit opt-in, full
   workload semantics, exact NetBox/Branching/plugin/runtime/schema tuple,
   allowlisted models and row contracts, empty in-scope tables, absence of
   prior/current baseline evidence, absence of deletes and competing branches,
   and unsupported signals/customizations. Repeat mutable checks under an
   advisory lock plus one atomic transaction immediately before loading.
3. Implement dependency-ordered, bounded set-based inserts for admitted model
   families. Resolve natural identities and FKs in temporary staging tables,
   validate one-to-one resolution and constraints before target DML, use COPY
   for staging, and insert targets/required side rows as sets. Do not emit
   per-row branch or destination audit artifacts.
4. Reuse the existing validation/fetch/normalization and durable pending-state
   producers. After target DML, produce aggregate statistics/issues and device
   identity candidates, then invoke the same locked workload/contributor/
   ownership/catch-up finalization contract through a branchless baseline-safe
   entrypoint. Finalization and target DML remain atomic where practical; any
   unavoidable post-commit work retains the existing generation guards and
   visible incomplete state.
5. Differentially run the same generated workload through current and fast
   paths in reset disposable databases. Compare canonical target/side-table
   fingerprints, statistics/issues/model results, durable workload and
   contributor payloads, identities/ownership state, ingestion/sync lifecycle,
   and explicit evidence-presence expectations. Exercise unsupported versions,
   nonempty tables, prior state, competing branches, deletes, hooks, ambiguity,
   and injected faults.
6. Build a deterministic anonymized fixture with approximately 535,777
   Interfaces, 277,915 MACAddresses, 82,572 InventoryItems, 70,230
   Vulnerabilities, 51,944 IPAddresses, 34,388 Prefixes, 23,083 Cables, 3,400
   Devices, and dependency rows bringing the logical total to about 1.17M.
7. Measure fast baseline wall time, SQL calls/change, rows/second, peak RSS,
   PostgreSQL WAL bytes, periodic throughput, and rate slope. Run a bounded
   current-path sample at multiple accumulated volumes and extrapolate only
   when a complete 1.17M run is impractical, labeling the method and observed
   nonlinear range.
8. Isolate index maintenance, FK checking, audit-table growth, WAL durability,
   autovacuum, batching, MPTT rebuild, and load-window settings. Change one
   factor per disposable run; report absolute and incremental contribution.

## Validation

- Selection/rollback unit tests and real PostgreSQL integration tests for every
  eligibility guard and admitted model specification.
- Paired current/fast baseline proof across target rows, required side tables,
  plugin evidence/state, lifecycle, downstream incremental diff/prune inputs,
  and intentionally absent branch/audit artifacts.
- Detached actual-shape benchmark with durable result/checkpoint/log/resource
  artifacts under a task-specific temporary directory and Compose project.
- Focused Django tests, `invoke harness-check`, `invoke harness-test`, lint, and
  the smallest relevant check/scenario/docs gates. No release gate.

## Rollback

Keep the feature disabled or remove its opt-in; selection then returns the
existing single-branch executor before target mutation. Because the load is
restricted to an empty destination and target writes are transactional, an
injected or unexpected failure leaves no partial direct baseline. After a
successful direct baseline, rollback is database restore/reseed rather than
branch rollback; this safety loss is explicit in the operator confirmation.

## Decision Log

- 2026-07-26: Preserve the existing uncommitted profiling/set-based work as the
  immutable starting state and make no commits.
- 2026-07-26: Treat per-row ObjectChange, ChangeDiff, and AppliedChange lineage
  as review/rollback evidence, not automatically required baseline state. Their
  omission is allowed only after downstream-consumer tracing and paired proof.
- 2026-07-26: Reject any design that merely makes the merge fast while retaining
  a multi-hour per-row staging phase as the claimed end-state; measure complete
  baseline wall time.
- 2026-07-26: Implement the exception as a disabled-by-default, exact-version,
  model- and row-contract allowlisted direct-to-main transaction. Preserve the
  ordinary branch path unchanged for every rejection and every later sync.
- 2026-07-26: Preserve durable ingestion, workload/contributor, identity,
  ownership, catch-up, issue, and statistics state. Omit only the one-time
  Branch/BranchEvent/ObjectChange/ChangeDiff/AppliedChange review and rollback
  lineage, and record that omission in the completed ingestion attestation.
- 2026-07-26: Paired independent-database comparison passed for target and
  owned-side-table fingerprints, logical totals, lifecycle, and issues. The
  only expected difference was 6,015 current-path destination ObjectChanges
  versus none for the fast baseline.
- 2026-07-26: The full requested shape completed in 341.136 seconds at 3,440.23
  customer-denominator changes/second, 0.045650 SQL statements/change,
  1,153.11 MiB peak RSS, and 1,462,253,440 WAL bytes. Periodic rates stayed
  flat within each dominant model family.
- 2026-07-26: One-factor load-window experiments found no defensible wall-time
  benefit from relaxed durability, deferred constraints, disabled autovacuum,
  or index rebuilds. Bounded set-based batching is the measured remedy.

## Completion Evidence

- Focused transactional integration tests: 4 passed in 14.214 seconds,
  including empty/prior-state eligibility, durable completion, unsupported row
  preflight, and injected-finalization atomic rollback.
- `invoke harness-check`: passed.
- `invoke harness-test`: 206 passed.
- Focused Ruff for the new engine, executor integration, benchmark, and tests:
  passed. The pre-existing broad lint findings in `forms.py`,
  `model_validation.py`, and `sync_facade.py` were left outside this bounded
  change rather than mechanically rewriting those dirty-tree modules.
- `git diff --check`: passed.
- `invoke docs`: passed. Django `manage.py check` passed in the isolated
  `fnb-fastbaseline-20260726` Compose project; the generic `invoke check`
  wrapper could not run because it targets the intentionally unused default
  Compose project.
- Paired and full-volume JSON evidence plus the operator-facing analysis are
  recorded in `docs/03_Plans/completed/evidence/fast-baseline-load/` and
  `docs/02_Reference/fast-baseline-load-evidence.md`.
