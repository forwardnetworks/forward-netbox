# Scale Hardening Architecture

## Goal

Move the large-sync architecture from "resumable enough to survive normal
timeouts" to a durable, inspectable, and recoverable execution system for very
large NetBox inventories.

The operator-visible outcome should be:

- Branching baselines remain native NetBox Branching workflows.
- Trusted first loads still have the faster `Fast bootstrap` path.
- Every long-running sync has a run-level support bundle with enough state to
  debug customer failures without asking for screenshots one shard at a time.
- A worker timeout, process restart, duplicate callback, or stale job does not
  force the operator to restart a multi-day baseline.
- Future bulk engines can accelerate apply operations without changing NQE row
  contracts or inventing a second sync workflow.

## Constraints

- Keep NQE as the only normalization and model-shaping layer.
- Keep mutation through native NetBox models and native Branching branches.
- Keep branch budgets bounded by operator guidance; do not hide scale issues by
  raising `Max changes per branch`.
- Keep fast bootstrap explicit because it trades away Branching diff review.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or
  private inventory rows.
- Keep release validation aligned with `invoke ci` and GitHub CI.
- Avoid new dependencies unless a later implementation plan proves they are
  required.

## Touched Surfaces

Likely production surfaces:

- `forward_netbox/models.py`
- `forward_netbox/migrations/`
- `forward_netbox/jobs.py`
- `forward_netbox/views.py`
- `forward_netbox/forms.py`
- `forward_netbox/tables.py`
- `forward_netbox/api/`
- `forward_netbox/utilities/resumable_branching.py`
- `forward_netbox/utilities/multi_branch_executor.py`
- `forward_netbox/utilities/multi_branch_lifecycle.py`
- `forward_netbox/utilities/ingestion_merge.py`
- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/utilities/sync_state.py`
- `forward_netbox/tests/`
- `scripts/playwright_forward_ui.mjs`
- `docs/01_User_Guide/`
- `docs/02_Reference/`
- `docs/00_Project_Knowledge/`

## Approach

### Current Implementation Status

The first implementation tranche has moved Branching orchestration onto
first-class execution records while keeping the existing sync-parameter state as
a compatibility cache:

- `ForwardExecutionRun` records the sync, source, backend, snapshot, validation
  run, branch budget, plan preview, heartbeat, and final baseline decision.
- `ForwardExecutionStep` records one durable shard step per planned Branching
  item, including query identity, model, shard keys, estimated/actual changes,
  job IDs, merge job IDs, branch/ingestion linkage, retry count, heartbeat, and
  last error.
- Branching coordinator jobs now create/update the ledger before queueing shard
  jobs.
- Stage jobs claim the current ledger step before work begins and skip duplicate
  jobs when the step is already terminal.
- Existing `Continue Ingestion`, merge requeue, and sync-summary behavior still
  use the compatibility state, but ledger updates mirror those transitions.
- Ingestion log export now includes the latest execution run bundle with step
  summaries and linked job details.

Remaining architecture work is tracked below: shard-scoped fetch, richer
stale-job reconciliation actions, UI/API surfaces for execution runs, and future
bulk apply engines.

### Tranche Update (2026-05-23)

This tranche closed several high-risk stability gaps in ledger-first orchestration
and recovery evidence:

- Stage claim/dispatch now derives run state from active execution-ledger records
  before consulting compatibility JSON, preventing stale `_branch_run` payloads
  from selecting the wrong shard index.
- Planner, runtime phase updates, and resumable plan-item helpers now consume
  display-state synthesis (ledger first, compatibility only when no ledger
  history exists), so stale compatibility payloads cannot steer new runs once
  execution-run history exists.
- Execution-run creation now ignores stale compatibility `execution_run_id`
  values that reference terminal runs, preventing completed historical runs
  from being resurrected as active orchestration state.
- Execution-run creation is now transaction-guarded with a sync row lock and
  in-lock active-run recheck, preventing duplicate active runs when multiple
  workers race to initialize the same Branching execution.
- Legacy compatibility pending-state behavior is preserved only for true
  pre-ledger syncs (no execution-run history), so upgrade/read-through
  scenarios continue to resume without reintroducing stale-state orchestration.
- Planner input now treats an explicit empty branch-run state as authoritative
  and does not silently fall back to persisted compatibility state.
- Merge eligibility now checks mergeable execution-step evidence first, then
  compatibility state as fallback.
- Query context resolution now prefers ingestion linkage from the active
  execution run/step chain before compatibility state.
- Chaos kill validation now verifies run-bundle structure and scenario-aligned
  recovery evidence (action type plus expected step state) before reporting
  pass.
- Live sync state now renders from active execution runs only. When only
  terminal run history exists, stale compatibility `_branch_run` payloads are
  suppressed from `branch_run` display/activity and merge/pending state checks.
- Stage execution now explicitly no-ops if no active execution run is claimable
  and ledger history exists, preventing stale queue replay.
- Stage enqueue and merge follow-on enqueue now avoid compatibility-only
  continuation once ledger history exists; resume fallback is allowed only for
  failed/timeout runs.

Validation evidence for this tranche:

- `python -m unittest discover -s scripts/tests -p 'test_*.py'`
- `invoke check`
- `invoke scenario-test`
- `invoke harness-test`
- `invoke test`
- `invoke ci`

### Remaining Control-Plane Audit (2026-05-23)

Follow-up audit after the ledger race/stale-state fixes confirms the active
Branching control plane is now ledger-first:

- Live runtime display, pending/awaiting checks, and progress/failure mutation
  paths now prefer active `ForwardExecutionRun` / `ForwardExecutionStep` state.
- Compatibility `_branch_run` writes are now suppressed once execution-run
  history exists, preserving compatibility JSON as read-through upgrade data
  instead of mutable active-runtime state.
- Stage-worker dispatch refuses compatibility-only continuation when execution
  run history exists but no claimable active run is present.
- Execution-run initialization is guarded by sync-row locking and ignores stale
  compatibility `execution_run_id` pointers to terminal runs.
- Ingestion merge queue fallback now uses synthesized display state (ledger
  first, compatibility only for pre-ledger syncs), so stale compatibility
  `pending_ingestion_id` values cannot re-open merge actions.
- Runtime no-op paths that skip stage execution/enqueue because no active run
  is claimable now prune stale compatibility `_branch_run` payloads, reducing
  lingering ambiguous runtime state after terminal ledger runs.
- Execution-run initialization also prunes stale compatibility `_branch_run`
  payloads when only terminal ledger history exists before starting a new run.
- Sync Health now reports latest execution-step query runtime/pushdown profiling
  (fetch-mode counts, fallback-step counts/reasons, and slowest models) so
  query-pushdown gaps are visible directly in native NetBox operator surfaces.
- Sync Health now reports compatibility-cache retirement status
  (ledger-history presence, stale `_branch_run` payload detection, and prune
  recommendation), making lingering legacy compatibility JSON visible from the
  native NetBox health surface.
- Added a native maintenance command (`forward_prune_compatibility_cache`) and
  invoke wrapper (`invoke prune-compat-cache`) to prune stale compatibility
  `_branch_run` payloads once ledger history exists and no run is active,
  with dry-run and JSON evidence output for release artifacts.
- Execution-run support bundle exports now include compatibility-cache evidence
  (legacy payload presence, active-run linkage, stale-payload detection, and
  prune recommendation), so exported artifacts directly prove retirement status
  without requiring separate health-page snapshots.
- Ledger display-state synthesis now prunes stale compatibility `_branch_run`
  payloads during read paths when only terminal run history exists, making
  compatibility-state retirement progress visible in normal UI/API flow.

Remaining compatibility behavior is intentionally constrained to pre-ledger
upgrade/read-through and does not steer active runs when ledger history exists.
Deferred long-term work remains:

- full retirement of compatibility `_branch_run` writes after the compatibility
  window
- deeper live query-pushdown proof from Forward runtime evidence
- broader apply-engine acceleration beyond the parity-safe model set
- destructive worker-kill chaos harnessing as an opt-in gate

### 1. Promote Branch State To An Execution Ledger

The current resumable Branching implementation stores plan state in
`ForwardSync.parameters`. That was the right bridge because it required no
schema migration and kept the workflow inside the existing sync object. It is
not the ideal long-term scale boundary.

Add first-class records:

- `ForwardExecutionRun`
  - sync
  - source
  - backend
  - snapshot selector
  - resolved snapshot ID
  - validation run
  - branch budget
  - status
  - phase
  - latest heartbeat
  - final baseline-ready decision
- `ForwardExecutionStep`
  - run
  - index
  - kind: coordinator, stage, merge, finalize
  - model string
  - query name
  - execution mode
  - execution value
  - commit ID
  - baseline snapshot ID
  - estimated changes
  - actual changes
  - status
  - branch
  - ingestion
  - job
  - retry count
  - last error
  - heartbeat

Keep the existing JSON branch-run state as a compatibility/read-through cache
while the ledger is introduced. New orchestration should write the ledger first
and derive display state from it.

### 2. Make Step Execution Idempotent

Each job should claim exactly one execution step before doing work.

Rules:

- Claim with a database transaction and row lock.
- If the step is already complete, exit successfully without side effects.
- If the step is already running under a live job, do not start a duplicate.
- If the previous job is stale or failed, increment retry count and claim it.
- Store stage and merge job IDs on the step before enqueueing dependent jobs.
- Enqueue the next step only after the current step reaches a terminal success
  state.

This avoids duplicate stage jobs, duplicate merge jobs, and lost parameter
updates when multiple NetBox workers are available.

### 3. Make Shard Fetch Truly Shard-Scoped

The resumable Branching implementation currently narrows retry planning to the
current model, then selects the persisted shard from a rebuilt model workload.
That is correct enough for recovery, but it still makes a model with many
shards pay repeated full-model NQE/diff cost.

Move toward shard-scoped fetch:

- Persist the shard predicate or bucket identity on each execution step.
- Add optional built-in NQE parameters for shard keys or hash buckets where the
  model can be filtered deterministically.
- Keep the NQE output shape identical to the unfiltered query.
- Record whether a step used shard-scoped fetch, model-scoped fallback, or full
  fallback.
- Keep full fallback available for models where safe deterministic filtering is
  not possible.

This is the biggest remaining performance item for Branching after the
resumable job-chain work. It reduces repeated Forward query work without
changing the NetBox mutation contract.

### 4. Add Stale-Job Recovery

Handled `JobTimeoutException` is only one failure mode. A worker can also be
restarted, killed, evicted, or disconnected before the Python exception handler
updates plugin state.

Add a native recovery action:

- `Reconcile Run`
- `Retry Current Step`
- `Requeue Merge`
- `Discard Failed Branch And Retry`

The reconciliation step should compare:

- execution step status
- NetBox job status
- job heartbeat or completion timestamp
- Branching branch status
- linked ingestion status

If a stage job is stale with no completed ingestion, mark the step retryable. If
a branch exists and has unmerged changes, keep it visible and require either
merge requeue or explicit discard before retrying.

### 5. Build A Run-Level Support Bundle

The current ingestion log export is useful but too narrow for multi-shard
baselines. Add sync/run-level export that includes:

- sync/source/backend/snapshot metadata
- validation result and drift policy summary
- all execution steps
- all linked ingestions
- all stage and merge job data/log entries
- branch names/statuses/change counts
- model results, query modes, query references, row counts, and runtimes
- sanitized ingestion issues
- current recovery recommendation

Do not include raw NQE rows. Do include enough query reference and model-result
metadata to identify whether a customer is using raw bundled query text,
repository paths, direct query IDs, or stale published queries.

### 6. Keep The Two-Lane Execution Model

Branching and fast bootstrap should not be blurred.

Branching:

- default reviewable path
- native branches and `ChangeDiff`
- resumable step chain
- bounded branch budget
- slower by design because it creates review artifacts

Fast bootstrap:

- explicit trusted baseline path
- no branch diff review
- same validation and adapters
- direct NetBox writes with native change tracking
- establishes baseline for later snapshot diffs when maps are eligible

The UI should make the tradeoff clear before the run starts and should show
whether the current run is a full baseline, hybrid/full fallback, or real
Forward `nqe-diffs` run.

### 7. Prepare For Future Bulk Engines

Future TurboBulk/parquet support should be an apply engine selected by capability
and model, not a separate product workflow.

Rules:

- Input remains NetBox-shaped rows from NQE.
- Conflict identity remains the same coalesce contract used by current adapters.
- Branching mode still creates and merges native branches.
- Direct mode still records native NetBox object changes and ingestion issues.
- Unsupported models fall back to current adapter execution.
- The support bundle must state which apply engine handled each model/step.
- Faster engines must prove parity with the adapter path before activation:
  validation, `save()`/signal behavior, object change tracking, Branching diff
  visibility, dependency skip behavior, row counters, and issue capture.

This keeps the architecture open to higher throughput without rewriting query
contracts or forcing users into a separate operational path.

### 8. Add Chaos And Scale Gates

The current CI gate proves unit, scenario, UI, docs, and packaging behavior. The
large-sync architecture also needs destructive local tests that prove recovery:

- kill a stage worker after the branch is created
- kill a stage worker during row application
- kill a merge worker after merge starts
- retry the same stage job twice
- retry the same merge job twice
- run with multiple workers and confirm only one worker claims each step
- force branch-budget overflow and confirm the split is persisted in the ledger
- retry a late shard and confirm it does not refetch a full model when
  shard-scoped fetch is supported
- export the run-level support bundle after each failure mode

These tests can stay outside normal public CI if they require Docker state or
longer runtime, but they should become a release checklist for large-sync
changes.

## Validation

Documentation-only pass:

```bash
invoke harness-check
invoke harness-test
invoke docs
```

Implementation tranches should add:

```bash
invoke lint
invoke check
invoke scenario-test
invoke test
invoke playwright-test
invoke ci
```

Large-sync implementation should also include local Docker recovery tests:

- stage timeout recovery
- stage hard-kill recovery
- merge timeout recovery
- duplicate enqueue prevention
- support bundle completeness

## Rollback

Keep the current parameter-backed resumable Branching state until the ledger is
proven. Rollback for the first implementation tranche should be:

- stop writing new execution ledger rows
- derive UI state from existing sync parameters
- keep old `Continue Ingestion` and ingestion merge actions
- leave ledger rows read-only for postmortem/debugging until a cleanup migration
  is safe

## Decision Log

- Chosen: first-class execution ledger as the target because long-running
  multi-shard baselines need atomic step ownership and complete support export.
- Chosen: shard-scoped fetch as the main Branching performance target because
  resumability alone does not remove repeated model-query cost.
- Chosen: keep fast bootstrap separate because it solves a different problem:
  establishing a trusted baseline quickly, not reviewing a branch diff.
- Chosen: keep future bulk engines below the execution backend because NQE and
  NetBox-native workflow remain the product contract.
- Rejected: relying indefinitely on JSON parameters for orchestration because
  concurrent workers and stale jobs can create ambiguous recovery states.
- Rejected: making every large import Branching-only because native `ChangeDiff`
  generation is valuable but expensive, and trusted baselines need a pragmatic
  first-load path.
- Rejected: parallel shard merging as an immediate step because dependency order
  and Branching merge semantics are more important than theoretical throughput.
- Rejected: storing raw rows in the execution ledger or support bundle because
  it increases sensitive-data risk and duplicates Forward as the source of
  truth.
