# Forward NetBox Architecture

`forward_netbox` is a NetBox plugin with one primary workflow: fetch Forward data through Forward API/NQE, transform rows into NetBox model operations, stage those operations in NetBox Branching branches, and optionally merge those branches.

## Runtime Flow

1. A `ForwardSource` stores Forward connection settings and resolves available networks.
2. A `ForwardSync` selects models, snapshot mode, branch budget, and auto-merge behavior.
3. The sync job resolves the Forward snapshot, validates query shape, fetches NQE rows, and builds either a Branching plan or a fast bootstrap workload.
4. A `ForwardValidationRun` records pre-branch drift/policy results. Blocking policies stop the sync before branch creation.
5. The selected execution backend applies rows through the same NetBox model adapters.
6. The Branching backend creates reviewable native Branching shards; the fast bootstrap backend writes directly after validation for large initial imports.
7. `ForwardIngestion`, `ForwardValidationRun`, and `ForwardIngestionIssue` retain run metadata, logs, statistics, and issues.

NQE remains the source of truth for normalization and model-shaped rows. Execution
backends may decide how validated rows are applied to NetBox, but they must not
introduce separate Python-side data mutation rules that diverge from the NQE map
contracts.

## Scaling Direction

Large inventories need two distinct execution lanes with one shared contract:

- `Branching` is the review lane. It should remain NetBox-native and
  Branching-native: bounded branches, native `ChangeDiff` review, native merge,
  native branch cleanup, and resumable shard/merge jobs.
- `Fast bootstrap` is the trusted baseline lane. It should continue to use the
  same NQE maps, validation, row adapters, statistics, and issue reporting, but
  it deliberately skips Branching review so very large first loads can establish
  a diff baseline without spending days generating branch diffs.
- Future bulk execution, including TurboBulk or parquet-backed loaders, belongs
  below those lanes as an apply engine. It must not become a separate source of
  truth or a separate sync workflow. If available, it can accelerate either a
  direct baseline or a Branching shard while still consuming the same NetBox-
  shaped NQE rows.

The long-term scale backlog is tracked in
`docs/03_Plans/completed/2026-05-15-scale-hardening-remaining-work.md`. Work should
fit one of the durable lanes there: ledger-first orchestration, shard-scoped
fetch contracts, engine-based apply mechanics, self-service diagnostics,
recovery/chaos validation, or branch alignment. Changes outside those lanes
should be treated skeptically because they risk creating a second sync product
instead of strengthening the native NetBox workflow.

Use that plan's Open Architecture Register as the source for remaining
scale-alignment work. New scale ideas should either update an existing register
row with evidence, add a new row with a clear target and next tranche, or be
explicitly rejected in the plan's decision log.

Use the plan's "Long-Term Completion Backlog" as the checklist for larger
refactors that remain intentionally open. Each item has a reason, current
position, and completion signal; future implementation plans should move one of
those rows forward with tests or explicit deferral evidence instead of adding
untracked TODOs.

The plan's "Remaining Long-Term Architecture Alignment" section is the current
checklist for larger refactors that are still intentionally incomplete:
ledger-only orchestration, per-model shard-fetch contracts, faster apply
engines under the existing workflows, fast-bootstrap baseline evidence,
self-service support bundles, chaos/scale release gates, branch alignment, and
capacity/database operations guidance.

The remaining long-term alignment work is concentrated in four areas:

- Retire active compatibility branch-state-cache orchestration after
  ledger-derived UI, API, recovery, and support behavior are proven across
  upgrades.
- Give every model an explicit fetch contract: shard-safe filters, optional NQE
  pushdown parameters, exact local safety filter, and fallback reason.
- Keep the adapter apply engine as the correctness baseline until a faster
  engine can prove equivalent NetBox validation, object change tracking,
  Branching semantics, and row-level issue handling.
- Promote chaos/scale testing and self-service diagnostics into release gates
  for any change that touches Branching execution, recovery, shard planning, or
  apply mechanics.

## Explicit Deferred Risks (Current Tranche)

These are intentionally tracked as deferred architecture items, not current
tranche blockers:

1. Compatibility branch-state cache removal after the compatibility window
   - Current state: ledger-first orchestration is active; compatibility JSON is
     retained as read-through/upgrade safety.
   - Blocking status: not a blocker for the current tranche.
2. Live Forward runtime proof for deeper query pushdown
   - Current state: shard/pushdown plumbing and profiling surface exist; live
     runtime capture is pending external run evidence.
   - Blocking status: not a blocker for the current tranche.
3. Destructive Docker worker-kill harnessing
   - Current state: opt-in local kill harness and synthetic chaos gate exist;
     scenario-by-scenario evidence capture remains ongoing.
   - Blocking status: not a blocker for the current tranche.
4. Future faster apply engine (`bulk_orm` and later engines)
   - Current state: apply-engine boundary is in place. The adapter remains the
     default engine; opt-in `bulk_orm` is enabled only for parity-tested simple
     models (`dcim.site`, `dcim.manufacturer`, `dcim.devicerole`, `ipam.vrf`,
     and `ipam.vlan`).
   - Blocking status: not a blocker for the current tranche.

The current resumable Branching state is stored in `ForwardSync.parameters` as a
compatibility bridge. The target scale architecture is a first-class execution
ledger:

- one execution/run record for a selected sync, snapshot, validation result,
  backend, and branch budget
- one step record per planned model/shard/merge/finalize action
- immutable query identity on each step: model, map name, execution mode,
  repository path or query ID, commit ID when present, snapshot ID, and baseline
  snapshot ID when using diffs
- mutable operational state on each step: queued, running, staged, merging,
  merged, skipped, failed, timeout, retryable, cancelled
- job IDs, branch IDs, ingestion IDs, retry counts, timestamps, heartbeat, and
  last error recorded on the step rather than only inside logs

This ledger keeps the native NetBox UI/API workflow but removes the biggest
scaling risks of parameter-only state: lost updates, ambiguous retries, and
hard-to-reconstruct support bundles.

The ledger must also become the durable evidence source. A support bundle should
remain useful after the run completes, after branches are cleaned up, after a
worker dies, and after the compatibility branch-state cache is cleared. Live
sync state can be a display cache, but it should not be the only place where
support can recover the planned steps, observed jobs, retry history, query
references, branch IDs, and final baseline decision.

The compatibility branch-state cache is not the long-term control plane. New
scale work should write and read `ForwardExecutionRun`/`ForwardExecutionStep`
first, then maintain the compatibility cache only where an existing NetBox view,
job entry point, or upgrade path still requires it. Active compatibility-cache
writes should not be removed until an old-state upgrade fixture, missing-JSON
recovery tests, and the scale/chaos gate prove that stage, merge, retry,
discard, finalize, health, API, and support-bundle behavior all work from
ledger state alone.

## Scale Hardening Principles

- Make every stage and merge job idempotent. Re-running a job must either resume
  the same step or report that the step already advanced; it must not enqueue a
  duplicate shard or merge.
- Claim execution steps with a database lock or equivalent lease before doing
  work. Multiple NetBox workers may run concurrently, and step advancement must
  not rely on best-effort JSON read/modify/write behavior.
- Advance stage, merge, retry, discard, and finalize transitions through
  explicit ledger guards. Duplicate callbacks and simultaneous workers should
  produce a no-op or one clear recovery recommendation, never a second hidden
  state transition.
- Treat stale worker death separately from handled timeouts. If a worker exits
  without running the exception handler, the UI needs a native recovery action
  that reconciles job state, branch state, and the execution ledger before
  requeueing the current step.
- Keep branch cleanup explicit. Successful auto-merge may remove the branch by
  default, but failed and manually reviewed branches should remain inspectable
  until the operator discards or retries them.
- Keep row data out of durable orchestration state and support bundles. Persist
  identities, counts, query references, branch/job IDs, and error metadata; do
  not persist customer rows or private inventory examples.
- Preserve the baseline contract. A successful fast bootstrap can seed later
  Branching diffs only for a later snapshot and only when the enabled maps
  resolve to query IDs or repository paths that can execute Forward diffs.
- Prefer throughput improvements that reduce duplicated work: query diffs,
  shard-scoped refetch for retries, NQE-side coalescing, bounded page sizes, and
  optional bulk apply engines. Do not hide large Branching workloads by raising
  branch budgets above local operational guidance.
- Scheduler overlap must remain bounded and ledger-native. The only supported
  overlap shape is to pre-stage one eligible next shard while the current shard
  is already queued for merge; merge jobs remain serialized, and an already
  staged shard is merged by the next ledger handoff rather than staged again.

## Query And Shard Fetch Direction

The current resumable Branching implementation reruns the current model query
and then selects the persisted shard from the rebuilt model workload. That is
recoverable, but it is not the final scale shape for very large models because a
model with many shards can pay full model query cost for every shard retry.

The target fetch layer should support shard-scoped execution:

- Each planned shard should carry a stable shard predicate or bucket identity
  that can be sent back to Forward NQE.
- Built-in NQE should accept optional shard parameters where the Forward data
  model can filter deterministically without changing NetBox row semantics.
- If a model cannot be safely shard-filtered in NQE, the planner should say so
  and treat it as a full-model refetch fallback.
- Diff execution should preserve the same contract: run Forward `nqe-diffs`
  against the selected query ID/path and then limit work to the current shard
  where possible.
- The support bundle should state whether each step used shard-scoped fetch,
  model-scoped fetch, full fallback, or bulk engine execution.

Current implementation status:

- Query fetch now records effective per-step fetch metadata (mode, key family,
  fetch/query parameters, and column filters) from query execution through
  workload planning into `ForwardExecutionStep`.
- Partitioned shard-scoped fetches now run concurrently (bounded by
  `query_fetch_concurrency`) for both full and diff query paths, while results
  are merged back in partition order for deterministic downstream apply/delete
  behavior.
- When shard-scoped fetch falls back (for example, scoped NQE fetch failure or
  diff fallback), the effective fetch mode is recorded (`full_fallback` or
  `diff_fallback`) and the fallback reason is retained in fetch metadata for
  support export.
- Recovery recommendations now explicitly detect stale active step heartbeat and
  stale run heartbeat and steer operators to `reconcile` instead of indefinite
  wait/monitor guidance.
- Run finalization now requires ledger evidence that stage steps are terminal;
  completion is refused while pending/running/staged stage steps still exist.
- Compatibility `_branch_run` writes are now fully suppressed while an active
  execution run exists; the ledger remains the only writable orchestration
  state during active Branching execution.
- Compatibility `_branch_run` writes are now also suppressed once any
  execution-run history exists, keeping compatibility JSON as read-through
  upgrade data rather than a mutable runtime control plane.
- Merge continuation now prefers execution-ledger run/step state over
  compatibility JSON so stale `_branch_run` flags cannot block auto-merge shard
  enqueue decisions.
- Plan-item lookup and execution context loading now prefer active ledger state
  over compatibility JSON when both are present, preventing stale `_branch_run`
  payloads from overriding current shard index or merge-wait status.
- Stage-worker claim and shard dispatch now synthesize state from the active
  execution run before reading compatibility JSON, so stale `_branch_run`
  payloads cannot misdirect shard claim index selection.
- Planner invocation now treats an explicit empty `branch_run_state` as
  authoritative input instead of falling back to stored compatibility JSON.
- Planner/runtime resumption paths now use branch-run display synthesis
  (active ledger first, compatibility fallback only when there is no ledger
  history), so stale `_branch_run` payloads cannot steer new runs once a sync
  has execution-run history.
- Live sync-state rendering now uses active execution runs only; when ledger
  run history exists but no run is active, stale compatibility `_branch_run`
  payloads are suppressed from activity/branch-run display and state-gating
  helpers.
- Sync Health now includes a query-runtime/pushdown profile from latest
  execution-step evidence (fetch-mode counts, fallback-step count/reasons, and
  slowest query models), so operators can identify pushdown gaps from native
  NetBox surfaces before collecting deeper support bundles.
- Sync Health now also includes compatibility-cache retirement diagnostics
  (ledger-history presence, stale `_branch_run` payload detection, and prune
  recommendation) so operators can verify whether a sync is truly ledger-only
  or still carrying legacy compatibility JSON.
- A native maintenance command (`forward_prune_compatibility_cache`) and task
  (`invoke prune-compat-cache`) can now prune stale compatibility `_branch_run`
  payloads in bulk once ledger history exists and no run is active, with
  dry-run and JSON report output for support/release evidence.
- Execution-run support bundle exports now include explicit compatibility-cache
  evidence (legacy payload presence, active-run linkage, stale-payload
  detection, and prune recommendation), so support artifacts can prove whether
  a run is operating ledger-only or still carrying legacy state.
- Ledger display-state synthesis now also prunes stale compatibility
  `_branch_run` payloads during read paths when only terminal run history
  remains, so legacy JSON state is actively retired as the UI/API reads
  execution state.
- Stage job dispatch now refuses to execute when no active execution run is
  claimable and ledger history already exists, preventing stale queued jobs
  from replaying completed runs.
- When stage dispatch or stage enqueue refuse execution due to completed ledger
  history, stale compatibility `_branch_run` payloads are now pruned from sync
  parameters so historical JSON cannot linger as ambiguous runtime state.
- Stage enqueue continuation now accepts ledger fallback only for failed/timeout
  resumable runs and suppresses stale compatibility `_branch_run` requeue when
  only terminal completed/cancelled run history exists.
- Execution-run initialization now ignores stale compatibility
  `execution_run_id` references to terminal runs, preventing historical
  completed/failed runs from being reused as active run state.
- Execution-run initialization now claims a sync-row lock and rechecks active
  nonterminal runs under that lock before creating a new run, preventing
  duplicate active ledger runs under concurrent worker startup.
- Execution-run initialization now also prunes stale compatibility `_branch_run`
  payloads when only terminal ledger history exists before creating a new run.
- Merge follow-on stage enqueue now requires an active ledger run (or the
  ingestion-linked run) and refuses compatibility-only continuation once ledger
  history exists.
- Merge queue eligibility fallback now uses synthesized branch-run display
  state (ledger first, compatibility only for true pre-ledger syncs), so stale
  compatibility `pending_ingestion_id` values cannot re-open merge actions.
- Legacy compatibility pending-state checks are still honored only for true
  pre-ledger syncs (no execution-run history), preserving upgrade/read-through
  behavior without reintroducing stale compatibility-state orchestration.
- Destructive `docker-chaos-kill` evidence capture now verifies that the
  exported execution-run bundle is present and contains run metadata, step
  details, and a recognized scenario-aligned recovery action before reporting
  success. Scenario-specific step evidence is also required (branch linkage,
  row-progress counters, or merge-job linkage depending on scenario).

## Operator Observability Target

The support surface should be one button from the sync or ingestion detail page.
The exported bundle should include:

- sync, source, backend, snapshot selector, resolved snapshot, branch budget, and
  enabled model list
- validation run status, drift policy decision, blocking reasons, and model
  result summaries
- execution plan and every step status, including job IDs, branch IDs,
  ingestion IDs, retry counts, heartbeat, and last error
- all job data and rendered job log entries for every coordinator, shard,
  merge, and finalize job in the run
- per-model query mode, query reference, row counts, delete counts, runtime, and
  diff baseline snapshot
- ingestion issues with sanitized payloads and no committed customer identifiers

This is the primary troubleshooting artifact for long-running customer syncs.
Individual ingestion log export remains useful, but a multi-shard baseline needs
run-level export because the failure may involve a previous shard, a merge job,
or a stale coordinator state.

## Recovery Model

Recovery should be explicit and native:

1. Reconcile the execution ledger against NetBox job state, Branching branch
   state, and plugin ingestion state.
2. If a step is running but its job is failed, missing, or stale beyond the
   configured heartbeat threshold, mark it retryable with a diagnostic reason.
3. If a branch was staged successfully but merge did not complete, offer requeue
   merge from the ingestion detail page.
4. If a branch was partially staged or is unsafe to reuse, keep it visible and
   create the retry as a new native Branching branch while linking both attempts
   in the run ledger.
5. If the final step succeeds without blocking issues, mark the final ingestion
   baseline-ready. Dirty or partially failed runs must not become diff
   baselines.
6. Preserve enough run/step/job/branch evidence to export a useful support
   bundle even when the compatibility sync state has been cleared or a later
   run has started.

## Production Boundaries

- Plugin state and job entrypoints: `forward_netbox/models.py`
- UI workflow: `forward_netbox/views.py`, `forms.py`, `tables.py`, and templates
- REST API workflow: `forward_netbox/api/`
- Forward API client: `forward_netbox/utilities/forward_api.py`
- Sync contracts: `forward_netbox/utilities/sync_contracts.py`
- Sync validation: `forward_netbox/utilities/model_validation.py`
- Query registry and shipped query loading: `forward_netbox/utilities/query_registry.py` and `forward_netbox/queries/`
- Query fetch, snapshot resolution, NQE execution, and model-result reporting: `forward_netbox/utilities/query_fetch.py`
- Diagnostic synthesis for IPAM/routing query warnings: `forward_netbox/utilities/query_diagnostics.py`
- Branch planning and branch-budget behavior: `forward_netbox/utilities/branch_budget.py`
- Multi-branch planning: `forward_netbox/utilities/multi_branch_planner.py`
- Multi-branch execution and retry behavior: `forward_netbox/utilities/multi_branch_executor.py`
- Fast bootstrap direct-write execution: `forward_netbox/utilities/fast_bootstrap_executor.py`
- Direct sync-stage execution: `forward_netbox/utilities/sync_execution.py`
- Multi-branch lifecycle helpers for branch creation, overflow retry, and resume state: `forward_netbox/utilities/multi_branch_lifecycle.py`
- Ingestion merge orchestration and signal suppression: `forward_netbox/utilities/ingestion_merge.py`
- Sync job orchestration and failure capture: `forward_netbox/utilities/sync_orchestration.py`
- Validation and drift-policy evaluation: `forward_netbox/utilities/validation.py`
- Validation force-allow audit helper: `forward_netbox/utilities/validation.py`
- NetBox row application and model adapters: `forward_netbox/utilities/sync.py`
- Row reporting, issue capture, shard heartbeat logging, and per-row continue-on-error handling: `forward_netbox/utilities/sync_reporting.py`
- Generic coalesce, upsert, delete-by-coalesce, and model lookup primitives: `forward_netbox/utilities/sync_primitives.py`
- Sync state, progress heartbeat, stale-progress activity, and execution-summary helpers: `forward_netbox/utilities/sync_state.py`
- Sync event flushing and clear-events bridging: `forward_netbox/utilities/sync_events.py`
- Sync facade helpers for snapshot resolution, NQE map access, query parameters, and job enqueueing: `forward_netbox/utilities/sync_facade.py`
- Logging/statistics: `forward_netbox/utilities/logging.py`
- Sensitive-content guard: `forward_netbox/utilities/sensitive_content.py` and `scripts/check_sensitive_content.py`

## Overgrown But Stable Areas

The following modules are intentionally treated as stable boundaries until a dedicated refactor plan exists:

- `forward_netbox/utilities/sync.py`: model-adjacent helper and shim glue, coalesce behavior, dependency failure handling, and row application.
- `forward_netbox/utilities/sync_runner_contracts.py`: runner conflict-policy, coalesce-identity, and diff-splitting contract helpers extracted from `sync.py`.
- `forward_netbox/utilities/sync_runner_adapters.py`: runner adapter and model-specific apply/delete helper family extracted from `sync.py`.
- `forward_netbox/utilities/sync_cable.py`: cable adapter apply/delete lookup helpers extracted from the main sync module.
- `forward_netbox/utilities/sync_interface.py`: interface, MAC address, and feature-tag adapter entrypoints extracted from the main sync module.
- `forward_netbox/utilities/sync_routing.py`: routing and peering helper logic plus apply/delete entrypoints extracted from the main sync module.
- `forward_netbox/utilities/sync_reporting.py`: row-level issue recording, dependency tracking, shard heartbeat logging, and aggregated warning/reporting helpers extracted from the main sync module.
- `forward_netbox/utilities/sync_primitives.py`: generic coalesce, upsert, delete-by-coalesce, optional-model, and lookup helpers extracted from the main sync module.
- `forward_netbox/models.py`: persisted model behavior, job state transitions, validation state, and execution-ledger state; validation override writes now delegate to `forward_netbox/utilities/validation.py`.
- `forward_netbox/utilities/sync_state.py`: execution-ledger state helpers, progress heartbeat, stale-progress display, display parameters, and sync activity summaries.
- `forward_netbox/utilities/sync_events.py`: event queue flush helper extracted from the main sync module.
- `forward_netbox/utilities/sync_facade.py`: remaining `ForwardSync` helper behavior, including snapshot resolution, enabled-model access, and enqueue wrappers.
- `forward_netbox/utilities/ingestion_merge.py`: ingestion merge orchestration plus merge-job enqueueing, change-total persistence, and branch cleanup.
- `forward_netbox/utilities/model_validation.py`: sync/source/NQE validation contract plus scheduled-time and enabled-model checks.
- `forward_netbox/utilities/multi_branch_planner.py`: query fetch, preflight, plan assembly, and model-result capture.
- `forward_netbox/utilities/query_diagnostics.py`: IPAM/routing diagnostic synthesis and warning aggregation extracted from the fetcher.
- `forward_netbox/utilities/multi_branch_executor.py`: branch execution, auto-merge, resume state, and overflow retry; the main state machine is now split into smaller helpers.
- `forward_netbox/utilities/sync_execution.py`: direct query/apply/delete sync-stage execution for the legacy non-execution runner path.
- `forward_netbox/utilities/multi_branch_lifecycle.py`: branch creation, branch cleanup, overflow retry, resume-state updates, and per-shard ingestion wiring.
- `forward_netbox/utilities/sync_orchestration.py`: sync job orchestration, status transitions, and failure capture.
- `forward_netbox/utilities/multi_branch.py`: compatibility shim that re-exports the planner and executor surfaces.
- `forward_netbox/utilities/sync_runner_adapters.py`: runner adapter and model-specific apply/delete helper family extracted from `sync.py`.
- `forward_netbox/utilities/sync_runner_contracts.py`: runner conflict-policy, coalesce-identity, and diff-splitting contract helpers extracted from `sync.py`.

Do not move code out of these modules as drive-by cleanup. Refactors should first add or update tests that pin the existing behavior.

## Intended Future Layers

Current and future refactors should stay inside these smaller layers without changing public behavior:

- contracts: validation of row shape, model identity, and coalesce rules
- query fetch: snapshot resolution, query execution, pagination, diffs, and per-model results
- planning: workload grouping, shard sizing, and branch budget estimation
- execution: branch lifecycle, shard retries, merge handoff, and resume state
- validation: pre-branch policy decisions, drift summaries, and blocking reasons
- adapters: per-NetBox-model row apply/delete behavior
- reporting: logs, statistics, model results, issues, row failures, and operator-facing progress
- primitives: generic coalesce, lookup, and update-or-create behavior shared by adapters

## Non-Negotiable Constraints

- Keep the default sync and merge behavior NetBox-native and Branching-native.
- Preserve the UI/API sync workflow; large-dataset behavior must be selected through the sync execution backend, not a separate tool.
- Keep normalization and model shaping in NQE; Python execution paths should consume the same native NetBox-shaped row contracts.
- Keep branch budgets configurable and bounded according to NetBox Branching guidance.
- Never persist customer data, credentials, private network IDs, or snapshot IDs in committed tests/docs.
- Keep shipped query changes paired with tests and reference documentation.
