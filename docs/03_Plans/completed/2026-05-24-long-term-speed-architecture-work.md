# 2026-05-24 Long-Term Speed Architecture Work

## Goal

Record the remaining long-term architecture work for speed, scale, and
reliability after the ledger-first, fast-bootstrap, shard-fetch, recovery, and
observability refactors.

This document is intentionally shorter than the full roadmap. Use it as the
implementation-facing checklist for what is still worth doing architecturally,
what has already been addressed, and what must not be turned into a separate
non-native sync workflow.

## Constraints

- NQE remains the source of truth for row shape, normalization, coalescing,
  filtering, and model identity.
- NetBox-native writes remain the only mutation path.
- Branching remains the reviewable steady-state path.
- Fast bootstrap remains an explicit trusted-baseline path for very large first
  imports.
- Faster engines such as `bulk_orm`, future TurboBulk, parquet loaders, or
  NetBox-native bulk primitives must plug under the existing apply-engine
  boundary.
- The execution ledger remains the orchestration control plane.
- No customer identifiers, network IDs, snapshot IDs, credentials, screenshots,
  or private row examples belong in committed artifacts.

## Related Roadmap Artifacts

- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-position.md`
- `docs/03_Plans/active/2026-05-23-long-term-scale-architecture-direction.md`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-remaining-refactors.md`
- `docs/03_Plans/active/2026-05-23-remaining-architecture-execution-summary.md`
- `docs/03_Plans/active/2026-05-24-long-term-architecture-closure.md`
- `docs/03_Plans/active/2026-05-24-long-term-architecture-next-tranche.md`

## Touched Surfaces

- `docs/03_Plans/active/2026-05-24-long-term-speed-architecture-work.md`
- Related roadmap artifacts listed above.
- Future runtime tranches will likely touch:
  - `forward_netbox/utilities/query_fetch_execution.py`
  - `forward_netbox/utilities/apply_engine.py`
  - `forward_netbox/utilities/apply_engine_decision.py`
  - `forward_netbox/utilities/branch_budget.py`
  - `forward_netbox/utilities/execution_ledger*.py`
  - `forward_netbox/utilities/execution_telemetry.py`
  - `forward_netbox/utilities/health*.py`
  - `forward_netbox/utilities/health_summary_blocks.py`
  - `forward_netbox/tests/`

## Approach

Use this file as the concise implementation checklist for the current
long-term speed architecture. The detailed roadmap remains authoritative for
status history and validation evidence.

## Current Architecture State

The current architecture is directionally correct. The major refactors fixed
the control-plane problems that made large imports fragile:

1. Execution is ledger-first, not compatibility-JSON-first.
2. Branching and fast bootstrap share the same NQE-shaped row contracts.
3. Shard-scoped fetch contracts exist for the supported model set.
4. Partitioned Forward fetch can run with bounded concurrency and deterministic
   result ordering.
5. Recovery behavior is reason-coded and visible in support bundles.
6. Sync Health and support bundles expose diff, fallback, recovery, density,
   throughput, delete/dependency, and operator-tuning signals.
7. Runtime fetch artifacts now avoid repeated scoped Forward query work during
   retry/resume without becoming a durable row store.
8. Delete-heavy plans now expose dependency order, delete-wave risk, max delete
   shard size, and likely reference-blocker risk before operators merge.
9. NetBox 4.5 and 4.6 support should remain one code path through capability
   gates and CI matrix coverage.

The remaining work is runtime economics and self-service polish. It should make
the existing workflow cheaper and easier to operate, not replace it.

## Current Continuation Assessment

Status: `architecture_converged_runtime_evidence_open`

The current codebase does not need another broad re-architecture before the
next release line. The main shape is correct:

- NQE remains the row-shaping and normalization boundary.
- NetBox-native writes remain the only mutation path.
- Branching remains the reviewable steady-state path.
- Fast bootstrap remains the trusted baseline path for very large first loads.
- The execution ledger is the runtime control plane.
- Faster write paths such as `bulk_orm`, future TurboBulk, parquet loaders, or
  NetBox-native bulk primitives remain apply-engine implementations, not
  separate product workflows.

The latest hardening work closed two important recovery gaps:

- orphaned queued stage steps with no job, branch, or ingestion now reset to
  `pending` during reconciliation and can be resumed through
  `invoke execution-run-recovery`.
- long-running live NetBox jobs are no longer failed solely because the
  execution-step heartbeat is stale; if that false failure happened already,
  reconciliation can restore the step to `running` while the live job continues.
- stale NetBox core job rows are no longer enough to block recovery when RQ
  state is inspectable. If the corresponding RQ job is absent from active queue,
  started, scheduled, and deferred registries, reconciliation treats the stage as
  stale and requeueable after a short heartbeat grace window. This keeps
  recovery much faster than the full stale-stage threshold while avoiding
  duplicate shard execution during transient PostgreSQL/RQ restarts. If RQ
  cannot be inspected, the previous row-based behavior is preserved.
- Forward exception text used in retry warnings, fallback reasons, and
  query-validation diagnostics is sanitized before persistence. This preserves
  the useful failure cause while redacting request identifiers that should not
  be copied into support bundles or committed evidence.

The remaining architecture work is now evidence-driven:

1. finish a completed large-run artifact or sanitized support bundle.
2. use it to prove fallback pressure is low or explainable.
3. use it to decide whether scheduler overlap is truly justified.
4. expand `bulk_orm` only through model-specific parity gates.
5. keep NetBox 4.5 and 4.6+ on one capability-gated branch.

Current local evidence is useful but not final release proof. The active large
run was resumed and is progressing through the native recovery path, but the
two open audit gates still require terminal large-run evidence:

- `runtime_fallback_reduction_evidence_verified`
- `scheduler_overlap_readiness_evidence_verified`

Do not close those gates from partial runs, screenshots, or synthetic-only
evidence.

## Current Speed Findings

The biggest speed wins already implemented are structural:

- bounded partitioned Forward fetch with deterministic result ordering.
- shard-scoped fetch contracts for supported models.
- runtime-only fetch artifacts for retry/resume within a run.
- explicit fallback reason and partition-retry telemetry.
- adaptive density learning and runtime-aware branch budget shaping.
- safe `bulk_orm` lane behind the apply-engine selector for the proven model
  set, now including the narrow `dcim.virtualchassis` guarded-assignment path
  and the dependency-anchored `dcim.macaddress` assignment path.
- exact dependency lookup caches now include negative interface misses for a
  runner lifetime. This keeps skip-heavy MAC/IP/cable-style shards from
  re-querying NetBox for the same absent interface while still invalidating the
  miss if that exact interface is created or remembered later in the run.
- support-bundle and Sync Health guidance that points operators at the first
  useful tuning action.
- source-level `query_preflight_enabled` control now allows large runs to skip
  duplicate startup preflight query sampling when faster initial planning is
  more important than preflight-first query validation.
- Query spec resolution is now cached per fetcher/model within a planning pass,
  so query-path maps are resolved once per model even when preflight and
  workload fetch both run.
- Single-device shard filter generation now uses `EQUALS_ANY` uniformly, which
  avoids avoidable first-attempt operator retries on Forward API paths that are
  strict about default operator semantics.
- parallel workload fetch now preserves the original enabled-model/query-map
  order when handing rows to the branch planner. Fetch jobs can complete out of
  order, but dependent models such as `dcim.macaddress` must not be planned
  before `dcim.interface`.
- Branching and Fast bootstrap now both consume the same dependency-phased
  workload ordering. Apply order is explicit contract state, not an accident of
  fetch completion order or form/model ordering.
- Sharded plan items now materialize their shard-scoped fetch contract during
  planning. Large `dcim.device` shards no longer keep the full-model planning
  fetch mode when the shard has stable `name` keys, and `dcim.cable` keeps exact
  direction-insensitive cable identity shards while using a device-column
  superset fetch plus local shard filtering.

The next speed work should be targeted, not broad:

1. **Reduce repeated full/model fallback first.**
   Fallback is the highest-value remaining runtime cost because it multiplies
   Forward query work. Fix it at the query contract or Forward API execution
   layer before adding scheduler complexity.
2. **Use partition-retry telemetry correctly.**
   A warning such as a single-value column-filter retry is not automatically a
   failure. If the alternate operator succeeds, that is a healthy avoided
   fallback. If it repeatedly falls through to full/model fetch, treat it as a
   query-fetch contract defect.
3. **Tune capacity before overlap.**
   Worker count, worker timeout, query-fetch concurrency, NQE page size,
   PostgreSQL settings, and disk/container placement should be represented in
   evidence before scheduler overlap is enabled.
4. **Promote apply engines slowly.**
   `bulk_orm` should expand only when each model proves create/update/delete,
   validation failure, row issue, dependency, object-change, Branching, support
   statistics, and runtime non-regression parity.

## Implemented Speed Tranche: Bounded Scheduler Overlap

Status: `implemented_opt_in_pending_large_run_evidence`

The Branching scheduler now has an opt-in `scheduler_overlap` parameter exposed
as `Stage next shard during merge` on the sync form.

Behavior:

- Requires auto-merge.
- Queues at most one eligible next stage job while the current shard is already
  `merge_queued`.
- The overlap job claims its explicitly assigned queued ledger step instead of
  trusting the run's normal `next_step_index`, because reconciliation correctly
  treats the current `merge_queued` shard as the first incomplete step.
- The overlap stage does not enqueue its own merge.
- When the prior merge completes, the normal merge-continuation handoff first
  checks for an already-staged next shard and queues that shard's merge.
- Duplicate stage jobs for `queued`, `running`, `staged`, or `merge_queued`
  steps are no-ops or return the existing job reference.

Non-goals:

- It does not run concurrent native Branching merges.
- It does not raise branch budgets.
- It does not change NQE row shape, normalization, coalescing, or model
  identity.
- It does not make the Branching path as fast as fast bootstrap for trusted
  first baselines.

Validation added:

- overlap stage jobs carry an explicit `overlap_stage` flag.
- overlap stage jobs claim the queued step assigned to the job.
- pre-staged shards are merged by the next native merge handoff.
- already staged shards are not staged again.
- the form stores overlap only when auto-merge is enabled.

## Current Watch Items

These are not reasons to redesign the product path, but they are the next
things to watch in large-run evidence:

- **Column-filter retry pressure.** The live run has shown Forward API warnings
  around single-value column-filter retries. That can be acceptable if the
  alternate operator succeeds and avoids broader fallback. If support bundles
  show persistent fallback after these retries, fix the column-filter payload
  contract or query-fetch execution path. Those warnings should now retain the
  actionable error while redacting request identifiers.
- **Scheduler overlap readiness.** Current metrics can identify queue/wait
  pressure, but overlap should remain disabled until repeated completed
  evidence proves it is the bottleneck after capacity tuning.
- **Runtime capacity preservation.** Evidence tasks must not reset worker
  counts or source fetch settings while a long run is active. Use
  `architecture-runtime-evidence --skip-chaos` for non-destructive refreshes.
- **Worker auto-restart during local testing.** Dev containers watch Python
  files and can restart workers during active jobs. RQ-aware job liveness now
  lets recovery distinguish a real live job from a stale core job row and safely
  requeue through the native recovery command.
- **Parallel fetch ordering.** A live run exposed that completion-order
  workload collection could plan dependent models out of order. That is now
  fixed by preserving job order after parallel fetch completion; new runs should
  not plan MAC address shards before interface shards. The branch planner and
  fast-bootstrap executor also use explicit dependency-phased apply ordering so
  this remains correct even if a future fetch surface returns rows out of order.
  A read-only plan probe against the field-scale source for only
  `dcim.macaddress` and `dcim.interface` confirmed the MAC query can complete
  first while the generated branch plan still schedules all interface shards
  before MAC address shards. The probe took about 567 seconds and created no
  branches or NetBox object changes.
- **MAC address apply cost.** `dcim.macaddress` has been promoted to the
  parity-tested `bulk_orm` safe set. It still uses the same NQE row shape and
  exact device/interface identity, but prefetches devices, interfaces, and
  existing MAC rows once per shard before using NetBox validation plus
  `bulk_create`/`bulk_update`. Missing device/interface rows still surface as
  per-row skipped or failed issues, matching the adapter contract.
- **Shard fetch fallback reduction.** Split plan items now persist the computed
  fetch contract instead of inheriting the full-model planning mode. Cable rows
  remain sharded by exact canonical endpoint identity, but the Forward fetch is
  narrowed by canonical local `device` values and then filtered locally back to
  the exact shard keys. This reduces repeated full cable/device fetches without
  changing NQE output, coalescing, or Branching semantics.
- **Dependency lookup economics.** High-volume adapter paths now use
  runner-level positive lookup caches for exact device, interface, and module
  bay identities. The cache stores only confirmed NetBox objects and does not
  cache misses, preserving same-run dependency creation behavior while reducing
  repeated per-row database reads.
- **Coalesce lookup economics.** Adapter coalesce resolution now checks for
  uniqueness with one bounded `LIMIT 2` lookup instead of a successful
  `first()` query followed by a duplicate `exists()` query. This preserves the
  same no-match, single-match, and ambiguous-match contract while reducing the
  hottest per-row database path. Exact device-scoped identity coalesces such as
  `dcim.interface(device, name)` and `dcim.modulebay(device, name)` also reuse
  the runner's positive lookup cache after batch priming, while non-exact
  lookups still hit the database and preserve ambiguous-row detection. Adapter
  updates compare foreign-key IDs directly instead of loading related objects
  only to determine that a dependency reference is unchanged. Batch priming for
  device-scoped identities now uses exact `(device_id, name)` predicates in
  bounded chunks instead of broad `device_id IN (...) AND name IN (...)`
  filters, avoiding accidental Cartesian overfetch on large interface/module
  shards.
- **IPAM and routing identity lookup economics.** Repeated adapter lookups for
  VRF and ASN identities now use the same runner-local positive-cache contract
  as device/interface dependencies. Misses are not cached, so same-run
  dependency creation still works. Optional routing and peering plugin
  coalesce lookups now use an allowlisted exact-identity cache for native keys
  such as BGP router assignment, BGP peer scope/peer, BGP address-family
  scope/family, OSPF instance/interface identity, and peering session peer
  identity. The cache is populated only after a bounded unique lookup or a
  successful upsert and is invalidated when cached objects are deleted.
- **MAC dependency skip economics.** Field-scale evidence showed many MAC
  address rows can legitimately reference interfaces that were not imported.
  MAC assignment now follows the existing IP address and cable contract:
  missing target interfaces become aggregated skipped rows instead of one
  failure issue per MAC address. This removes a high-cardinality issue-write
  path while keeping dependency failures explicit when the interface itself is
  known to have failed.
- **Compatibility cleanup.** Compatibility payloads should remain upgrade and
  read-through surfaces only. New orchestration behavior should use the
  execution ledger directly.
- **Support self-service.** The support bundle, recovery command, Sync Health,
  and scale benchmark need to remain the operator-facing way to troubleshoot
  large runs without sharing private data.

## Remaining Long-Term Architecture Work

The broad architecture is now in the right shape. The remaining work should be
treated as targeted hardening and economics work, not another platform rewrite.

What should stay fixed:

1. **One native workflow.**
   - Operators should choose Branching or Fast bootstrap, but both paths should
     share the same NQE maps, row contracts, validation behavior, statistics,
     support bundle shape, and execution ledger.
   - Do not add a separate "large customer" workflow outside NetBox-native
     models, Branching, and the existing apply-engine boundary.
2. **NQE as the data contract.**
   - Filtering, normalization, coalescing, and model identity belong in NQE and
     the shipped query contracts.
   - Python can protect NetBox with validation, reference checks, shard safety,
     and local post-filtering where required, but it should not become a second
     normalization language.
3. **Execution ledger as the control plane.**
   - Shard claim, fetch, stage, merge, retry, skip, stale recovery, timeout
     handling, and support export should all remain ledger-derived.
   - Compatibility JSON should stay migration/read-through only, not an active
     scheduling surface.
4. **Apply engines as implementation details.**
   - `bulk_orm`, future TurboBulk, parquet loaders, or NetBox-native bulk APIs
     should plug under the apply-engine selector.
   - Operators should not have to learn a separate sync mode just because a
     faster model engine is available.

Work still worth doing:

1. **Close runtime fallback evidence.**
   - Finish a completed large-run artifact or sanitized support bundle where
     run state, step state, row failures, fallback, partition retry, and diff
     utilization all agree.
   - Use that evidence to decide whether the next fetch change is query/NQE,
     Forward API execution, or local safety filtering.
2. **Expand fast apply only through parity gates.**
   - Promote one model family at a time.
   - Require create, update, delete, validation failure, row issue,
     dependency, object-change, Branching, support-statistics, and runtime
     non-regression parity before enabling a model beyond the current safe set.
3. **Keep scheduler overlap evidence-gated.**
   - Overlap is now implemented as an opt-in path that pre-stages at most one
     eligible shard while the current shard is already queued for merge.
   - Keep overlap disabled by default until repeated completed evidence proves
     queue or merge wait dominates after worker count, query-fetch concurrency,
     page size, PostgreSQL, and disk placement are already tuned.
   - The implementation must remain bounded by dependency order, branch budget,
     and ledger ownership. It must never become a side queue.
4. **Harden delete-heavy filtered syncs.**
   - Keep delete dependency planning visible before merge.
   - Treat delete waves, reference blockers, and filtered-source prune runs as
     first-class plan risks, not ordinary source-row volume.
5. **Keep model rules centralized.**
   - New per-model behavior should enter through the model contract registry
     unless there is a documented temporary exception.
   - Future call-site cleanup should remove duplicate fetch/apply/delete/health
     rule reads only when tests preserve behavior.
6. **Keep NetBox 4.5 and 4.6+ unified.**
   - New NetBox 4.6+ features should be runtime capability gates on the same
     branch.
   - Future TurboBulk or native bulk capability should be proven behind the
     apply-engine boundary with the same support-bundle evidence shape.
7. **Make self-service troubleshooting the release bar.**
   - A support bundle plus Sync Health should be enough to answer:
     - whether the run is alive.
     - what shard/model is active.
     - whether diffs are active.
     - whether fallback is high.
     - whether row failures are isolated.
     - whether a recovery command is safe.
     - whether capacity tuning or backend choice is the next operator action.

Work that should stay deferred unless evidence changes:

1. Durable row storage outside Forward/NetBox evidence.
2. Python-side replacement for NQE normalization.
3. Unbounded parallel branch mutation.
4. A separate non-native import product.
5. Broad module splitting without a behavior-preserving test boundary.
6. Raising branch budgets to hide fetch, delete-density, or merge-cost
   problems.

Current closure criteria:

1. `architecture-completion-audit` should remain green except for gates that
   explicitly require live/runtime evidence.
2. A completed large-run benchmark or sanitized support bundle should close:
   - runtime fallback reduction evidence.
   - scheduler overlap readiness evidence.
3. The large-run artifact should show no hidden incomplete-step completion, no
   unresolved stale job ownership issue, no unbounded fallback split storm, and
   row failures that are bounded, visible, and model-local.
4. Docs should tell operators when to use Fast bootstrap, when to switch to
   Branching, when API diffs are active, and how to export the one-button
   support bundle before escalation.

## What The Prior Architecture Missed

The 0.8 and 0.9 refactors solved resumability, visibility, and native workflow
alignment. They did not completely remove large-run cost.

Remaining cost centers:

1. Forward query cost can still dominate when shard pushdown falls back to
   full/model fetch.
2. Adapter apply cost can still dominate for models that are not safe for
   `bulk_orm`.
3. Native Branching merge/diff cost still exists; the plugin can shard,
   recover, and explain it, but cannot make native branch merge free.
4. Scheduler execution is intentionally conservative. Bounded overlap should be
   added only when support evidence proves queue or merge wait pressure and
   database/worker headroom.
5. Capacity and tuning guidance needs to stay operator-facing so field users
   can self-diagnose without sending screenshots first.

## Long-Term Architecture Position

The current direction is still the right long-term architecture. We should not
start another broad rewrite or add a second sync product. The durable path is to
make the existing native NetBox workflow cheaper, stricter, and more
self-service:

1. Keep one workflow surface:
   - NQE defines normalized rows.
   - NetBox adapters/apply engines perform native mutations.
   - Branching remains the reviewable steady-state path.
   - Fast bootstrap remains the explicit trusted-baseline path.
2. Keep the execution ledger as the control plane:
   - every fetch, stage, merge, retry, skip, failure, and recovery transition
     should be represented as ledger state.
   - old compatibility payloads should stay upgrade/read-through only, not an
     active runtime control surface.
3. Keep speed improvements under explicit boundaries:
   - fetch acceleration belongs under shard-scoped NQE/query fetch.
   - write acceleration belongs under the apply-engine boundary.
   - future TurboBulk/parquet/native bulk behavior belongs under the same
     apply-engine capability gate.
4. Keep Branching-native safety visible:
   - branch budget is guidance, not a blind row cap.
   - delete-heavy and high-density models need conservative budget shaping.
   - merge and run progress must be observable from Sync Health and support
     bundles.
5. Treat field-scale evidence as a release gate:
   - local unit/synthetic tests prove invariants.
   - a large sanitized support bundle or approved local field-scale run proves
     fallback, scheduler, row-failure, and completion behavior.

## Long-Term Completion Position

The architecture should be considered mostly converged, but not fully closed.
The remaining work is not another rewrite. It is the set of controls that make
large syncs predictable enough for field use without direct developer
supervision.

What is in the right shape now:

1. Ledger-first orchestration is the durable control plane.
2. NQE remains the only row normalization and filtering source of truth.
3. Branching remains the native reviewable steady-state path.
4. Fast bootstrap remains the explicit trusted-baseline path for very large
   first loads.
5. Shard-scoped fetch, fallback reason codes, partition retries, density
   learning, row failures, support bundles, and Sync Health all hang off the
   same execution model.
6. Faster write engines are behind the apply-engine boundary instead of being
   exposed as a separate operator workflow.

What still has to be proven or hardened before we call the architecture fully
settled:

1. Complete a current large-run artifact where ledger run state, step state,
   baseline readiness, and benchmark completion all agree.
2. Preserve runtime capacity settings through evidence and chaos workflows so
   capacity review reflects the tuned environment, not the compose default.
3. Prove repeated fallback is low or explicitly explainable on a real
   field-scale dataset.
4. Prove whether scheduler overlap is actually needed after worker count,
   query-fetch concurrency, page size, Postgres, and disk placement are tuned.
5. Expand `bulk_orm` only after model-specific parity tests prove adapter
   equivalence.
6. Keep NetBox 4.5 and 4.6+ on one capability-gated branch.
7. Keep future TurboBulk/parquet/native bulk work under the apply-engine
   boundary.

The most important current architectural hardening item was runtime capacity
preservation. Current evidence showed that `docker compose up` and chaos
restore paths could reset `netbox-worker` back to the compose default replica
count. That made local evidence misleading: the system may have been tuned, but
the evidence workflow could silently test a smaller worker pool. Runtime
evidence now accepts `--capacity-worker-replicas`, scales workers before probes,
and preserves that count through chaos setup and restore. This remains
architecture work because release evidence must measure the intended runtime
profile.

The second priority is to close the field-scale evidence loop. We should not
relax audit thresholds to make the plan look complete. The correct closure is a
current run or sanitized support bundle that proves:

- run completion is consistent.
- row failures are bounded and visible.
- fallback count and fallback runtime share are acceptable.
- partition retry pressure is acceptable.
- diff utilization is working when query IDs are used.
- scheduler overlap is either unnecessary or justified by capacity-backed
  evidence.

The third priority is speed economics inside the existing architecture:

- reduce fallback before adding scheduler complexity.
- tune workers, query concurrency, page size, Postgres, and disk placement
  before introducing overlap.
- promote more `bulk_orm` models only through parity gates.
- treat future NetBox/TurboBulk capability as another apply engine, not a new
  product path.

Do not spend time on:

- a second non-native sync workflow.
- durable row storage outside NetBox/Forward evidence.
- Python-side row normalization that should live in NQE.
- broad module splitting without a behavior-preserving test boundary.
- widening branch budgets to hide fallback or delete-density problems.

## Architectural Hardening Still Worth Doing

These are the remaining long-term items that make sense after the 0.8/0.9
refactors. They are hardening and economics work, not a new architecture.

### P0: Ledger Completion Invariants

Status: `completed_current_baseline`

Objective:
- Make it impossible for a run to be treated as successfully completed while
  stage steps remain `pending`, `queued`, or `running`.

Why:
- A historical local large-run benchmark found a run marked completed while many
  steps were still non-terminal. That evidence is now rejected by the benchmark
  gate, but the runtime invariant should also be enforced at the ledger layer.

Next work:
1. Keep benchmark/support-bundle checks as an external guard so older bad runs
   remain detectable.
2. Add new regression cases when future recovery or merge transitions can alter
   run completion.

Implemented now:
- `mark_run_completed` refuses to complete runs with incomplete stage steps.
- final-index merge completion now requires all planned stage steps to be
  successfully terminal.
- out-of-order final shard merges point the run back at the earliest incomplete
  stage instead of completing the run.
- final-ingestion baseline readiness now uses the same completion-safe
  condition.
- reconciliation can reopen historical completed runs with incomplete stage
  steps, clear baseline readiness, point back at the incomplete shard, and
  record a `completed_run_reopened` event.
- `forward_scale_benchmark --reconcile` and
  `invoke architecture-runtime-evidence --scale-reconcile` expose that repair
  path for live run selectors before benchmark export.
- `invoke runtime-capacity-review` produces a read-only host/worker/source
  capacity artifact, and `architecture-runtime-evidence` can include it via
  `--capacity-source-name`.
- local Docker compose mounts a git-ignored runtime fetch-artifact scratch
  directory into NetBox and workers and raises the local artifact cap to
  `512 MiB`. This keeps the model-fallback reuse optimization effective during
  large local runs without changing production defaults or storing row payloads
  in committed artifacts.
- local Docker compose can disable worker autoreload with
  `FORWARD_NETBOX_WORKER_AUTORELOAD=0`, which keeps large ingestion tests from
  restarting active RQ workers during ordinary source edits while preserving the
  autoreload default for normal development.
- newly created syncs now enable the parity-tested safe `bulk_orm` model set by
  default through the shared sync normalization contract. Existing stored syncs
  preserve their saved setting, and adapter-required models remain on the
  adapter path.
- live smoke syncs now use the same safe `bulk_orm` default and expose an
  explicit adapter-only comparison flag.
- scale benchmark evidence now warns when a large run reports only adapter apply
  engines. This prevents adapter-only evidence from being mistaken for proof of
  the optimized ingestion path.
- regression coverage verifies that an out-of-order final shard merge does not
  complete the run and does not mark baseline readiness.

Completion signal:
- No current code path can mark a multi-step run complete while non-terminal
  steps remain, and the scale benchmark reports `run_completion=pass` for new
  large runs.

### P0: Release-Grade Scale Evidence

Status: `in_progress`

Objective:
- Make large-run evidence repeatable enough that release readiness does not
  depend on screenshots or operator memory.

Next work:
1. Use `forward_scale_benchmark` against either a local run ID or a sanitized
   support bundle.
2. Require these checks before claiming a scale release is green:
   - support bundle shape
   - run completion consistency
   - row-failure rate
   - fallback rate/runtime share
   - diff utilization
   - partition retry pressure
   - scheduler overlap readiness
3. Keep sensitive-content scanning in the offline support-bundle path.

Completion signal:
- Release notes can cite a current benchmark artifact without including private
  row data, network IDs, snapshot IDs, credentials, or screenshots.

### P0: Runtime Fallback Reduction

Status: `in_progress`

Objective:
- Reduce full/model fetch fallback without weakening NQE-as-source-of-truth.

Next work:
1. Use benchmark/support evidence to identify repeated fallback by model and
   reason.
2. Fix fallback at the correct layer:
   - query contract when the filter shape is wrong.
   - Forward query/runtime handling when API behavior is the cause.
   - local safety filtering only when it preserves NQE row semantics.
3. Keep full/model fallback available with explicit reason codes.

Completion signal:
- Repeated large runs show low fallback counts, or remaining fallback reasons
  are explicit, bounded, and actionable.

### P1: Apply-Engine Expansion

Status: `in_progress`

Objective:
- Expand `bulk_orm` only where parity is proven.

Next work:
1. Use the existing `bulk_orm_expansion.parity_plan` to select candidates.
2. Add model-specific create/update/delete/failure/statistics parity tests.
3. Promote models only after adapter parity and runtime non-regression are
   demonstrated.

Completion signal:
- More high-volume models use faster writes without losing Branching behavior,
  validation behavior, object-change tracking, row issue capture, or support
  statistics.

### P1: Evidence-Gated Scheduler Overlap

Status: `implemented_opt_in_pending_terminal_evidence`

Objective:
- Reduce idle time only if evidence shows scheduler or merge queue wait is the
  bottleneck.

Next work:
1. Keep collecting throughput and wait-share evidence from support bundles.
2. Tune capacity and query concurrency first.
3. Add a bounded ledger-derived scheduler window only if repeated evidence
   shows queue/merge wait dominates and the database/worker pool has headroom.

Completion signal:
- Tail runtime improves without duplicate stage claims, dependency-order
  violations, merge regressions, or ambiguous ledger state.

### P1: Capability-Gated NetBox Version Acceleration

Status: `planned`

Objective:
- Use NetBox 4.6+ and future bulk capabilities without creating divergent
  release branches.

Next work:
1. Keep 4.5 and 4.6+ on one branch.
2. Add runtime capability probes where newer NetBox features can accelerate a
   model safely.
3. Route those capabilities through existing model contracts and apply-engine
   decisions.

Completion signal:
- New NetBox capabilities can be enabled per environment while preserving the
  same NQE maps, sync configuration, tests, docs, and support-bundle shape.

## Completed Current Baseline

### Run-Local Fetch Artifacts

Status: `completed_current_baseline`

- Runtime-only artifact reuse exists for shard-scoped retry/resume paths.
- Artifacts are keyed by run, model, query identity, snapshot/baseline, fetch
  mode, fetch params, shard keys, and tag scope hashes.
- Branching shard stage jobs now also reuse a run-local query-context artifact
  (snapshot info, snapshot metrics, and resolved device-tag scope) keyed by
  snapshot and tag selectors, so each shard does not re-run the same context
  API resolution work.
- Query-path resolution now also reuses run-local artifacts for resolved
  repository references (`query_path -> query_id/commit`) so shard jobs do not
  repeatedly resolve the same repository query reference within one run.
- Device-tag scope resolution now executes a single filtered query (instead of
  an additional full-device counting query used only for logging), cutting
  startup query load while preserving filtering semantics.
- IP/routing diagnostic queries are now scoped to the models present in the
  current fetched workload result set, preventing repeated diagnostic query
  execution on unrelated shard stages where those models are merely enabled.
- Source-level `query_diagnostics_enabled` now allows operators to disable
  diagnostic query passes for throughput-focused large runs while keeping
  default diagnostic visibility enabled.
- Row payloads are not persisted in support bundles.
- Completed/failed run cleanup prunes artifacts.

### Delete And Dependency Planning

Status: `completed_current_baseline`

- Plan previews expose `delete_dependency_plan`.
- Delete-heavy workloads include dependency order, delete shard counts, delete
  share, max delete shard size, and reference-blocker risk.
- Warnings now distinguish delete waves, near-budget delete shards, and
  dependency-anchor risk.

### Shard Fetch And Fallback Visibility

Status: `completed_current_baseline_with_runtime_evidence_remaining`

- Supported models have explicit fetch contracts.
- Full and diff shard fetch paths support partition splitting and alternate
  single-value operator retry before broader fallback.
- Support bundles and Sync Health aggregate fallback reason summaries and
  partition retry summaries.

### Recovery And Ledger Behavior

Status: `completed_current_baseline`

- Stale queued/running stage states can be reset or requeued when no branch is
  associated.
- Orphaned queued stage states with no job, branch, or ingestion reset to
  `pending` during reconciliation so the native enqueue path can safely resume
  the shard instead of waiting forever.
- Long-running live NetBox stage jobs are no longer failed solely because the
  execution-step heartbeat is quiet. If an older reconciliation pass already
  marked such a step failed while its job is still live, reconciliation restores
  it to `running` and records `failed_stage_with_live_job_auto_restore`.
- Branch-associated stale states escalate to explicit manual intervention.
- Auto-merge timeout recovery has bounded requeue.
- No-progress watchdog evidence is included in support bundles.

## Remaining Architecture Work

### P0: Runtime Fallback Reduction

Status: `in_progress`

Objective:
- Reduce repeated full/model fallback after shard pushdown is attempted.

Next work:
1. Use support bundles and trend exports to identify fallback reasons by model,
   fetch mode, and runtime cost.
2. Fix repeated fallback at the safest layer:
   - NQE/query contract when row shape or filter semantics are the issue.
   - Forward query/runtime handling when API execution is the issue.
   - local safety filter only when it preserves NQE row semantics.
3. Keep full/model fallback available with explicit reason codes.

Completion signal:
- Repeated large runs show low fallback counts, or residual fallback causes are
  explainable and visible in support bundles.

Do not:
- Hide fallback pressure by widening branch budgets.
- Add Python-side normalization that diverges from NQE.

### P1: Apply-Engine Expansion

Status: `in_progress`

Objective:
- Expand `bulk_orm` only where it is proven equivalent to the adapter path.

Next work:
1. Implement the candidate-specific tests listed by
   `bulk_orm_expansion.parity_plan`.
2. Require these gates before enabling a model:
   - create parity
   - update parity
   - delete parity
   - validation failure parity
   - row issue parity
   - dependency behavior parity
   - object-change tracking parity
   - Branching behavior parity
   - support-bundle statistics parity
   - runtime non-regression
3. Keep adapter fallback automatic and visible.

Implemented baseline:
- `bulk_orm_expansion.parity_plan` now exposes the next candidate models,
  source of recommendation, promotion lane, blocker/risk metadata,
  lane-specific gate, generic parity checklist, and candidate-specific test
  IDs.
- Sync Health and architecture audit consume the same parity-plan payload.
- The health UI shows the next parity target without enabling any additional
  model.
- `dcim.virtualchassis` has moved into the safe `bulk_orm` set after focused
  parity tests for create, update, delete fallback, guarded validation failure,
  row issue capture, dependency skip, support statistics, and runtime
  non-regression.
- the next candidate is `dcim.device`; guard tests now keep it on the adapter
  engine until dependency-resolution parity is proven.

Completion signal:
- Additional models move to `bulk_orm` only after parity tests and runtime
  evidence prove equal or better behavior.

Do not:
- Promote relationship-heavy models without proving NetBox validation,
  Branching behavior, object-change tracking, and row-level issue parity.
- Create a separate bulk-only sync workflow.

### P1: Execution Throughput Smoothing

Status: `implemented_opt_in_pending_terminal_evidence`

Objective:
- Reduce idle time between fetch, stage, and merge work without violating
  dependency order or branch-change guardrails.

Next work:
1. Use `throughput_smoothing`, `large_run_tuning`, and
   `operator_tuning_summary` from a completed large run to determine whether
   the opt-in overlap path should become a recommended tuning action.
2. Tune supported knobs first:
   - query fetch concurrency
   - worker count
   - worker timeout
   - Postgres capacity
   - disk/container placement
3. Keep the bounded ledger-derived scheduler window opt-in until repeated
   evidence shows queue/merge wait dominates and capacity exists.

Completion signal:
- Large-run tail latency improves without duplicate stage claims, increased row
  failures, merge regressions, or ambiguous support-bundle state.

Do not:
- Add side queues outside the execution ledger.
- Run unbounded concurrent branch mutations for the same dependency chain.

### P1: Capacity Profile Calibration

Status: `in_progress`

Objective:
- Make large-ingest performance self-service from NetBox surfaces.

Next work:
1. Calibrate small/medium/large/very-large guidance from repeated local and
   field runs.
2. Keep docs aligned with Sync Health signals:
   - diff utilization
   - fallback rate and fallback runtime share
   - query fetch concurrency
   - queue/merge wait share
   - density confidence
   - delete-wave risk
3. Keep support bundle export as the preferred field diagnostic artifact.

Completion signal:
- Operators can decide whether to restore diffs, reduce fallback, add capacity,
  use fast bootstrap, tune query concurrency, or stay on Branching without
  shell access.

### P2: Future Bulk Engines

Status: `planned`

Objective:
- Prepare for NetBox/TurboBulk/parquet/native bulk primitives without splitting
  the product workflow.

Next work:
1. Treat every faster write mechanism as an apply engine behind the existing
   boundary.
2. Reuse the same model contract registry, validation, row issue, ledger, and
   support-bundle surfaces.
3. Capability-gate future NetBox features at runtime and in CI.

Completion signal:
- A faster engine can be enabled or disabled without changing NQE contracts,
  sync definitions, or operator workflow.

Do not:
- Fork long-lived NetBox-version-specific behavior when capability gates and
  tests can keep one branch.

## Recommended Execution Order

1. Add apply-engine parity-plan output for candidate models without enabling new
   `bulk_orm` models yet.
2. Use live support bundles to choose the next runtime fallback fix.
3. Re-run large-ingest regression and inspect fallback, retry, delete-wave, and
   throughput metrics.
4. Implement scheduler overlap only if repeated evidence shows it is the real
   bottleneck after capacity and query tuning.
5. Keep future NetBox/TurboBulk work under the apply-engine boundary.

## Validation

Planning-only changes:

```bash
git diff --check -- docs/03_Plans/active/2026-05-24-long-term-speed-architecture-work.md
poetry run invoke harness-check
```

Runtime implementation tranches should add focused Django tests for the touched
behavior and then run:

```bash
poetry run invoke harness-check
poetry run invoke architecture-audit-check
poetry run invoke check
poetry run invoke test
```

Release candidates should also verify:

```bash
poetry run invoke docs
poetry run invoke ci
```

Completion audit expectations:

- `bulk_orm_parity_plan_present` should stay completed.
- `bulk_orm_candidate_parity_tests_complete` should stay completed for the
  first candidate while the model remains adapter-backed.
- `field_scale_runtime_matrix_verified` is now completed by the approved live
  smoke matrix artifact.
- `runtime_fallback_reduction_evidence_verified` should remain
  `needs_external_evidence` until repeated large-run support bundles prove low
  or explainable fallback.
- `scheduler_overlap_readiness_evidence_verified` should remain
  `needs_external_evidence` until repeated large-run support bundles prove
  whether scheduler overlap is warranted.

Current evidence snapshot:

- `poetry run invoke architecture-completion-audit` currently reports
  `14` completed, `0` failed, and `2` external/runtime evidence gaps.
- `docs/03_Plans/evidence/architecture-runtime-evidence.json` now includes
  scale-benchmark-derived checks for runtime fallback reduction and scheduler
  overlap readiness.
- `docs/03_Plans/evidence/runtime-capacity-review.json` records the local
  worker count, host CPU/memory, recommended PostgreSQL settings, and optional
  source fetch settings used for scheduler-overlap capacity review.
- `docs/03_Plans/evidence/execution-run-119-recovery.json` records a
  sanitized execution-run recovery snapshot. It includes run/step status,
  counts, recovery recommendation, and job IDs only; it intentionally omits
  shard keys, filter values, query IDs, snapshot IDs, and row payloads.
- Runtime evidence accepts `--capacity-worker-replicas <count>` and preserves
  that worker count through chaos setup/restore so capacity review measures the
  tuned local profile instead of the compose default.
- Runtime evidence accepts `--capacity-query-fetch-concurrency <count>` and
  `--capacity-nqe-page-size <count>` with `--capacity-source-name` so harness
  seed/reset does not erase source fetch tuning before capacity review.
- Latest refreshed local evidence recorded `4` active `netbox-worker` replicas
  after the runtime-evidence chaos probes, with capacity review status `pass`.
- Latest refreshed local evidence also recorded source
  `query_fetch_concurrency=6`, `nqe_page_size=10000`, and `timeout=1200`
  after harness seed/reset, with `capacity_source_tuning_applied=true`.
- Latest non-destructive runtime evidence refresh was written at
  `2026-05-24T12:01:19Z` using `--skip-chaos --scale-run-id 119` and the
  local source capacity knobs. This preserved source tuning evidence while a
  live ingestion stayed active.
- `docs/03_Plans/evidence/scale-runtime-evidence.json` now points at local
  execution run `119`, which has `166` steps and reached shard `81`.
  This is stronger diagnostic evidence than the older reopened run, but it is
  still not completion evidence because the run remains `running`.
- Shard `74/166` completed native Branching merge after staging cleanly with
  `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries.
- Shard `75/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- Shard `76/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- Shard `77/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- Shard `78/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- Shard `79/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9794` attempted rows, `9794` applied rows, `9794` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- Shard `80/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9794` attempted rows, `9794` applied rows, `9794` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- Shard `81/166` is now running stage with `nqe_column_filter`, one column
  filter, and estimated `9794` changes.
- Run `119` exposed and validated an additional recovery invariant: a stage
  step can be left `queued` without a queued job after stale no-branch recovery.
  The runtime now resets that orphaned queued state to `pending`, recovery
  recommendations call for `reconcile` rather than `wait`, and the new
  `execution-run-recovery` task can enqueue the next shard through native
  NetBox job handling. The local run was resumed through that path, shard
  `46/166` merged with `9295` attempted rows, `9295` applied rows, `9295`
  actual changes, and `0` row failures; shard `47/166` then staged and merged
  with the same clean row counts; shard `48/166` staged and merged with `9294`
  attempted rows, `9294` applied rows, `9294` actual changes, and `0` row
  failures; shard `49/166` staged and merged with `9502` attempted rows,
  `9502` applied rows, `1265` actual changes, and `0` row failures; shard
  `50/166` staged and merged with `9502` attempted rows, `9502` applied rows,
  `1289` actual changes, and `0` row failures; shard `51/166` then merged
  cleanly in the final live check; shard `52/166` staged and merged with
  `9502` attempted rows, `9502` applied rows, `1317` actual changes, and `0`
  row failures; shard `53/166` staged and merged with `9502` attempted rows,
  `9502` applied rows, `1163` actual changes, and `0` row failures.
- Run `119` also exposed a live speed/resume issue in the `ipam.prefix` shard:
  repeated non-retryable Forward HTTP 400 partition failures were recursively
  split and logged before fallback. The fetch path now treats those failures as
  non-retryable and falls back once through the existing full/model fallback
  path, while preserving split retries for transient/timeout failures.
- The same retry caused a deterministic re-plan to split the claimed persisted
  shard into smaller candidate items. Resume selection now recombines subset
  candidates under the persisted ledger shard boundary, so the claimed shard can
  continue instead of failing with an unresolved index.
- The fix carried past the recovered shard and exposed another recovery
  invariant: stale stage jobs must be shard-owned. A late job for an older
  claimed shard can finish after the run has advanced; that job must not mark
  the current shard failed. Stage-job exception handling now records failures
  only against the claimed shard when it is still active and owned by that job.
  If the claimed shard is already staged, merge-queued, merged, skipped,
  cancelled, owned by a different job, or behind the run pointer, the job exits
  as stale and preserves the active execution run.
- The stale-claimed-job path is covered by
  `ForwardJobsTest.test_stage_forward_branch_item_stale_claim_failure_does_not_fail_current_step`.
  After the fix, `invoke execution-run-recovery --run-id=119 --enqueue-next`
  resumed run `119` through the native NetBox queue. Shard `54/166` requeued as
  job `539`, merged cleanly, and advanced to shard `55/166`. The refreshed
  evidence showed `54` merged shards, `1` running shard, `111` pending shards,
  and `0` failed rows.
- Shard `55/166` then staged and merged cleanly with `9502` attempted rows,
  `9502` applied rows, `1281` actual changes, and `0` row failures. Shard
  `56/166` staged cleanly and reached `merge_queued` with `9502` attempted
  rows, `9502` applied rows, and `0` row failures.
- Shard `56/166` then merged cleanly with `9502` attempted rows, `9502`
  applied rows, `1406` actual changes, and `0` row failures. The latest
  non-terminal evidence has shard `57/166` running with no failed execution
  steps.
- Shard `57/166` staged cleanly and entered merge with `9501` attempted rows,
  `9501` applied rows, `1365` actual changes, and `0` row failures. After it
  merged, the run advanced into the next `ipam.prefix` section at shard
  `58/166`. The active job snapshot showed no prefix retry/fallback warnings
  yet and no failed execution steps.
- Shard `58/166` then staged cleanly through the prefix path with `9795`
  attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is currently `merge_queued`, which proves
  the first prefix shard cleared stage/apply without reproducing the earlier
  prefix retry storm or unresolved-shard failure.
- Shard `58/166` then completed the Branching merge cleanly. Native merge logs
  showed `5000/9795` and `9795/9795` progress, followed by `Merge completed:
  9795 applied, no failed`, then `merge_queued -> merged`. This verifies sparse
  merge heartbeat/logging on a large prefix merge and moved the run to shard
  `59/166`.
- Shard `59/166` then staged cleanly through the same prefix path with `9795`
  attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is currently `merge_queued`; the merge job
  is running and had not emitted progress logs yet at the evidence refresh.
- Shard `59/166` then completed the Branching merge cleanly. Native merge logs
  showed `5000/9795` and `9795/9795` progress, followed by `Merge completed:
  9795 applied, no failed`, then `merge_queued -> merged`. This gives a second
  consecutive large prefix shard with clean stage/apply/merge behavior and
  moved the run to shard `60/166`.
- Shard `60/166` then staged cleanly through the same prefix path with `9795`
  attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is currently `merge_queued`, giving a
  third consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- Shard `60/166` then completed the Branching merge cleanly and the run
  advanced to shard `61/166`.
- Shard `61/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is currently `merge_queued`, giving a
  fourth consecutive prefix shard that cleared stage/apply without retry
  storms, unresolved-shard failures, or row failures.
- Shard `61/166` then completed the Branching merge cleanly and the run
  advanced to shard `62/166`. The latest sanitized recovery snapshot shows
  shard `62/166` actively staging through the same `ipam.prefix` path with no
  row failures or last error. This keeps the prefix path moving through native
  stage and merge without reopening the earlier unresolved-shard issue.
- Shard `62/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is currently `merge_queued`, giving a
  fifth consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- Shard `62/166` then completed the Branching merge cleanly and the run
  advanced to shard `63/166`. The latest sanitized recovery snapshot shows
  shard `63/166` actively staging through the same `ipam.prefix` path with no
  row failures or last error.
- Shard `63/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is currently `merge_queued`, giving a
  sixth consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- Shard `63/166` then completed the Branching merge cleanly and the run
  advanced to shard `64/166`. The latest sanitized recovery snapshot shows
  shard `64/166` actively staging through the same `ipam.prefix` path with no
  row failures or last error.
- Shard `64/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is currently `merge_queued`, giving a
  seventh consecutive prefix shard that cleared stage/apply without retry
  storms, unresolved-shard failures, or row failures.
- Shard `64/166` then completed the Branching merge cleanly and the run
  advanced to shard `65/166`. The latest sanitized recovery snapshot shows
  shard `65/166` actively staging through the same `ipam.prefix` path with no
  row failures or last error.
- Shard `65/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is currently `merge_queued`, giving an
  eighth consecutive prefix shard that cleared stage/apply without retry
  storms, unresolved-shard failures, or row failures.
- Shard `65/166` then completed the Branching merge cleanly and the run
  advanced to shard `66/166`. The latest sanitized recovery snapshot shows
  shard `66/166` actively staging through the same `ipam.prefix` path with no
  row failures or last error.
- Shard `66/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is currently `merge_queued`, giving a
  ninth consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- Shard `66/166` then completed the Branching merge cleanly and the run
  advanced to shard `67/166`. The latest sanitized recovery snapshot shows
  shard `67/166` actively staging through the same `ipam.prefix` path with no
  row failures or last error.
- Shard `67/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is currently `merge_queued`, giving a
  tenth consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- Shard `67/166` then completed the Branching merge cleanly and the run
  advanced to shard `68/166`. The latest sanitized recovery snapshot shows
  shard `68/166` actively staging through the same `ipam.prefix` path with
  `nqe_column_filter`, one column filter, estimated `9795` changes, `9795`
  attempted rows, `9795` applied rows, `9795` actual changes, `0` row failures,
  and `0` step retries. It is currently `merge_queued`, giving an eleventh
  consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- Shard `68/166` then completed the Branching merge cleanly and the run
  advanced to shard `69/166`. The latest sanitized recovery snapshot shows
  shard `69/166` staged cleanly through the same `ipam.prefix` path with
  `nqe_column_filter`, one column filter, `9795` attempted rows, `9795` applied
  rows, `9795` actual changes, `0` row failures, and `0` step retries. It is
  currently `merge_queued`, giving a twelfth consecutive prefix shard that
  cleared stage/apply without retry storms, unresolved-shard failures, or row
  failures.
- Shard `69/166` then completed the Branching merge cleanly and the run
  advanced to shard `70/166`. The latest sanitized recovery snapshot shows
  shard `70/166` staged cleanly through the same `ipam.prefix` path with
  `nqe_column_filter`, one column filter, `9795` attempted rows, `9795` applied
  rows, `9795` actual changes, `0` row failures, and `0` step retries. It is
  currently `merge_queued`, giving a thirteenth consecutive prefix shard that
  cleared stage/apply without retry storms, unresolved-shard failures, or row
  failures.
- Shard `70/166` then completed the Branching merge cleanly and the run
  advanced to shard `71/166`. The latest sanitized recovery snapshot shows
  shard `71/166` staged cleanly through the same `ipam.prefix` path with
  `nqe_column_filter`, one column filter, `9795` attempted rows, `9795` applied
  rows, `9795` actual changes, `0` row failures, and `0` step retries. It is
  currently `merge_queued`, giving a fourteenth consecutive prefix shard that
  cleared stage/apply without retry storms, unresolved-shard failures, or row
  failures.
- Shard `71/166` then completed the Branching merge cleanly and the run
  advanced to shard `72/166`. The latest sanitized recovery snapshot shows
  shard `72/166` staged cleanly through the same `ipam.prefix` path with
  `nqe_column_filter`, one column filter, `9795` attempted rows, `9795` applied
  rows, `9795` actual changes, `0` row failures, and `0` step retries. It is
  currently `merge_queued`, giving a fifteenth consecutive prefix shard that
  cleared stage/apply without retry storms, unresolved-shard failures, or row
  failures.
- Shard `72/166` then completed the Branching merge cleanly and the run
  advanced to shard `73/166`. The latest sanitized recovery snapshot shows
  shard `73/166` staged cleanly through the same `ipam.prefix` path with
  `nqe_column_filter`, one column filter, `9795` attempted rows, `9795` applied
  rows, `9795` actual changes, `0` row failures, and `0` step retries. It is
  currently `merge_queued`, giving a sixteenth consecutive prefix shard that
  cleared stage/apply without retry storms, unresolved-shard failures, or row
  failures.
- Shard `73/166` then completed the Branching merge cleanly and the run
  advanced to shard `74/166`. Shards `74/166` through `78/166` then staged and
  merged cleanly through the same `ipam.prefix` path with `nqe_column_filter`,
  one column filter, `0` row failures, and `0` step retries. The latest
  sanitized recovery snapshot shows shards `74/166` through `80/166` staged
  and merged cleanly. Shard `81/166` is now actively staging with estimated
  `9794` changes, no run-level error, no row failures, and recovery
  recommendation `wait` because the active stage job is live.
- The merge path now has a sparse native progress hook for long Branching
  merges: the existing per-change merge loop periodically refreshes execution
  ledger heartbeat and emits large-interval NetBox job-log progress. This makes
  long merges diagnosable without changing Branching semantics, creating side
  queues, or widening branch budgets.
  Live shard `48/166` verified the hook by emitting merge progress at
  `5000/9294` and `9294/9294` before completing.
- The task harness was hardened so unit tests no longer overwrite the canonical
  `docs/03_Plans/evidence/scale-runtime-evidence.json` artifact with synthetic
  benchmark data. Tests that need fake scale reports now use temporary roots or
  restore the previous evidence file.
- Runtime evidence now supports a non-destructive refresh path:
  `invoke architecture-runtime-evidence --skip-chaos --scale-run-id <run-id>`.
  This reuses fresh prior chaos evidence while refreshing scale/capacity
  evidence. In skip-chaos mode the task does not run `docker compose up`,
  reseed the UI harness, scale workers, or run worker-kill probes, which is the
  correct path when a long live ingestion is active.
- Field-scale smoke evidence now records per-step timeout failures instead of
  dropping the artifact, so intentionally short diagnostic timeouts remain
  diagnosable without overwriting passed field-scale evidence.
- Field-scale smoke evidence also writes incremental sanitized step evidence to
  `docs/03_Plans/evidence/field-scale-runtime-matrix.json` so an interrupted
  or long-running matrix leaves the latest completed step state.
  `invoke field-scale-runtime-matrix --resume=True` can run the matrix
  independently from the heavier architecture evidence task, and `--step` can
  run one long step at a time.
  The main runtime evidence now references that artifact via
  `field_scale_runtime_matrix_verified.evidence.artifact_path`.
  Runtime evidence also reuses this artifact when `--run-field-scale` is not
  provided, provided the artifact is fresh and passed.
  Latest refreshed runtime evidence reused the existing artifact and recorded
  `field_scale_status=artifact-passed`, closing the field-scale matrix gate.
  The approved smoke matrix durations were approximately 16 seconds for
  Branching validate-only, 180 seconds for Branching plan-only, and 17 seconds
  for fast-bootstrap validate-only.
- The larger local report found fallback below warning thresholds, fallback
  runtime share around `0.5%`, no row failures across `532592` attempted rows,
  no partition retry pressure, and scheduler overlap status
  `candidate_after_capacity_review` with high queue wait share. Those signals
  are useful but cannot close the fallback/scheduler gates until a completed
  large-run artifact is available.
- The next evidence pass should be a larger approved run or exported support
  bundle that proves fallback pressure is low/explainable and confirms whether
  scheduler overlap is actually warranted, with run completion metadata matching
  the step states.
- The parity-tested `bulk_orm` safe set is now exposed as a native sync-form
  performance option instead of requiring manual parameter edits. This keeps the
  adapter-required model contract intact while making the fastest safe apply
  path self-service for baseline and regression tests.
- Safe `bulk_orm` is now also auto-enabled whenever a sync has no explicit
  `enable_bulk_orm` override, for both Branching and fast bootstrap. Explicit
  opt-out (`enable_bulk_orm=false`) still forces adapter-only behavior. This
  removes legacy unset-state drift that left parity-tested speed on the table
  for older syncs while preserving contract-safe rollback.
- Local Django test tasks now guard against running against the shared Docker
  NetBox runtime while a Forward execution run is queued, running, or waiting.
  This prevents the test suite from disturbing live RQ job state during long
  ingestion proof runs; intentional shared-runtime test runs require
  `FORWARD_NETBOX_ALLOW_SHARED_RUNTIME_TESTS=1`.
- Branch row-budget shaping can now pack more rows only when model density
  learning has high confidence that rows produce materially fewer NetBox
  changes than the branch budget. Delete-heavy work remains capped
  conservatively. This treats the configured branch value as the intended
  change budget, not as a hard row-count cap, while still preserving overflow
  retry protection when a branch actually exceeds the budget.
- Runtime capacity review now records local storage placement for Docker,
  Postgres, and fetch artifacts so field-scale evidence can distinguish code
  bottlenecks from local IO placement issues.
- Shard-scoped fetch fallback now has a run-local model-level artifact path.
  When a scoped column-filter fetch falls back to full model execution, the
  first shard stores the unfiltered full model result under the current
  run/snapshot/query/tag contract. Later shards can reuse that artifact and
  apply the normal local shard filter instead of rerunning the same full NQE.
  This preserves NQE as the source of truth and keeps fallback visible in
  fetch metadata.
- Full Django regression runs now have an isolated compose-project path via
  `invoke test-isolated`. This lets large ingestion testing continue in the
  primary runtime while full tests run against separate Postgres/Redis/RQ state,
  keeping validation available without disturbing active execution runs.
- The Docker build context is now pruned with `.dockerignore`. The first
  isolated test proof showed Docker sending a 621 MB context mostly from local
  logs, virtualenvs, generated docs, node modules, and build artifacts. Those
  paths are excluded so local rebuilds and isolated test bootstraps spend time
  on the plugin source instead of generated debris.
- Isolated full Django regression is proven against the separate
  `forward-netbox-test` compose project. After the Docker context pruning and
  density-cap fix, `invoke test-isolated` ran `654` tests successfully in about
  `51.5s` while the primary runtime still had active ingestion runs. This is the
  current fast, safe path for full regression during field-scale ingestion
  validation.

Field-scale evidence command shape:

```bash
poetry run invoke architecture-runtime-evidence \
  --sync-name ui-harness-sync \
  --scale-sync-name "$FORWARD_SMOKE_SYNC_NAME" \
  --run-field-scale

poetry run invoke architecture-completion-audit
```

Use `--sync-name` for the local synthetic sync used by chaos probes. Use
`--scale-sync-name` for the large NetBox sync whose execution-run support bundle
should feed `forward_scale_benchmark`. If both are omitted, the task defaults to
the local UI harness, which is useful for wiring checks but too small to close
the fallback or scheduler gates.

Offline evidence can use the same audit path:

```bash
poetry run invoke architecture-runtime-evidence \
  --sync-name ui-harness-sync \
  --scale-input-json /path/to/sanitized-support-bundle.json

poetry run invoke architecture-completion-audit
```

Use `--scale-run-id <execution-run-id>` instead when the large execution run
exists in the local NetBox database.

If the live run is historical and the benchmark reports completed-run
inconsistency, rerun with `--scale-reconcile` so the execution ledger is repaired
before benchmark export. Do not use this with `--scale-input-json`, because
offline support bundles are read-only evidence.

Offline support-bundle evidence is accepted only after the input passes the
same configured sensitive-content patterns used by the repo guard. Add
customer-local identifiers to `.sensitive-patterns.local.txt` before running
field evidence checks.

## Rollback

This file is a planning artifact and has no runtime effect. If it diverges from
the roadmap, refresh it from:

- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-remaining-refactors.md`
- `docs/03_Plans/active/2026-05-23-long-term-scale-architecture-direction.md`

Runtime workstreams remain independently reversible:

- fallback remediation must preserve full/model fallback.
- apply-engine expansion must preserve adapter fallback.
- scheduler smoothing must remain ledger-derived and disableable.
- future bulk engines must not alter NQE contracts or operator workflow.

## Decision Log

- The project should not re-architect into a second sync product.
- The correct direction is one native NetBox workflow with better engine
  economics underneath it.
- Runtime fallback reduction and apply-engine parity are the highest-value next
  architecture items.
- Scheduler overlap is valuable only if wait pressure is proven by support
  evidence.
- Future bulk capabilities should be capability-gated acceleration surfaces,
  not separate workflows.
