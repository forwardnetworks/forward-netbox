# 2026-05-23 Long-Term Scale Architecture Direction

## Goal

Define the durable architecture direction for very large Forward-to-NetBox
imports after the 0.8/0.9 scale refactors, with emphasis on speed, stability,
operator self-service, and native NetBox behavior.

## Constraints

- NQE remains the source of truth for model shape, normalization, coalescing,
  filtering, and row identity.
- NetBox-native model writes remain the only mutation path.
- Branching remains the native review path for steady-state sync.
- Fast bootstrap remains an explicit baseline path for large first imports.
- Faster apply engines must plug under the existing workflow rather than create
  a second sync product.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials,
  screenshots, or private row examples.

## Touched Surfaces

- `docs/03_Plans/active/2026-05-23-long-term-scale-architecture-direction.md`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-remaining-refactors.md`

## Approach

Use this document as the durable architecture direction for future speed and
scale work. It does not change runtime behavior by itself; it records the
target shape, workstream priority, non-goals, release gates, and decision logic
for future implementation tranches.

## Current Architecture State

The current architecture is materially better than the original large-ingest
path:

1. Execution is ledger-first.
   - Runs, steps, jobs, branches, retries, heartbeats, recovery events, and
     support evidence are tracked as execution state rather than only mutable
     sync-parameter JSON.
2. Branching and fast bootstrap share the same contracts.
   - Both paths consume the same NQE-shaped rows, validation logic, adapter
     behavior, issue reporting, statistics, and support evidence.
3. Shard-scoped fetch exists for the supported model set.
   - The planner can carry shard predicates and query fetch can run bounded
     partitioned fetches for full and diff paths.
4. Recovery is explicit.
   - Stale stage, merge, and run states have reason-coded recovery behavior and
     support-bundle evidence.
5. Operator visibility is first class.
   - Sync Health and support bundles expose fallback, diff, recovery, density,
     throughput, capacity, partition retry, and tuning signals.
6. Faster apply is gated.
   - `bulk_orm` exists as an apply engine boundary, but model expansion is
     blocked until parity evidence proves correctness.
7. Version support should be one branch.
   - NetBox 4.5 and 4.6 behavior should be capability-gated and tested in a
     matrix rather than maintained as separate long-lived branches.

## What The Prior Refactor Did Not Fully Solve

The prior architecture refactor solved the control plane: resumability,
observability, supportability, native Branching behavior, and fast baseline
setup. It did not eliminate all runtime cost.

Remaining cost centers:

1. Forward query cost can still dominate.
   - If shard-scoped fetch falls back to full/model fetch, a large model can
     pay repeated query cost across retries or shards.
2. Adapter apply cost can still dominate.
   - Relationship-heavy models still need adapter behavior until bulk parity is
     proven for validation, object changes, Branching semantics, row issues,
     and dependency ordering.
3. Branch merge cost is still native Branching cost.
   - We can shard and recover it, but native branch diff and merge are still
     expensive on large change sets.
4. Scheduler throughput is still conservative.
   - The ledger gives us a safe boundary for future overlap, but overlap should
     be added only when support evidence shows wait pressure and capacity
     headroom.
5. Field tuning still needs to become more self-service.
   - Operators need clear guidance on whether to fix diffs, reduce fallback,
     tune query concurrency, increase worker/database capacity, or choose fast
     bootstrap.

## Long-Term Target Shape

The target is not a new sync system. The target is one native NetBox workflow
with interchangeable execution engines underneath it.

### 1) Shared Contract Layer

Each model should have an explicit contract:

- NQE map identity and row shape.
- Coalesce identity.
- dependency ordering.
- safe shard filter fields.
- diff eligibility.
- local safety filter.
- delete behavior.
- apply-engine eligibility.
- diagnostic fields safe for support bundles.

This keeps NQE as the normalizer and gives the planner enough information to
make predictable decisions without Python-side mutation workarounds.

### 2) Fetch Engine

The fetch engine should minimize repeated Forward query work.

Target behavior:

- prefer query ID or repository path execution when diffs are needed.
- run Forward API diffs only when the map identity supports it.
- push shard predicates into NQE where model contracts say it is safe.
- partition large shard filters with bounded concurrency.
- retry narrower partition forms before escalating to full/model fallback.
- record count-only retry and fallback evidence for support.
- convert retry evidence into tuning guidance so operators can distinguish
  healthy avoided fallback from retry pressure that still needs query-contract
  remediation.

The fetch engine should not silently broaden scope. If it falls back, the
support bundle and Sync Health should show model, operation, reason, runtime
cost, and recommended fix layer.

### 3) Planning Engine

The planning engine should turn validated rows and model contracts into a
runtime-safe workload.

Target behavior:

- use Branching guidance as an operator guardrail, not as a brittle exact cap.
- use learned density only when confidence is sufficient.
- keep delete-heavy and relationship-heavy models conservative.
- avoid widening budgets to hide Branching pressure.
- preserve dependency order across models.
- expose estimated branches, rows, changes, and expected bottlenecks before the
  run starts.

### 4) Execution Ledger

The ledger should remain the only orchestration control plane.

Target behavior:

- every stage, merge, finalize, discard, and retry action maps to a ledger step.
- every step is idempotent.
- duplicate jobs become no-ops or explicit recovery events.
- stale worker death is recoverable without manually editing sync parameters.
- support bundles can reconstruct the run after branches are cleaned up.
- compatibility caches remain read-through upgrade aids only, not runtime
  control state.

### 5) Apply Engine Boundary

The adapter path remains the correctness baseline. Faster engines are
acceleration surfaces below the same workflow.

Target behavior:

- adapter engine is always available.
- `bulk_orm` expands only per model after parity tests pass.
- future TurboBulk/parquet/native bulk primitives plug into the same boundary.
- fast bootstrap and Branching can both use faster apply where semantics match.
- unsupported models automatically remain on adapters with an exposed reason.
- blocked models are grouped into promotion lanes so the next safe parity proof
  and the highest expected performance payoff are both visible.

Promotion gates for a faster apply model:

- create parity.
- update parity.
- delete parity.
- validation failure behavior.
- row issue behavior.
- dependency behavior.
- object-change behavior.
- Branching behavior.
- support-bundle statistics.

### 6) Scheduler Throughput

Scheduler overlap should be a measured optimization, not a default assumption.

Target behavior:

- support evidence separates query fetch, apply, queue wait, merge wait, merge
  duration, and scheduler idle time.
- overlap is considered only when wait pressure dominates and capacity is
  available.
- any prefetch/prestage behavior is bounded by ledger state and dependency
  order.
- no side queues exist outside the execution ledger.

### 7) Operations And Self-Service

Large imports should be diagnosable from NetBox without shell access.

Target behavior:

- one-button support bundle export includes run, step, job, branch, query,
  fallback, recovery, density, throughput, and tuning summaries.
- Sync Health gives first-order actions instead of raw counters only.
- Sync Health gives explicit backend advice so operators know whether a slow
  run should remain Branching, use Fast bootstrap for a trusted first baseline,
  or switch back to Branching after bootstrap.
- troubleshooting docs explain how to interpret those actions.
- release gates include support-bundle shape and recovery scenarios.
- capacity guidance covers workers, database, disk, timeouts, query page size,
  query concurrency, and when to use fast bootstrap.

## Recommended Next Workstreams

### A0) Model Contract Registry

Status: `completed_current_baseline_with_call_site_migration_remaining`

Priority: highest structural cleanup before more model-specific speed work.

Do next:

1. Keep one explicit contract surface per supported model.
2. Move fetch, delete, apply-engine, dependency, diff, and diagnostic rules
   toward that contract surface.
3. Make architecture audit fail when a supported model lacks a complete
   contract.

Implemented baseline:

- `forward_netbox.utilities.model_contracts` now composes the current
  model-specific contract surfaces into one audited registry.
- `forward_architecture_audit` emits registry status and gap detail.
- `forward_architecture_completion_audit` includes a registry completion check.

Completion signal:

- a developer can determine a model's NQE identity, coalesce identity,
  dependency order, shard filters, diff eligibility, safety filters, delete
  behavior, apply-engine eligibility, and support-safe diagnostics from one
  place.
- future speed work no longer needs to add parallel per-model rules across
  fetch, planning, apply, delete, and health modules.

### A) Runtime Fallback Reduction

Priority: highest speed impact.

Do next:

1. Use support-bundle fallback and partition-retry summaries to identify models
   still paying full/model fallback.
2. Fix repeated fallbacks in the model contract or NQE query where safe.
3. Keep the local safety filter as the final guard.
4. Add tests for full and diff paths whenever a fallback class is reduced.

Completion signal:

- high-volume models mostly show shard-scoped or diff-scoped fetch.
- fallback reasons are rare, explainable, and actionable.
- partition retry guidance is either absent, informational, or tied to a clear
  remediation layer when retry attempts still fail.

### B) Apply Engine Expansion

Priority: high for fast bootstrap and simple steady-state models.

Do next:

1. Keep the current safe `bulk_orm` set small.
2. Promote one additional low-dependency model at a time.
3. Require parity evidence before enabling by default.
4. Keep relationship-heavy models on adapters until dependency behavior is
   proven.
5. Use `recommended_next_models` for the safest next parity proof and
   `high_impact_blocked_models` for the speed-focused target list; do not treat
   either list as enablement approval.

Completion signal:

- Sync Health and architecture audit show a growing safe model set with no
  untested promotion.
- Promotion lanes remain explicit enough that future speed work can choose a
  high-impact model while still satisfying every parity gate first.

### C) Diff Baseline And Query Identity Hardening

Priority: high for steady-state performance.

Do next:

1. Make it obvious in UI/logs when a map cannot use API diffs.
2. Prefer query ID or repository path execution for steady-state maps.
3. Preserve fast-bootstrap baseline evidence so the next compatible snapshot
   can use diffs.
4. Keep raw query mode supported but clearly mark it as non-diff-capable.

Completion signal:

- a fast bootstrap followed by Branching on a later snapshot produces
  diff-scoped fetch where query identity supports it.

### D) Scheduler Smoothing

Priority: medium; should follow evidence.

Do next:

1. Continue collecting queue/merge/fetch/apply timing.
2. Add bounded overlap only if wait pressure dominates.
3. Gate overlap by database/worker capacity and dependency order.

Completion signal:

- support bundles can prove overlap was needed before the feature is enabled.

### E) Capacity Guidance

Priority: medium; improves field success.

Do next:

1. Convert tuning summaries into user-facing docs.
2. Add large-ingest recommended settings for workers, timeouts, database, disk,
   query page size, and query concurrency.
3. Keep the guidance tied to observable Sync Health signals.
4. Keep backend advice separate from capacity advice: Fast bootstrap is a
   baseline choice, while Branching remains the steady-state diff/review path.

Completion signal:

- operators can decide between fast bootstrap, Branching, query tuning, or
  capacity tuning without sending screenshots first.
- the Health tab gives the same backend recommendation support would give from
  the support bundle.

## Long-Term Follow-On Backlog

These items are the remaining architecture work that would materially improve
speed, stability, or self-service beyond the current ledger-first refactor.
They should be implemented only as evidence-backed tranches, not as broad
rewrites.

### P0: Reduce Repeated Forward Query Work

Problem:
- Large runs still become slow when shard-scoped fetch falls back to repeated
  full/model query execution.

Direction:
- Continue reducing runtime fallback reasons by model.
- Keep shard predicates in model contracts and NQE-safe query surfaces.
- Add a run-local fetch artifact boundary only for retry/resume reuse, not for
  Python-side normalization or alternate source-of-truth behavior.
- Preserve support evidence for any broadened fetch scope.

Acceptance signal:
- high-volume models usually fetch by diff or shard scope.
- repeated retries do not re-run the same expensive full query without an
  explicit reason.
- support bundles show when query work was reused, retried, or broadened.

### P0: Prove Faster Apply Engines Model By Model

Problem:
- Adapter apply remains the correctness baseline, but relationship-light
  models can be faster through bulk-oriented engines if parity is proven.

Direction:
- Promote one candidate model family at a time.
- Keep `bulk_orm` and any future TurboBulk/parquet path behind the same apply
  engine boundary.
- Require create, update, delete, validation, Branching, object-change,
  row-issue, and dependency parity before enablement.
- Leave relationship-heavy models on adapters until equivalent semantics are
  demonstrated.

Acceptance signal:
- architecture audit reports the candidate model as parity-proven.
- synthetic and live smoke baselines show equal correctness and better runtime.
- unsupported models automatically fall back to adapters with clear reasons.

### P0: Make Baseline-To-Diff Transitions Self-Evident

Status: `completed_current_baseline`

Problem:
- Operators can confuse fast bootstrap, first Branching reconciliation, and
  later diff-scoped Branching runs.

Direction:
- Keep Fast bootstrap as explicit trusted-baseline creation.
- Preserve enough execution identity for the next Branching run to explain
  whether API diffs are available.
- Make non-diff reasons visible in Sync Health, ingestion details, and support
  exports.
- Warn when raw query mode or missing query identity prevents diffs.

Acceptance signal:
- a support bundle can answer: baseline exists, query identity is diff-capable,
  selected snapshot pair is eligible, and this run did or did not use API
  diffs.

Implemented baseline:
- execution-run support-bundle metrics now include
  `diff_baseline_transition`.
- Sync Health shows the same message in `Query Runtime & Pushdown` as
  `Baseline to diff`.
- the scale benchmark report includes a dedicated
  `diff_baseline_transition` check.
- transition codes distinguish:
  - `api_diff_active`
  - `fast_bootstrap_baseline_lane`
  - `no_diff_capable_query_identity`
  - `missing_or_ineligible_diff_baseline`
  - `baseline_present_but_full_mode`
  - `diff_requested_but_fell_back`
  - `partial_or_mixed_diff_transition`

Remaining calibration:
- compare repeated large-run support bundles to make sure the transition code
  matches operator expectations before tightening benchmark thresholds.

### P1: Add Evidence-Gated Scheduler Overlap

Problem:
- The current scheduler is conservative. That is good for correctness, but
  idle time can be expensive when fetch/apply/merge waits dominate.

Direction:
- Add bounded prefetch/prestage overlap only when throughput smoothing evidence
  shows wait pressure and capacity headroom.
- Keep all queued work represented in the execution ledger.
- Preserve dependency order and model ordering.
- Add explicit kill/restart recovery tests before enabling overlap broadly.

Acceptance signal:
- support bundles prove wait pressure before overlap is enabled.
- overlap can be disabled without changing row contracts or apply behavior.
- worker death during overlapped work reconciles cleanly from ledger state.

### P1: Harden Delete And Dependency Planning

Problem:
- Filtered imports and large delete waves can create reference pressure and
  operator confusion if delete work is not modeled separately enough.

Direction:
- Keep delete planning dependency-aware by model.
- Split delete-heavy work by expected change density, not just source row
  count.
- Surface preflight estimates for delete volume and likely dependency blockers.
- Keep skip/issue aggregation consistent with create/update paths.

Acceptance signal:
- delete-heavy filtered syncs shard predictably.
- reference blockers are reported as row issues or preflight risks, not opaque
  shard failures.
- delete counters reflect actual NetBox operations, not duplicated planning
  estimates.

### P1: Build A Repeatable Scale Benchmark Harness

Status: `completed_current_baseline`

Problem:
- Live large datasets are useful but slow, sensitive, and hard to compare
  across code changes.

Direction:
- Maintain sanitized synthetic fixtures that stress:
  - high interface/IP volume
  - cable/LAG/reference-heavy relationships
  - filtered delete waves
  - routing plugin models
  - diff-capable query identity
- Record runtime, fallback, diff, retry, apply, merge, and recovery metrics.
- Keep the harness aligned with real support-bundle fields so field evidence
  and lab evidence compare directly.

Acceptance signal:
- release candidates can show before/after speed and resilience changes without
  customer-specific data.
- regressions in fallback rate, diff utilization, or row failures fail the
  benchmark gate.

Implemented baseline:
- `forward_scale_benchmark` evaluates the latest execution run for a sync, a
  specific run ID, or an exported support-bundle JSON file.
- `invoke scale-benchmark` writes the sanitized benchmark report under
  `docs/03_Plans/evidence/` by default.
- the report uses existing execution-run support-bundle metrics, so it does not
  introduce a second telemetry store or retain row data.
- checks cover run completion, row failures, pushdown efficiency/runtime,
  diff utilization, partition retry pressure, throughput wait, and apply-engine
  evidence.

Remaining calibration:
- tune warning/fail thresholds as repeated synthetic and live non-sensitive
  scale evidence accumulates.
- add additional synthetic data shapes only when they map to a real support
  issue or release-risk pattern.

### P2: Capacity And Deployment Profiles

Problem:
- Large imports depend on NetBox workers, database, disk, Redis, timeout, and
  Forward query behavior. Operators need guidance without guessing.

Direction:
- Convert observed Sync Health signals into documented deployment profiles.
- Provide small, medium, large, and very-large recommendations for:
  - NetBox worker count
  - RQ timeout
  - database storage and I/O
  - query page size
  - query fetch concurrency
  - branch budget guidance
- Keep recommendations tied to observed health signals, not static folklore.

Acceptance signal:
- an operator can look at Sync Health and choose whether to add capacity,
  reduce query fallback, use Fast bootstrap, or leave the system alone.

### P2: Version Capability Probing

Problem:
- NetBox 4.5, 4.6, and future releases expose different capabilities. Separate
  branches would increase maintenance risk.

Direction:
- Keep one code path.
- Add runtime capability probes where behavior changes by NetBox or plugin
  version.
- Keep CI matrix coverage for supported NetBox minors.
- Add future TurboBulk/custom-object capabilities under the existing apply and
  model-contract boundaries.

Acceptance signal:
- supported NetBox versions pass the same behavioral tests with only explicit
  capability-gated differences.
- future fast apply surfaces can be enabled without forking sync workflows.

## Explicit Non-Goals

- Do not add Python-side normalization that changes NQE row semantics.
- Do not raise branch budgets to hide native Branching pressure.
- Do not create a separate bulk-sync product.
- Do not use side queues outside the ledger.
- Do not make fast bootstrap implicit.
- Do not bypass NetBox validation or object-change expectations for speed.
- Do not split NetBox 4.5 and 4.6 into divergent long-lived branches.

## Release Gate For Architecture Changes

Any future change in these areas should pass the smallest relevant targeted
tests first, then the release gate:

```bash
poetry run invoke harness-check
poetry run invoke lint
poetry run invoke docs
poetry run invoke architecture-audit-check
poetry run invoke architecture-completion-audit
poetry run invoke check
```

For runtime behavior changes, also run targeted Django tests for the touched
surface and at least one synthetic scenario covering support-bundle evidence.

## Validation

This is a documentation-only architecture direction update. Required validation:

- `git diff --check -- docs/03_Plans/active/2026-05-23-long-term-scale-architecture-direction.md docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- `poetry run invoke harness-check`
- `poetry run invoke docs`

Runtime implementation tranches derived from this plan must add targeted code
tests for the touched surface before claiming completion.

## Rollback

Revert this document and remove the link from
`docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`. Because the
change is documentation-only, rollback has no runtime or migration impact.

## Decision Log

- Keep the architecture native to NetBox and Branching because field failures
  have been about scale, visibility, and recovery, not a need for a separate
  inventory system.
- Treat fast bootstrap as the large initial-load lane and Branching as the
  review lane; both must share contracts so switching lanes does not create a
  second normalization pass.
- Prioritize reducing repeated work before increasing concurrency.
- Keep faster apply engines behind parity gates because incorrect fast writes
  are worse than slow correct writes.
- Use the ledger as the only durable orchestration boundary because it is the
  piece that makes recovery, support export, and future scheduler work safe.
