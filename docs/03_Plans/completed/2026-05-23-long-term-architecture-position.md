# 2026-05-23 Long-Term Architecture Position

## Goal

Record the current long-term architecture position for large
Forward-to-NetBox syncs, including what the prior architecture refactors
already solved, what they did not solve, and what remains worth doing for
speed, stability, and operator self-service.

## Constraints

- NQE remains the source of truth for row shape, normalization, coalescing,
  filtering, and model identity.
- NetBox-native model writes remain the only mutation path.
- Branching remains the native review path for steady-state sync.
- Fast bootstrap remains an explicit trusted-baseline path for large first
  imports.
- Faster write paths such as `bulk_orm`, future TurboBulk, parquet-backed
  loaders, or future NetBox bulk primitives must plug under the same execution
  workflow, not become separate sync products.
- Runtime artifacts and support bundles must not persist customer row data,
  network IDs, snapshot IDs, credentials, screenshots, or private examples.

## Touched Surfaces

- `docs/03_Plans/active/2026-05-23-long-term-architecture-position.md`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- Related architecture planning references:
  - `docs/03_Plans/active/2026-05-23-long-term-scale-architecture-direction.md`
  - `docs/03_Plans/active/2026-05-23-long-term-architecture-remaining-refactors.md`
  - `docs/03_Plans/active/2026-05-23-architecture-state-and-remaining-work.md`

## Approach

Use this document as the concise decision record for future long-term
architecture work. The detailed roadmap and remaining-refactors documents stay
authoritative for workstream status and validation evidence; this file explains
the architectural position in a smaller operator/developer-readable form.

## Current Position

The architecture should stay as one native NetBox workflow with execution
engines underneath it. We should not split the project into separate products
for small syncs, large Branching syncs, and fast baseline imports.

The correct long-term shape is:

1. NQE defines the normalized NetBox-shaped rows.
2. The model contract registry defines model-specific fetch, coalesce,
   dependency, delete, apply-engine, and diagnostic rules.
3. The execution ledger is the only orchestration control plane.
4. Branching remains the reviewable steady-state path.
5. Fast bootstrap remains the trusted first-baseline path.
6. Faster apply engines are optional acceleration layers below the shared
   workflow.
7. Sync Health and support bundles are the primary self-service diagnostic
   surfaces.

This keeps the project aligned with NetBox-native behavior while still giving
large environments a path that can finish within practical runtime limits.

## What The Prior Refactor Solved

The 0.8 and 0.9 architecture work fixed the control-plane problems that were
making large syncs brittle:

1. Execution is ledger-first.
   - Runs, steps, jobs, branches, retries, heartbeats, stale-state recovery,
     and support evidence are represented as execution state instead of only
     mutable sync-parameter JSON.
2. Fast bootstrap and Branching share row contracts.
   - Both paths consume the same NQE-shaped rows, validation rules, row issue
     handling, statistics, and support evidence.
3. Shard-scoped fetch is present.
   - The planner can carry shard predicates and the fetch layer can execute
     bounded partitioned fetches for full and diff paths.
4. Recovery is reason-coded.
   - Stale stage, merge, and run conditions now produce explicit recovery
     events and recommendations.
5. Operator visibility is first class.
   - Sync Health and support bundles expose diff, fallback, recovery, density,
     throughput, capacity, partition retry, and large-run tuning evidence.
6. Faster apply has a boundary.
   - The adapter path remains the correctness baseline while `bulk_orm` and
     future engines can be proven model by model.
7. NetBox version support is intended to stay unified.
   - Version-specific behavior should be capability-gated and CI-tested, not
     maintained as separate long-lived branches.

## What It Did Not Fully Solve

The remaining problems are runtime-economics problems, not another control-plane
rewrite.

1. Forward query cost can still dominate.
   - If shard-scoped fetch falls back to full/model fetch, a large model can
     still pay repeated query cost across shards or retries.
2. Adapter apply cost can still dominate.
   - Relationship-heavy models still need adapter semantics until a faster
     engine proves NetBox validation, object-change, Branching, row-issue, and
     dependency parity.
3. Native Branching merge cost remains real.
   - The plugin can shard, recover, and explain Branching work, but it cannot
     make native branch diff/merge free for huge change sets.
4. Scheduler throughput is intentionally conservative.
   - That is correct for safety. Overlap should be added only when support
     evidence proves queue or merge wait dominates and there is worker/database
     headroom.
5. Delete-heavy filtered syncs need stronger planning.
   - Filter changes can create large delete waves and reference blockers that
     should be planned and surfaced separately from normal source-row work.
6. Field operations need tighter self-service loops.
   - The project has the right health/support surfaces; the remaining work is
     making those surfaces decide the next action without requiring raw log
     interpretation.

## Architecture Decision

Do not replace the current architecture. Tighten it.

The current shape is the right long-term direction because it preserves the
native NetBox product boundaries:

- NQE is the data-shaping layer.
- NetBox models and validation are the mutation layer.
- Branching is the review layer.
- The execution ledger is the orchestration layer.
- Apply engines are interchangeable implementation details below that
  workflow.

The next work should reduce repeated work inside this architecture, not create
parallel side channels.

## Remaining High-Value Work

### 1) Runtime Fallback Reduction

Priority: highest.

Target:

- reduce model/full fallback after shard pushdown is attempted.
- keep local safety filters as the final guard.
- use support-bundle fallback reason summaries to pick the right fix layer.
- correct repeated fallback in the model contract or NQE query where possible.

Completion signal:

- support bundles show rare, explainable fallback.
- repeated large runs do not repeatedly broaden the same model to full/model
  fetch without a clear reason.

### 2) Run-Local Fetch Artifact Boundary

Priority: completed current baseline; continue monitoring field evidence.

Target:

- add a runtime-only artifact boundary for retry/resume reuse of scoped fetch
  results.
- keep artifacts scoped to execution run, model, shard, snapshot, query
  identity, and shard predicate.
- store row data only in temporary runtime storage.
- store only count/status metadata in the execution ledger and support bundle.
- clean artifacts deterministically on completion, cancel, and failure.

Non-goal:

- this is not a durable row cache and not a second source of truth.

Current baseline:

- shard-scoped fetches now store bounded run-local artifacts after successful
  fetch.
- retry/resume can reuse a valid artifact instead of repeating the same Forward
  query.
- only support-safe artifact metadata is written into execution metadata.
- temporary artifacts are pruned when execution runs complete or fail through
  the current ledger paths.

Completion signal:

- retrying a failed shard can reuse a valid scoped fetch artifact instead of
  re-running the same expensive Forward query.
- support bundles can say whether fetch work was reused, retried, broadened, or
  discarded without exposing row data.

### 3) Apply Engine Promotion

Priority: high for simple/high-volume models, lower for relationship-heavy
models.

Target:

- expand `bulk_orm` only through parity gates.
- keep adapter fallback automatic and visible.
- keep future TurboBulk/parquet/native bulk primitives behind the same engine
  boundary.

Promotion gates:

- create parity
- update parity
- delete parity
- validation failure behavior
- row issue behavior
- dependency behavior
- object-change behavior
- Branching behavior
- support-bundle statistics

Completion signal:

- one additional model family is promoted only after tests prove equivalent
  behavior and better or equal runtime.

### 4) Delete And Dependency Planning

Priority: completed current baseline; continue calibrating from field evidence.

Target:

- estimate delete waves separately from source-row work.
- shard delete-heavy work by expected change density and dependency risk.
- surface likely reference blockers before merge when possible.
- keep row issue aggregation consistent with create/update paths.

Current baseline:

- mixed workloads are split into apply then delete phases.
- delete workloads execute in dependency order.
- delete-heavy device workloads use conservative row budgets.
- plan previews include `delete_dependency_plan` with delete volume, delete
  shard sizing, dependency-ordered model execution, per-model dependency risk,
  and warning codes for delete waves or likely reference blockers.

Completion signal:

- filtered syncs do not produce surprising oversized delete shards.
- reference blockers become preflight risks or row issues instead of opaque
  shard failures.

### 5) Evidence-Gated Scheduler Overlap

Priority: medium; implement only after evidence says it helps.

Target:

- add bounded prefetch or prestage overlap only when support bundles show wait
  pressure dominates runtime.
- require worker and database headroom before enabling overlap.
- keep every in-flight action represented in the execution ledger.

Non-goals:

- no side queues outside the ledger.
- no unbounded concurrent mutations for the same dependency chain.
- no branch-budget widening to hide native Branching pressure.

Completion signal:

- overlap can be disabled without behavior change.
- worker death during overlapped work reconciles from ledger state.

### 6) Capacity Profiles And Self-Service Operations

Priority: medium, ongoing.

Target:

- keep small, medium, large, and very-large deployment profiles documented.
- map profile advice to Sync Health/support-bundle signals.
- tell operators whether to fix diffs, reduce fallback, increase capacity,
  tune query concurrency, use Fast bootstrap, or stay on Branching.

Completion signal:

- a support bundle plus Sync Health summary is enough to choose the next
  operational action without screenshots as the primary evidence.

### 7) Capability-Gated NetBox 4.6+ And Future Bulk Features

Priority: medium, release-gate work.

Target:

- keep one branch for supported NetBox versions.
- use runtime capability probes for version-specific features.
- test supported NetBox minors in CI.
- treat future bulk features as apply engines, not separate workflows.

Completion signal:

- NetBox 4.5 and 4.6+ pass the same behavioral suite with only explicit
  capability-gated differences.

## Recommended Execution Order

1. Keep the model contract registry as the first place for model-specific
   rules.
2. Use support-bundle fallback metrics to reduce repeated model/full fetch
   fallback.
3. Promote one additional apply-engine model family through parity gates.
4. Strengthen delete/dependency planning for filtered syncs.
5. Add scheduler overlap only after support evidence shows wait pressure and
   capacity headroom.
6. Keep capacity guidance and NetBox capability gates updated as the runtime
   evidence changes.

## Release Gate For Future Architecture Work

Any future change in this lane should prove:

```bash
poetry run invoke harness-check
poetry run invoke lint
poetry run invoke docs
poetry run invoke architecture-audit-check
poetry run invoke architecture-completion-audit
poetry run invoke check
```

Runtime behavior changes should also include targeted Django tests and a
support-bundle or Sync Health evidence check for the changed surface.

## Decision Log

- Keep one native NetBox workflow. Do not create a separate large-sync product.
- Treat Fast bootstrap as a trusted baseline path, not a replacement for
  Branching review.
- Keep Branching conservative for steady-state diffs, but do not treat the
  10k guidance as a brittle exact row cap.
- Reduce repeated Forward query work before widening Branching budgets.
- Promote faster apply engines only after parity gates pass.
- Add scheduler overlap only from evidence, not intuition.
- Keep row data out of durable orchestration state and support bundles.

## Validation

For this planning artifact:

```bash
git diff --check -- docs/03_Plans/active/2026-05-23-long-term-architecture-position.md docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md
poetry run invoke harness-check
```

Future runtime changes derived from this position must include the relevant
targeted Django tests plus the standard architecture gates:

```bash
poetry run invoke harness-check
poetry run invoke lint
poetry run invoke docs
poetry run invoke architecture-audit-check
poetry run invoke architecture-completion-audit
poetry run invoke check
```

## Rollback

This is a planning artifact only. Rollback is a normal git revert or file
deletion.

Runtime work derived from this position must remain independently reversible:

- fetch artifact reuse must preserve the existing direct fetch/fallback path.
- apply-engine promotion must keep adapter fallback.
- scheduler overlap must be disableable without data migration.
- delete/dependency planning must keep destructive changes visible before
  merge.
- capability-gated behavior must default to the current path when a capability
  is absent.
