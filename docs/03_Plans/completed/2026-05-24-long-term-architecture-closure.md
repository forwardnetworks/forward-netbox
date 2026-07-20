# 2026-05-24 Long-Term Architecture Closure

## Goal

Capture the current long-term architecture position for scale, speed,
stability, and self-service operation. This is the short decision artifact for
whether the project needs another broad re-architecture before the next release.

## Constraints

- NQE remains the source of truth for row shape, normalization, filtering,
  coalescing, and model identity.
- NetBox-native writes remain the only mutation path.
- Branching remains the reviewable steady-state path.
- Fast bootstrap remains the explicit trusted-baseline path for very large
  first imports.
- Faster engines such as `bulk_orm`, future TurboBulk, parquet loaders, or
  NetBox-native bulk primitives must plug under the apply-engine boundary.
- The execution ledger remains the orchestration control plane.
- No customer identifiers, network IDs, snapshot IDs, credentials,
  screenshots, or private row examples belong in committed artifacts.

## Related Artifacts

- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- `docs/03_Plans/active/2026-05-23-long-term-scale-architecture-direction.md`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-remaining-refactors.md`
- `docs/03_Plans/active/2026-05-24-long-term-speed-architecture-work.md`
- `docs/03_Plans/active/2026-05-24-long-term-architecture-next-tranche.md`
- `docs/03_Plans/evidence/architecture-runtime-evidence.json`
- `docs/03_Plans/evidence/scale-runtime-evidence.json`
- `docs/03_Plans/evidence/runtime-capacity-review.json`
- `docs/03_Plans/evidence/field-scale-runtime-matrix.json`

## Touched Surfaces

- `docs/03_Plans/active/2026-05-24-long-term-architecture-closure.md`
- `docs/03_Plans/active/2026-05-24-long-term-speed-architecture-work.md`
- `docs/03_Plans/active/2026-05-24-long-term-architecture-next-tranche.md`

## Approach

Use this document as the concise architecture closure position. The detailed
roadmap remains the source for implementation history and validation evidence;
this file records the decision boundary for whether more re-architecture is
needed.

## Current Position

The architecture is in the right shape. We should not start another broad
rewrite and should not create a second sync product. The correct long-term path
is one NetBox-native workflow with better economics and visibility underneath
it:

1. NQE produces normalized, filtered, model-shaped rows.
2. The model contract registry describes model-specific fetch, identity,
   dependency, delete, apply-engine, and diagnostic behavior.
3. The execution ledger owns run, step, retry, recovery, completion, and
   support-bundle state.
4. Branching remains the native reviewable steady-state workflow.
5. Fast bootstrap remains a trusted first-baseline workflow for very large
   initial loads.
6. Faster write engines remain implementation details behind the apply-engine
   selector.

This keeps the project aligned with NetBox-native behavior while still leaving
room for faster model-specific write paths and future NetBox capabilities.

## Current Runtime Status

Status: `architecture_converged_runtime_evidence_open`

The current runtime evidence supports the architecture direction, but it does
not fully close the scale proof. The active local large run is still running,
so it is diagnostic evidence rather than completion evidence.

Latest sanitized local evidence:

- refreshed at `2026-05-24T12:01:19Z`.
- execution run `119` is `running` with `166` total steps.
- `80` steps are merged, `1` step is running, and `85` steps remain pending.
- shard `68/166` completed native Branching merge after staging `9795`
  attempted rows, `9795` applied rows, `9795` actual changes, and `0` failed
  rows.
- shard `69/166` completed native Branching merge after staging cleanly with
  `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` failed
  rows, and `0` retries.
- shard `70/166` completed native Branching merge after staging cleanly with
  `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` failed
  rows, and `0` retries.
- shard `71/166` completed native Branching merge after staging cleanly with
  `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` failed
  rows, and `0` retries.
- shard `72/166` completed native Branching merge after staging cleanly with
  `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` failed
  rows, and `0` retries.
- shard `73/166` completed native Branching merge after staging cleanly with
  `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` failed
  rows, and `0` retries.
- shard `74/166` completed native Branching merge after staging cleanly with
  `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries.
- shard `75/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- current active step is shard `76/166` for `ipam.prefix`.
- shard `76/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- current active step is shard `77/166` for `ipam.prefix`.
- shard `77/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- current active step is shard `78/166` for `ipam.prefix`.
- shard `78/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- current active step is shard `79/166` for `ipam.prefix`.
- shard `79/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9794` attempted rows, `9794` applied rows, `9794` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- current active step is shard `80/166` for `ipam.prefix`.
- shard `80/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9794` attempted rows, `9794` applied rows, `9794` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- current active step is shard `81/166` for `ipam.prefix`.
- shard `81/166` is using `nqe_column_filter`, one column filter, and estimated
  `9794` changes. The latest recovery snapshot shows it running stage with no
  failures or last error.
- no run-level error is present.
- no row failures are present across `532592` attempted rows in the current
  benchmark evidence.
- recovery recommendation is `wait`, because the active stage job is live.

Interpretation:

- The ledger-first recovery model is working through native NetBox job
  execution.
- The prefix path has advanced through repeated clean stage and merge cycles
  after the earlier retry/replan/stale-job fixes.
- The remaining proof is terminal large-run evidence, not another architecture
  redesign.

## What The Recent Architecture Work Fixed

The prior large refactors fixed the control plane. The important improvements
are now in place:

1. Execution is ledger-first instead of compatibility-payload-first.
2. Branching and fast bootstrap share the same NQE-shaped row contracts.
3. Shard-scoped fetch contracts exist for the supported model set.
4. Partitioned Forward fetch can run with bounded concurrency and deterministic
   result ordering.
5. Runtime-only fetch artifacts can reduce repeated scoped fetch work during
   retry and resume without becoming a durable row store.
6. Branch density, delete waves, dependency order, fallback reasons, partition
   retries, and recovery events are visible in Sync Health and support bundles.
7. Completion invariants prevent runs from being treated as complete while
   non-terminal stage steps remain.
8. Recovery behavior is reason-coded, bounded, and visible.
9. Fast apply is represented as an apply-engine boundary, not a separate
   operator workflow.
10. NetBox 4.5 and 4.6+ should remain on one branch through capability gates
    and CI coverage.
11. Orphaned queued shard state now resets through ledger reconciliation and
    can be resumed with the native execution-run recovery task instead of
    manual database changes.

## What The Prior Architecture Still Missed

The remaining problems are runtime economics and field evidence, not workflow
design:

1. Forward query cost can still dominate if shard pushdown falls back to
   broader model fetches.
2. Adapter apply cost can still dominate for high-volume models that are not
   proven safe for `bulk_orm`.
3. Native Branching merge/diff cost still exists; the plugin can shard,
   recover, and explain it, but cannot make native branch merge free.
4. Scheduler overlap is still only a candidate. It should not be implemented
   until repeated support-bundle evidence shows queue or merge wait dominates
   and the worker/database profile has headroom.
5. Large-run evidence still needs a completed run or sanitized support bundle
   where run completion, step state, fallback, row failure, diff utilization,
   and scheduler-readiness evidence all agree.

## Architecture Decision

Stay on the current architecture and finish the proof points. Do not introduce
a new product path.

Accepted direction:

- keep one sync workflow surface.
- keep row semantics in NQE.
- keep orchestration in the execution ledger.
- keep Branching as the steady-state review path.
- keep fast bootstrap as the large first-baseline path.
- keep faster write engines behind the apply-engine boundary.
- keep future NetBox/TurboBulk/parquet behavior capability-gated under the same
  model contracts and apply-engine selection.

Rejected directions:

- a second non-native sync workflow.
- durable row storage outside Forward/NetBox evidence.
- Python-side normalization that should live in NQE.
- widening branch budgets to hide fallback, delete-density, or merge-pressure
  problems.
- unbounded scheduler concurrency outside the execution ledger.
- splitting NetBox 4.5 and 4.6+ into long-lived divergent branches.

## Remaining Work

### P0: Completed Large-Run Evidence

Status: `open_run_active`

Need a completed field-scale run or sanitized support bundle proving:

- ledger run state and step state agree.
- no incomplete stage steps are hidden by a completed run status.
- row failures are bounded and visible.
- fallback count and fallback runtime share are low or explainable.
- partition retry pressure is low or actionable.
- query-ID diffs are actually used after baseline readiness exists.
- scheduler overlap is either unnecessary or justified by capacity-backed
  wait-pressure evidence.

This is the main gate before claiming the architecture is fully proven.

Current evidence:

- the local large run is active and has progressed to shard `81/166`.
- the recovery path is now native and repeatable via
  `invoke execution-run-recovery`.
- completion is still not proven until the run reaches a terminal completed
  state and the benchmark checks pass.

### P0: Runtime Fallback Reduction

Status: `open_until_repeated_large_run_evidence`

Continue reducing full/model fallback by fixing the right layer:

- NQE/query contract when the filter or row shape is wrong.
- Forward query/runtime behavior when API execution drives fallback.
- local safety filtering only when it preserves NQE row semantics.

Fallback must remain available as a safe fallback path, but repeated fallback
must be visible and actionable.

### P1: Apply-Engine Expansion

Status: `planned_per_model`

Expand `bulk_orm` only after model-specific parity is proven:

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

The adapter path remains the correctness baseline and fallback.

### P1: Evidence-Gated Scheduler Overlap

Status: `planned_after_evidence`

Add bounded scheduler overlap only if support bundles repeatedly show queue or
merge wait dominates after tuning:

- worker count
- worker timeout
- query fetch concurrency
- NQE page size
- PostgreSQL capacity
- disk/container placement

If implemented, overlap must be ledger-derived and bounded. It must not create
side queues or concurrent mutation paths that bypass dependency order.

### P1: Capacity Profile Calibration

Status: `in_progress`

Keep turning runtime evidence into operator-facing guidance:

- small, medium, large, and very-large profiles.
- first diagnostic action from Sync Health and support bundles.
- clear guidance for when to use fast bootstrap versus Branching.
- clear warnings when query-ID diffs are not available.
- large-run support-bundle export as the standard field diagnostic artifact.

### P2: Future Bulk Engines

Status: `planned`

Future NetBox/TurboBulk/parquet/native bulk capabilities should be treated as
additional apply engines:

- capability-gated at runtime.
- covered in CI where possible.
- selected through the model contract/apply-engine boundary.
- invisible to NQE map shape and normal operator workflow.

## Recommended Execution Order

1. Finish a completed large-run evidence artifact or sanitized support-bundle
   ingestion.
2. Use that evidence to close or prioritize runtime fallback fixes.
3. Decide whether scheduler overlap is justified only after capacity and query
   tuning are represented in the evidence.
4. Promote the next `bulk_orm` model only after parity tests pass.
5. Keep NetBox 4.6+/future bulk behavior behind capability gates on the same
   branch.

## Validation

Planning-only validation:

```bash
git diff --check -- docs/03_Plans/active/2026-05-24-long-term-architecture-closure.md
poetry run invoke harness-check
```

Runtime evidence validation:

```bash
poetry run invoke architecture-runtime-evidence \
  --sync-name ui-harness-sync \
  --scale-run-id <execution-run-id> \
  --scale-reconcile

poetry run invoke architecture-completion-audit
```

Offline support-bundle evidence:

```bash
poetry run invoke architecture-runtime-evidence \
  --sync-name ui-harness-sync \
  --scale-input-json /path/to/sanitized-support-bundle.json

poetry run invoke architecture-completion-audit
```

Release-candidate validation:

```bash
poetry run invoke harness-check
poetry run invoke harness-test
poetry run invoke architecture-audit-check
poetry run invoke check
poetry run invoke test
poetry run invoke docs
poetry run invoke ci
```

## Rollback

This file is a planning artifact and has no runtime effect. If it diverges
from implementation evidence, refresh it from the runtime evidence artifacts
and the active long-term roadmap.

Runtime workstreams remain independently reversible:

- fallback remediation must preserve full/model fallback.
- apply-engine expansion must preserve adapter fallback.
- scheduler smoothing must remain ledger-derived and disableable.
- future bulk engines must not alter NQE contracts or operator workflow.

## Decision Log

- The current architecture is the right long-term shape.
- Remaining work is proof, economics, and self-service visibility, not another
  product rewrite.
- A completed large-run artifact is required before declaring the architecture
  fully proven.
- Scheduler overlap is a possible future optimization, not a default next
  step.
- Future bulk capabilities should plug into the apply-engine boundary, not
  fork the workflow.
