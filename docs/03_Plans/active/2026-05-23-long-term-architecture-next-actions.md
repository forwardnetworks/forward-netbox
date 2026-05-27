# 2026-05-23 Long-Term Architecture Next Actions

## Goal

Capture the remaining long-term architecture work needed after the ledger,
fast-bootstrap, shard-fetch, recovery, and observability refactors so the
project is positioned for larger datasets without creating a second sync
product.

## Constraints

- NQE remains the source of truth for row shape, normalization, coalescing, and
  model identity.
- NetBox-native model writes remain the only mutation path.
- Branching remains the native review path for steady-state sync.
- Fast bootstrap remains an explicit baseline path for large first imports.
- Support bundles and Sync Health remain the operator-facing troubleshooting
  surfaces.
- No customer identifiers, network IDs, snapshot IDs, credentials, screenshots,
  or private row examples should be committed.

## Touched Surfaces

- `docs/03_Plans/active/2026-05-23-long-term-architecture-next-actions.md`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- `docs/03_Plans/active/2026-05-23-architecture-state-and-remaining-work.md`
- `ARCHITECTURE.md`
- Future implementation tranches will likely touch:
  - `forward_netbox/utilities/query_fetch_execution.py`
  - `forward_netbox/utilities/branch_budget.py`
  - `forward_netbox/utilities/execution_ledger*.py`
  - `forward_netbox/utilities/execution_ledger_metrics.py`
  - `forward_netbox/utilities/health_summary_blocks.py`
  - `forward_netbox/utilities/ingestion_merge.py`
  - `forward_netbox/utilities/sync.py`
  - `forward_netbox/tests/`

## Approach

### Current Baseline

The architecture is now in a much better shape than the earlier large-ingest
path:

1. Execution state is ledger-first rather than compatibility JSON first.
2. Branching and fast bootstrap share the same NQE-shaped row contracts.
3. Shard-scoped fetch contracts exist for the supported model set.
4. Partitioned fetch can run concurrently with deterministic result ordering.
5. Recovery automation handles stale stage/merge/run states with explicit
   reason codes and support-bundle evidence.
6. Support bundles and Sync Health expose pushdown, fallback, recovery, density,
   and throughput signals.
7. NetBox version support is intended to stay on one code path with
   capability/version gates.
8. Support bundles and Sync Health now expose first-order large-run tuning
   actions, so operators do not have to infer the next step from raw metrics.
9. Support bundles now include scheduler-overlap readiness evidence, so any
   future overlap work has to be justified by measured queue/merge wait
   pressure and capacity preconditions.
10. Single-value shard column-filter failures now retry an equivalent native
    filter operator before broad fallback, reducing avoidable full/model refetch
    without changing NQE row semantics; count-only retry metadata is kept for
    support triage and aggregated in Sync Health/support bundles.

The previous architecture work solved the control-plane problem: resumability,
state reconstruction, supportability, and native NetBox/Branching behavior. The
remaining work is mostly runtime economics: reducing repeated fetch/apply work,
expanding faster apply paths only where parity is proven, and tuning concurrency
from measured bottlenecks.

## What The Current Architecture Still Needs

The current architecture is directionally correct: it keeps NQE as the source of
truth, preserves NetBox-native writes, and uses Branching as the review path
instead of inventing a parallel sync system. The remaining long-term work should
therefore improve the cost of the existing workflow, not replace it.

The earlier large refactors solved these core problems:

1. Runtime state is reconstructable from execution runs and steps instead of
   only from mutable sync-parameter JSON.
2. Fast bootstrap and Branching now share the same model-shaped NQE row
   contracts, validation, issue reporting, and support evidence.
3. Branching runs can recover from stale stage, merge, and worker states with
   explicit ledger evidence.
4. Operators can export support bundles with query, pushdown, fallback,
   recovery, density, and throughput evidence.
5. Large runs have a native NetBox troubleshooting surface through Sync Health.

What those refactors did not fully solve:

1. Large datasets can still pay too much repeated Forward query cost when a
   model falls back from shard-scoped fetch to full/model fetch.
2. Apply cost is still adapter-bound for complex models until a faster engine
   proves parity with NetBox validation, object changes, Branching semantics,
   and row-level issue handling.
3. Scheduler throughput is still mostly sequential around branch stage/merge
   boundaries; deeper overlap needs measured queue/merge wait pressure before
   implementation. The current baseline now exposes a readiness gate for that
   decision instead of leaving it as an informal judgment call.
4. Branch budgets are safer than before, and confidence-informed budget
   decisions are now explicit so learned density does not silently overreact to
   noisy runs.
5. Capacity guidance is now visible in health/support output, but the docs
   should continue turning those signals into concrete worker, database, disk,
   and timeout recommendations.

These gaps are architecture work, not one-off bug fixes. They should be handled
as bounded workstreams with tests and field evidence.

## Remaining Long-Term Architecture Work

### 1) Runtime Fallback Remediation

This is the highest-value performance item. Repeated full/model fallback means a
large model can still re-fetch too much data even though shard contracts exist.

Target state:

- support bundles identify fallback reason, model, fetch mode, and runtime cost
  clearly enough to pick the right fix.
- fallback summaries classify the likely remediation layer so operators can
  start with NQE contract, Forward query runtime, diff execution, timeout, or
  manual exception analysis.
- repeated fallbacks are corrected at the NQE contract or Forward API execution
  layer when safe.
- full/model fallback remains available, but it is rare, explainable, and
  visible.

Implemented baseline:

- fallback summaries now include remediation action codes and suggested fix
  layers for model-contract fallback, shard pushdown fallback, diff fallback,
  timeout pressure, parameter-contract issues, and unknown exceptions.
- full and diff shard fetch paths retry equivalent single-value native
  column-filter operators before escalating to full/model fallback.
- fetch metadata includes count-only partition retry summaries when split or
  alternate-operator retries happen.
- support bundles and Sync Health aggregate partition retry attempts, successes,
  avoided fallback counts, and per-model/per-operation counts.

Avoid:

- broad Python-side normalization that diverges from NQE.
- hiding fallbacks by widening branch budgets.

### 2) Confidence-Informed Branch Budget Auto-Tuning

Status: `completed_current_baseline`

Density learning now has an explicit planner policy.

Target state:

- high-confidence learned density can influence model row budgets.
- medium-confidence learned density is blended or capped.
- low-confidence or stale density falls back toward conservative defaults.
- delete-heavy paths and hard branch limits remain guarded regardless of
  confidence.
- support bundles explain which density input was used and why.

Implemented baseline:

- `effective_row_budget_for_model()` and `effective_workload_row_budget()` now
  accept `model_change_density_profile`.
- Branch planning, overflow re-splitting, display parameters, workload summary,
  and execution summary all use the same confidence-informed density policy.
- Density summaries expose `budget_density`, `budget_policy`, and
  `budget_policy_reason` so operators can see whether learned density was used,
  blended, or ignored.

Avoid:

- using a single anomalous run to widen a model budget.
- making the 10k Branching guidance look like a hard product maximum; keep it
  as an operator guardrail while still allowing small overshoots where measured
  native change counts justify them.

### 3) Apply Engine Expansion

`bulk_orm` should grow only where it remains semantically equivalent to the
adapter path.

Target state:

- each new model family has parity tests for create, update, delete, skipped
  rows, validation failures, object-change behavior, and Branching behavior.
- unsupported or relationship-heavy models stay on adapters until evidence says
  otherwise.
- future TurboBulk/parquet/native bulk primitives plug into the same apply
  engine boundary.
- Sync Health and architecture audit expose a `bulk_orm_expansion` summary with
  safe models, blocked models, blocker reasons, required parity gates, and next
  action before any future model promotion.

Avoid:

- a second workflow for bulk ingestion.
- faster writes that bypass NetBox validation or object-change expectations.

### 4) Throughput Smoothing

The next scheduler improvement should be evidence-led. If support metrics show
stage queue or merge wait dominates, add bounded overlap; otherwise tune worker,
database, and query concurrency first.

Target state:

- query fetch, apply, merge wait, merge duration, and scheduler wait are
  separable in support evidence.
- support evidence explicitly says whether scheduler overlap is not indicated,
  lacks evidence, or is only a candidate after worker/database capacity review.
- the scheduler can prepare or prefetch only the next eligible step when
  dependency order, branch budget, and DB/worker headroom are clear.
- all in-flight state remains ledger-derived and reconstructable.

Avoid:

- unbounded concurrent mutations for the same dependency chain.
- side queues outside the execution ledger.

### 5) Capacity And Operations Guidance

Large imports should be self-service for operators who do not have shell access.

Target state:

- Sync Health gives first-order actions: restore diffs, reduce fallback,
  increase worker/runtime capacity, check database headroom, or tune query
  concurrency.
- troubleshooting docs explain how to interpret those actions.
- support bundles contain enough evidence to debug Blake-style field failures
  without screenshots as the primary artifact.

Avoid:

- burying the required evidence only in raw JSON.
- making field debugging depend on customer data examples.

### 6) Future NetBox 4.6+ And Bulk Primitives

Keep one branch and one architecture path with capability gates.

Target state:

- NetBox 4.5 users keep the current native behavior.
- NetBox 4.6+ features are enabled through capability detection and tests.
- TurboBulk or future NetBox bulk primitives are apply engines, not separate
  sync products.

Avoid:

- diverging release branches for behavior that can be capability-gated.
- NetBox-version-specific query contracts unless the model API truly requires
  it.

## Recommended Next Execution Order

1. Use live fallback summaries to reduce repeated runtime fallback reasons.
2. Pick one additional apply-engine candidate model family and prove or reject
   `bulk_orm` parity.
3. Re-run large-ingest regression and inspect `large_run_tuning`,
   `throughput_smoothing`, and `operator_tuning_summary`.
4. Implement scheduler overlap only if repeated evidence shows queue/merge wait
   is the dominant bottleneck after query and capacity tuning.

Release readiness for any of these tranches should require:

- focused Django tests for the touched behavior.
- `poetry run invoke harness-check`
- `poetry run invoke architecture-audit-check`
- `poetry run invoke check`
- fresh support-bundle evidence for any Branching, recovery, or large-run
  behavior change.

### Priority 1: Runtime Fallback Remediation

Use the new `fallback_reason_summary`, pushdown trends, and support-bundle
metrics to identify models that still fall back after shard pushdown is
attempted.

Implementation direction:

1. Collect fallback reason summaries from repeated large runs.
2. Group by model, fetch mode, and fallback reason.
3. Fix repeated fallbacks at the safest layer:
   - NQE contract or query shape when the issue is semantic/query-side.
   - Forward API/query execution handling when the issue is transport/runtime.
   - local safety filter only when preserving NQE row semantics requires it.
4. Keep deterministic full/model fallback with explicit reason codes for cases
   that cannot be shard-filtered safely.

Completion signal:

- Repeated large runs show low fallback counts, or residual fallback causes are
  explicitly explainable in support bundles and Sync Health.

### Priority 2: Apply Engine Expansion With Parity Gates

Keep adapter apply behavior as the correctness baseline. Expand `bulk_orm` only
for models where parity is proven.

Implementation direction:

1. Add one model family at a time.
2. Prove create/update/delete parity against the adapter path.
3. Prove NetBox validation, object-change tracking, Branching behavior, and
   row-level issue behavior are equivalent.
4. Keep automatic fallback to adapters for relationship-heavy or unsupported
   cases.

Completion signal:

- Additional models are enabled for `bulk_orm` only after tests prove parity and
  runtime evidence shows equal or better behavior.

### Priority 3: Throughput Smoothing

Do not add deeper scheduler concurrency until the new throughput metrics prove
queue or merge-wait pressure.

Implementation direction:

1. Use `throughput_smoothing` metrics to separate:
   - Forward query time
   - stage queue time
   - NetBox apply time
   - merge queue/wait time
   - merge duration
2. Tune supported knobs first:
   - `query_fetch_concurrency`
   - NetBox worker count
   - Postgres capacity
   - container CPU/memory placement
3. If wait pressure remains, add a bounded ledger-derived scheduler window that
   can prepare the next eligible shard only when:
   - dependency order is explicit
   - branch budget remains enforced
   - DB/worker headroom exists
   - support bundles can reconstruct the state

Do not implement:

- unbounded concurrent branch mutations for the same dependency chain
- hidden branch-budget widening
- non-ledger side queues

Completion signal:

- Large-run tail latency improves without increasing row failures, duplicate
  stage claims, merge failures, or operator ambiguity.

### Priority 4: Capacity And Operations Guidance

Make large-ingest performance self-service by documenting practical tuning and
surfacing the relevant metrics in support bundles.

Current implementation:

- execution support bundles include `operator_tuning_summary`
- Sync Health includes `large_run_tuning`
- both summarize whether the first action is to restore diff utilization,
  reduce fallback fetch, tune worker timeout/capacity, inspect worker/database
  headroom, or adjust query fetch concurrency

Implementation direction:

1. Document recommended worker, Postgres, disk, and container-storage guidance
   for large ingest runs.
2. Keep support-bundle sections focused on bottleneck/tuning signals that do
   not require shell access.
3. Keep local runtime/test layout aligned with the same guidance.

Completion signal:

- Operators can identify whether a slow run is query-bound, DB-bound,
  merge-bound, or worker-bound from native NetBox surfaces and the support
  bundle.

### Priority 5: Future Bulk Engines

Treat TurboBulk, parquet-backed loaders, or future NetBox bulk primitives as
apply engines below the existing workflow, not new workflows.

Implementation direction:

1. Keep the public workflow stable:
   - fast bootstrap for trusted initial baselines
   - Branching for reviewable steady-state changes
2. Add future bulk engines behind the existing apply-engine boundary.
3. Require the same parity gates as `bulk_orm`:
   - validation behavior
   - object change tracking
   - Branching semantics where applicable
   - row-level issue behavior
   - support-bundle evidence

Completion signal:

- A faster engine can be enabled or disabled without changing NQE contracts,
  sync definitions, or operator workflow.

### Priority 6: Release Gates And Regression Evidence

Keep architecture changes from regressing field stability.

Implementation direction:

1. Keep `architecture-completion-audit`, `architecture-audit-check`,
   `harness-check`, and focused Django tests green.
2. Keep NetBox version coverage in one CI matrix instead of branch-specific
   workflows.
3. Keep destructive/chaos recovery evidence fresh when recovery behavior
   changes.
4. Keep support-bundle output contract stable as it becomes the primary field
   diagnostic artifact.

Completion signal:

- Releases can prove state recovery, support export, query pushdown, and
  version-gated behavior before packaging.

## Validation

Minimum validation for this planning artifact:

```bash
poetry run invoke harness-check
poetry run invoke architecture-audit-check
poetry run invoke check
```

Future implementation tranches should add targeted tests for the specific
behavior they change, then run the broader gates before release.

## Rollback

This document is non-runtime planning state. If it conflicts with the main
roadmap, refresh it from:

- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- `docs/03_Plans/active/2026-05-23-architecture-state-and-remaining-work.md`

Runtime changes should remain independently reversible by workstream:

- fallback remediation should preserve existing fallback modes
- apply-engine expansion should keep adapter fallback
- scheduler work should be feature-gated or bounded by ledger state
- future bulk engines should be removable without changing sync definitions

## Decision Log

- The next architecture work should optimize measured runtime bottlenecks, not
  add another workflow.
- The main risk has shifted from missing resumability to runtime efficiency and
  clear field diagnostics.
- `bulk_orm` and future bulk engines are acceleration surfaces, not sources of
  row truth.
- Scheduler overlap should be added only after throughput evidence proves it is
  the bottleneck.
- Branching guidance should remain conservative; speed improvements should come
  from less duplicated work, safer fetch scoping, parity-proven apply engines,
  and better capacity tuning.
