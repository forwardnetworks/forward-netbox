# 2026-05-24 Long-Term Architecture Next Tranche

## Goal

Capture the remaining long-term architecture work for speed, stability,
scale, and self-service operation after the ledger-first, shard-fetch,
fast-bootstrap, recovery, support-bundle, and apply-engine refactors.

This is not a new architecture. It is the next tranche of work that should be
done inside the current architecture so large Forward-to-NetBox syncs become
faster, more predictable, and easier to operate without developer supervision.

## Constraints

- NQE remains the source of truth for normalization, filtering, coalescing,
  model identity, and row shape.
- NetBox-native model writes remain the only mutation path.
- Branching remains the reviewable steady-state workflow.
- Fast bootstrap remains the explicit trusted-baseline workflow for very large
  first loads.
- Faster write paths such as `bulk_orm`, future TurboBulk, parquet loaders, or
  NetBox-native bulk APIs must stay behind the apply-engine boundary.
- The execution ledger remains the orchestration control plane.
- No customer identifiers, network IDs, snapshot IDs, credentials,
  screenshots, or private row examples belong in committed artifacts.

## Touched Surfaces

- `docs/03_Plans/active/2026-05-24-long-term-architecture-next-tranche.md`
- Related roadmap and closure artifacts:
  - `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
  - `docs/03_Plans/active/2026-05-24-long-term-architecture-closure.md`
  - `docs/03_Plans/active/2026-05-24-long-term-speed-architecture-work.md`
- Future implementation tranches may touch:
  - `forward_netbox/utilities/query_fetch_execution.py`
  - `forward_netbox/utilities/apply_engine.py`
  - `forward_netbox/utilities/apply_engine_decision.py`
  - `forward_netbox/utilities/model_contracts.py`
  - `forward_netbox/utilities/execution_ledger*.py`
  - `forward_netbox/utilities/execution_telemetry.py`
  - `forward_netbox/utilities/health*.py`
  - `forward_netbox/utilities/scale_benchmark.py`
  - `forward_netbox/tests/`

## Approach

Use this file as the concise decision and execution checklist for the next
long-term architecture tranche. The detailed roadmap remains authoritative for
implementation history and validation evidence. Future work should update this
file only when a remaining item is completed, rejected, or materially reshaped
by runtime evidence.

## Current Architecture Position

The architecture is mostly converged. The project should not add a second
large-customer import workflow or move normalization out of NQE. The durable
path is one native NetBox workflow with better economics underneath it:

1. NQE returns normalized, model-shaped rows.
2. The model contract registry describes model identity, dependencies, shard
   fetch behavior, delete behavior, apply-engine eligibility, and safe
   diagnostics.
3. The execution ledger owns run, step, retry, recovery, merge, completion,
   and support-bundle state.
4. Branching remains the reviewable steady-state path.
5. Fast bootstrap establishes a trusted baseline when Branching is too costly
   for a first load.
6. Faster write engines plug into the apply-engine selector without changing
   operator workflow or NQE contracts.

The remaining work is targeted hardening, runtime economics, and release-grade
proof.

## What The Previous Architecture Missed

The 0.8 and 0.9 architecture work fixed the control plane: resumability,
ledger-owned state, visibility, branch sharding, recovery, fast bootstrap, and
support bundles. It did not eliminate the core cost centers of very large
syncs.

Remaining cost centers:

1. Forward query cost when shard pushdown falls back to full/model fetch.
2. Adapter apply cost for high-volume models that are not proven safe for a
   faster apply engine.
3. Native Branching merge/diff cost, which can be sharded and recovered but
   cannot be made free by the plugin.
4. Delete-heavy filtered syncs, where dependency order and reference blockers
   can dominate row volume.
5. Conservative scheduler behavior, which avoids corruption but may leave
   throughput on the table if queue/merge wait is proven to dominate.
6. Capacity drift between intended test settings and actual worker/database/
   storage runtime settings.

## Next Tranche Work

### P0: Close Field-Scale Evidence

Status: `open_runtime_evidence_required`

Objective:

- Produce a completed large-run artifact or sanitized support bundle where run
  state, step state, row failures, fallback, partition retry, diff utilization,
  and scheduler-readiness evidence all agree.

Why:

- Unit and synthetic tests prove invariants. They do not prove field-scale
  runtime behavior.
- The release bar should be a reusable support artifact, not screenshots or
  operator memory.

Completion signal:

- `architecture-completion-audit` is green except for intentionally deferred
  future-capability items.
- Large-run benchmark evidence shows no hidden incomplete steps, bounded row
  failures, explainable fallback, and safe recovery posture.

### P0: Reduce Repeated Fallback

Status: `open_evidence_driven`

Objective:

- Reduce full/model fetch fallback where repeated runtime evidence shows it is
  materially expensive.

Correct fix layer:

- Fix NQE/query contracts when the filter or row shape is wrong.
- Fix Forward query execution handling when the API/runtime behavior causes
  avoidable fallback.
- Use local safety filtering only when it preserves NQE row semantics and is
  explicitly reported as fallback behavior.

Completion signal:

- Repeated large runs show low fallback counts, or each remaining fallback has
  an explicit, bounded, actionable reason.

### P0: Keep Runtime Capacity Honest

Status: `active_guardrail`

Objective:

- Ensure evidence runs measure the intended runtime profile, not a silently
  reset compose default.

Required evidence:

- worker count
- worker timeout
- query fetch concurrency
- NQE page size
- PostgreSQL settings
- disk/container placement
- host CPU and memory availability

Completion signal:

- Runtime evidence includes capacity settings and preserves them through
  evidence collection and chaos-safe probes.

### P1: Expand Apply Engines Only Through Parity Gates

Status: `planned_per_model`

Objective:

- Promote additional models to faster apply engines only when model-specific
  parity is proven.

Required parity gates:

- create behavior
- update behavior
- delete behavior
- validation failure behavior
- row issue behavior
- dependency behavior
- object-change tracking
- Branching behavior
- support-bundle statistics
- runtime non-regression

Completion signal:

- Each promoted model has targeted tests and runtime evidence proving that the
  faster engine matches adapter semantics.

### P1: Add Scheduler Overlap Only If Evidence Justifies It

Status: `deferred_until_capacity_backed_evidence`

Objective:

- Add bounded scheduler overlap only if completed support evidence repeatedly
  shows queue or merge wait dominates after normal tuning.

Rules:

- Overlap must be ledger-derived.
- Overlap must respect dependency order.
- Overlap must respect branch budget and model contracts.
- Overlap must not create a side queue or a second mutation path.
- Overlap must be disableable and visible in support bundles.

Completion signal:

- Support evidence shows queue/merge wait pressure with available worker,
  database, and storage headroom, and overlap tests prove no duplicate claims
  or out-of-order unsafe merges.

### P1: Harden Delete-Heavy Filtered Syncs

Status: `active_followup`

Objective:

- Make filtered-source prune/delete runs predictable before merge.

Required behavior:

- delete waves are planned separately from ordinary source-row volume.
- dependency anchors are visible before merge.
- reference blockers are reported as model-local row issues when possible.
- protected deletes skip cleanly with aggregate statistics.
- delete-heavy shard sizing respects branch-change guidance without treating
  the guidance as a brittle hard cap.

Completion signal:

- Filtered runs can explain delete counts, dependency order, skipped protected
  rows, and reference blockers before an operator commits to merge.

### P1: Finish Model Contract Call-Site Cleanup

Status: `incremental_cleanup`

Objective:

- Keep model-specific behavior centralized in the model contract registry
  instead of rebuilding per-model rules in scattered call sites.

Scope:

- fetch contract lookup
- identity/coalesce metadata
- delete dependency order
- apply-engine eligibility and blockers
- support-safe diagnostics
- shard budget hints

Completion signal:

- New model behavior can be added by updating the contract and adapter tests,
  without editing multiple unrelated planning, health, and support paths.

### P2: Future NetBox 4.6+ And TurboBulk Capability Gates

Status: `future_capability`

Objective:

- Keep NetBox 4.5 and 4.6+ on one branch and treat future bulk features as
  runtime capabilities, not divergent code lines.

Rules:

- NetBox-version-specific behavior must be capability-gated.
- TurboBulk/parquet/native bulk behavior must plug under the apply-engine
  selector.
- CI should cover supported NetBox versions in the same workflow matrix.
- Operator-facing NQE maps and sync workflow should not fork by NetBox version.

Completion signal:

- Future bulk capability can be enabled or disabled per environment without
  changing maps, sync setup, support-bundle shape, or release branch.

## Explicit Non-Goals

Do not spend engineering effort on:

1. A separate non-native import product.
2. Durable customer row storage outside Forward and NetBox evidence.
3. Python-side normalization that belongs in NQE.
4. Widening branch budgets to hide fallback, delete-density, or merge-cost
   problems.
5. Unbounded branch concurrency.
6. Broad module splitting without behavior-preserving tests.
7. Long-lived divergent NetBox 4.5 and 4.6 branches.

## Recommended Execution Order

1. Finish or export a completed large-run evidence artifact.
2. Use that artifact to decide whether fallback, apply cost, delete planning,
   capacity, or scheduler wait is the next real bottleneck.
3. Fix repeated fallback at the correct layer.
4. Promote the next fast apply model only through parity gates.
5. Add scheduler overlap only after capacity-backed evidence proves it is
   warranted.
6. Keep capability-gated NetBox 4.6+/TurboBulk work under the existing
   apply-engine boundary.

## Validation

Planning artifact checks:

```bash
git diff --check -- docs/03_Plans/active/2026-05-24-long-term-architecture-next-tranche.md
python scripts/check_sensitive_content.py docs/03_Plans/active/2026-05-24-long-term-architecture-next-tranche.md
poetry run invoke harness-check
poetry run invoke architecture-completion-audit
```

Runtime proof checks:

```bash
poetry run invoke architecture-runtime-evidence \
  --skip-chaos \
  --scale-run-id <execution-run-id> \
  --capacity-worker-replicas <worker-count> \
  --capacity-source-name <source-name> \
  --capacity-query-fetch-concurrency <concurrency> \
  --capacity-nqe-page-size <page-size>

poetry run invoke architecture-completion-audit
```

## Rollback

This file is a planning artifact and has no runtime effect. If implementation
or field evidence contradicts it, update this document and the active roadmap
with the current evidence instead of preserving stale guidance.

## Decision Log

- Keep one NetBox-native workflow; do not create a second large-customer import
  path.
- Keep NQE as the row normalization and filtering source of truth.
- Keep speed work underneath model contracts, shard fetch, and apply-engine
  boundaries.
- Treat scheduler overlap as evidence-gated future work, not a default
  next step.
- Treat future NetBox 4.6+, TurboBulk, parquet, or native bulk features as
  capability-gated apply engines on the same branch.
- Do not close the architecture proof until a completed large-run artifact or
  sanitized support bundle validates runtime behavior.
