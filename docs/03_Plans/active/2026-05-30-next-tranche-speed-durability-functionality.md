# Next Tranche: Speed, Durability, And Functionality

## Goal

Define the next concrete tranche after the current large-run operator-readiness
work. The purpose is to make very large Forward-to-NetBox syncs faster, more
durable, and easier to operate while TurboBulk or future NetBox-native bulk
capabilities mature.

This is not a replacement architecture. It is a prioritized execution plan for
improving the existing native sync path:

1. Use real large-run evidence to identify the first bottleneck.
2. Reduce avoidable Forward query and fallback cost before adding scheduler
   complexity.
3. Harden recovery against real worker death.
4. Keep faster apply engines behind the existing apply-engine boundary.
5. Make TurboBulk a capability-gated engine, not a separate sync workflow.

## Constraints

- NQE remains the source of truth for row shape, normalization, filtering,
  coalescing, and model identity.
- NetBox-native writes remain the only mutation path.
- Branching remains the reviewable steady-state lane.
- Fast bootstrap remains the explicit trusted-baseline lane for very large
  initial seeds.
- `bulk_orm`, TurboBulk, parquet loaders, or future NetBox-native bulk APIs
  must plug into the existing apply-engine selector.
- The execution ledger remains the orchestration and recovery control plane.
- Scheduler overlap is evidence-gated and must keep merges serialized.
- Do not commit customer identifiers, tenant labels, network IDs, snapshot IDs,
  credentials, screenshots, or private row examples.
- Keep NetBox 4.5.x and 4.6.x behavior on one shared capability-gated branch.
- Do not promote relationship-heavy models to a faster engine without adapter
  parity proof.

## Touched Surfaces

Likely production surfaces:

- `forward_netbox/utilities/query_fetch_execution.py`
- `forward_netbox/utilities/apply_engine.py`
- `forward_netbox/utilities/apply_engine_bulk.py`
- `forward_netbox/utilities/apply_engine_decision.py`
- `forward_netbox/utilities/model_contracts.py`
- `forward_netbox/utilities/execution_ledger.py`
- `forward_netbox/utilities/execution_ledger_metrics.py`
- `forward_netbox/utilities/execution_ledger_serialization.py`
- `forward_netbox/utilities/health.py`
- `forward_netbox/utilities/health_checks.py`
- `forward_netbox/utilities/health_summary_blocks.py`
- `forward_netbox/utilities/scale_benchmark.py`
- `forward_netbox/management/commands/forward_architecture_completion_audit.py`
- `forward_netbox/management/commands/forward_scale_benchmark.py`
- `forward_netbox/management/commands/forward_chaos_probe.py`
- `tasks.py`

Likely docs and test surfaces:

- `docs/00_Project_Knowledge/runtime-tuning-runbook.md`
- `docs/00_Project_Knowledge/validation-matrix.md`
- `docs/00_Project_Knowledge/release-playbook.md`
- `docs/01_User_Guide/troubleshooting.md`
- `docs/01_User_Guide/usage.md`
- `docs/02_Reference/model-mapping-matrix.md`
- `docs/03_Plans/evidence/`
- `forward_netbox/tests/test_apply_engine.py`
- `forward_netbox/tests/test_health.py`
- `forward_netbox/tests/test_scale_benchmark.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_synthetic_scenarios.py`

## Approach

Work in small releaseable slices. Each slice must either ship an operator-visible
improvement or produce evidence that prevents speculative complexity.

### P0: Field-Scale Evidence Intake

Problem:

Current unit, scenario, and synthetic coverage prove invariants, but the next
speed decision should be made from a completed large run or a sanitized support
bundle.

Build:

- Capture a completed large-run support bundle after the current field run
  finishes.
- Run architecture/runtime evidence against that bundle or completed run.
- Record:
  - total runtime
  - model wave timing
  - shard throughput windows
  - fallback pressure by model and reason
  - partition retry pressure
  - fetch/apply/merge/wait split
  - delete wave timing and protected dependency skips
  - scheduler-overlap readiness
  - runtime knobs and capacity evidence
- Produce a decision table:
  - fix fallback first
  - tune runtime capacity first
  - expand apply engine first
  - keep waiting for TurboBulk because current bottleneck is Branching merge
  - defer because remaining pressure is low or explainable

Completion signal:

- A committed plan/evidence note references the sanitized artifact path.
- No speed claim depends on screenshots or operator memory.
- `architecture-completion-audit` is green except for explicitly deferred
  future capability gates.

### P0: Fallback And Partition Top-Offender Fix

Problem:

Before TurboBulk, repeated full/model fallback can multiply Forward query cost
and dominate runtime more than row apply.

Build:

- Use support-bundle `fallback_pressure` output to pick exactly one top offender.
- Classify the offender:
  - unsafe NQE filter shape
  - Forward API operator retry that succeeds
  - partition retry that falls through to full/model fetch
  - missing shard-safe natural key
  - intentionally unshardable model
- Fix at the narrowest correct layer:
  - NQE/query contract for filter-shape defects
  - query-fetch execution for avoidable API retry defects
  - partition split heuristics for oversized partitions
  - model contract only when row-shape parity is proven
- Keep rare or explainable fallback visible instead of hiding it behind
  aggressive local filtering.

Completion signal:

- The top offender either disappears from repeated evidence or has a bounded,
  documented reason.
- Scale-benchmark fixtures cover the reason code and before/after behavior.

### P0: TurboBulk Capability Map

Problem:

TurboBulk is likely the biggest future apply-speed lever, but waiting for it
without an integration contract risks losing time and creating a second workflow.

Build:

- Add a capability map for TurboBulk availability:
  - NetBox version support
  - installed package/API availability
  - model support
  - create/update support
  - delete support
  - validation and error reporting shape
  - object-change and Branching behavior
  - rollback/discard behavior
- Add a dormant apply-engine decision branch for `turbobulk` only when the
  capability probe passes.
- Keep `adapter` as the correctness baseline and `bulk_orm` as the currently
  proven faster engine.
- Do not expose TurboBulk as generally selectable until at least one model
  passes parity and runtime evidence.

Completion signal:

- Health and support bundles can say `turbobulk_unavailable`,
  `turbobulk_available_not_enabled`, `turbobulk_candidate`, or
  `turbobulk_enabled_for_safe_models`.
- No runtime code imports or calls TurboBulk unless the capability gate passes.

### P1: First TurboBulk Or Faster-Engine Candidate

Problem:

The first model promoted to TurboBulk has to prove the integration boundary
without taking on relationship-heavy semantics.

Candidate rules:

- Prefer simple, high-volume, low-relationship models first.
- Do not start with `dcim.interface`, `dcim.device`, cable, IP assignment, or
  BGP relationship-heavy models.
- Candidate must be material in runtime evidence, not just theoretically large.

Required parity gates:

- create behavior
- update behavior
- delete behavior where supported
- NetBox validation failure behavior
- row issue behavior
- dependency skip behavior
- object-change tracking
- Branching diffs
- rollback/discard behavior
- support-bundle statistics
- runtime non-regression

Completion signal:

- Candidate-specific tests pass for adapter and faster-engine behavior.
- A support bundle or scale benchmark shows the model selected the faster
  engine and preserved issue/recovery accounting.

### P1: Real Worker-Kill Chaos Release Gate

Problem:

Handled timeouts are not the same as hard worker death. Long runs need proof
for process death during staging, row apply, and merge.

Build:

- Extend the opt-in destructive harness evidence to include:
  - scenario
  - execution run ID
  - active step ID
  - branch ID when present
  - killed worker or container ID
  - restored worker count
  - recovery recommendation
  - exported support-bundle path
- Preserve the current scenario set:
  - kill stage worker before branch creation
  - kill stage worker after branch creation
  - kill stage worker during row apply
  - kill merge worker during merge
- Keep destructive chaos outside default CI.

Completion signal:

- Each scenario exports durable metadata plus a support bundle.
- The release playbook requires the destructive gate before releases touching
  Branching execution, recovery, shard planning, or apply mechanics.

### P1: Capacity Tuning Burn-In

Problem:

Operators need to know whether speed improved because the code improved or
because runtime capacity changed.

Build:

- Standardize capacity evidence for large-run benchmarks:
  - worker count
  - worker timeout
  - `query_fetch_concurrency`
  - `nqe_page_size`
  - PostgreSQL settings
  - database/storage placement
  - host CPU and memory availability
  - queue wait and worker saturation
- Run a controlled burn-in with one tuning batch at a time:
  - workers +50%, rounded up
  - `query_fetch_concurrency` +25%, capped at 16
  - `nqe_page_size` +20%, capped at 10000
  - restart workers only
  - hold for 60 minutes
- Record whether throughput, issue rate, and fallback pressure improved.

Completion signal:

- The Health adaptive-capacity recommendation matches observed burn-in results.
- Support can tell a customer exactly whether to tune, hold, or roll back.

### P1: Delete And Dependency Operator Polish

Problem:

The current large-run tranche adds delete-wave visibility and dependency
preflight. The next tranche should turn that into an operator loop.

Build:

- Add blocker-audit output that groups protected dependency skips by suggested
  model inclusion.
- Add a troubleshooting section for expected `ForwardDependencySkipError`
  during scoped syncs.
- Add a "rerun with these models included" recommendation when omitted models
  are known.
- Keep protected dependency skips non-fatal unless policy explicitly changes.

Completion signal:

- A scoped sync that omits BGP/routing/peering dependencies produces clear
  preflight and post-run guidance.
- Support bundle evidence includes omitted model strings and skip counts without
  private row data.

### P2: Scheduler Overlap Implementation Decision

Problem:

Scheduler overlap can help only when queue or merge wait dominates after normal
tuning. It can also create ordering risk if implemented too broadly.

Decision rule:

- Enable or expand overlap only when repeated evidence says:
  - queue/merge wait is a material runtime share
  - worker/database/storage headroom exists
  - issue rate is low
  - fallback pressure is low or explainable
  - branch budget remains respected

Supported shape:

- Stage at most one eligible next shard while the current shard is queued for
  merge.
- Keep merge serialized.
- Hand off the already staged shard through the execution ledger.
- Export overlap decisions in support bundles.

Completion signal:

- Evidence moves readiness to `candidate` for repeated runs.
- Ledger transition tests prove no duplicate claims, unsafe ordering, or
  concurrent merge behavior.

### P2: Support Bundle Self-Service

Problem:

Support bundles now carry richer evidence, but operators still need a concise
triage answer.

Build:

- Add a top-level `operator_next_action` summary:
  - `wait`
  - `tune_capacity`
  - `include_dependency_models`
  - `fix_fallback_top_offender`
  - `run_recovery`
  - `open_issue_with_bundle`
- Add a short reason list and exact supporting metrics.
- Keep raw customer rows out of the bundle.

Completion signal:

- A support engineer can answer "what should I tell the customer next?" from
  the bundle without reading every execution step.

### P2: NetBox 4.6 Capability And CI Hardening

Problem:

The plugin should support NetBox 4.5.x and 4.6.x without forked behavior or
guessing which APIs exist.

Build:

- Keep 4.6 behavior behind explicit capability checks.
- Add or maintain CI coverage for the supported 4.5 and 4.6 matrix.
- Document version-specific capabilities in reference docs.
- Avoid version-string branching where feature probes are safer.

Completion signal:

- CI matrix is green for supported NetBox versions.
- Health/support evidence can report capability availability when relevant.

## Recommended Execution Order

1. Finish current large-run operator-readiness work.
2. Capture completed field-scale evidence or a sanitized support bundle.
3. Fix the single largest fallback or partition offender if evidence shows one.
4. Add worker-kill metadata and make destructive chaos a release gate.
5. Add the TurboBulk capability map behind dormant gates.
6. Choose the first TurboBulk/faster-engine candidate from runtime evidence.
7. Run candidate parity tests and a controlled benchmark.
8. Expand scheduler overlap only if capacity-backed evidence repeatedly says it
   is a candidate.
9. Polish support-bundle `operator_next_action` after the evidence fields are
   stable.
10. Keep NetBox 4.6 capability and CI hardening running in parallel.

## Validation

Docs-only updates:

```bash
invoke harness-check
invoke harness-test
invoke docs
```

Query-fetch or fallback changes:

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke test-isolated --test-label forward_netbox.tests.test_scale_benchmark --no-keep-runtime
invoke test-isolated --test-label forward_netbox.tests.test_synthetic_scenarios --no-keep-runtime
invoke docs
```

Apply-engine or TurboBulk capability changes:

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke test-isolated --test-label forward_netbox.tests.test_apply_engine --no-keep-runtime
invoke test-isolated --test-label forward_netbox.tests.test_sync.ForwardSyncRunnerTest --no-keep-runtime
invoke test-isolated --test-label forward_netbox.tests.test_health --no-keep-runtime
invoke scenario-test
invoke docs
```

Execution-ledger, recovery, scheduler, or chaos changes:

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke test-isolated --test-label forward_netbox.tests.test_sync.SchedulerOverlapPolicyTest --no-keep-runtime
invoke test-isolated --test-label forward_netbox.tests.test_jobs.ForwardJobsTest --no-keep-runtime
invoke scenario-test
invoke scale-chaos-test
invoke docs
```

Runtime evidence:

```bash
invoke scale-benchmark --sync-name "<sync-name>" --output-json docs/03_Plans/evidence/scale-benchmark.json
invoke architecture-runtime-evidence --skip-chaos --scale-run-id <execution-run-id>
```

Sanitized support-bundle evidence:

```bash
invoke scale-benchmark --input-json /path/to/sanitized-support-bundle.json --output-json docs/03_Plans/evidence/scale-benchmark.json
```

## Rollback

- Planning/doc-only changes can be reverted without data cleanup.
- Fallback/query-fetch changes should be feature-gated or revertable by model
  contract so the system can return to full/model fetch.
- Faster-engine changes must fall back to `adapter` when capability probes,
  parity gates, or runtime settings fail.
- Scheduler-overlap changes must be disableable per sync without migration.
- Chaos harness metadata changes should be additive and safe to ignore by older
  tooling.

## Decision Log

- Treat TurboBulk as an apply engine, not a separate workflow, because the
  existing NQE, ledger, Health, support-bundle, Branching, and Fast bootstrap
  boundaries already give the right operator model.
- Fix repeated fallback before adding scheduler complexity because fallback can
  multiply Forward query work and hide the real bottleneck.
- Keep relationship-heavy models adapter-backed until faster-engine parity is
  proven. Speed without Branching/object-change/error parity is a correctness
  regression.
- Require real worker-kill proof before release claims that touch recovery.
  Timeouts and synthetic stale-state tests are useful but not enough for hard
  process death.
- Use runtime evidence to decide scheduler overlap. It should solve measured
  queue/merge wait pressure, not become default acceleration.
