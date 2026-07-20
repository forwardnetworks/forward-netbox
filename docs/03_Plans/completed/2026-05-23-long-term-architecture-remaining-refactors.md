# 2026-05-23 Long-Term Architecture Remaining Refactors

## Goal

Capture the remaining long-term architecture work that should be completed or
explicitly deferred before treating the large-scale Forward-to-NetBox sync
architecture as stable. The focus is speed, reliability, operational clarity,
and keeping one native NetBox workflow.

## Constraints

- NQE remains the source of truth for row shape, normalization, coalescing,
  filtering, and model identity.
- NetBox-native model writes remain the only mutation path.
- Branching remains the reviewable steady-state path.
- Fast bootstrap remains an explicit trusted baseline path for large first
  imports.
- Faster engines such as `bulk_orm`, TurboBulk, parquet-backed loaders, or
  future NetBox bulk primitives must plug below the existing execution
  workflow, not become separate sync products.
- No customer identifiers, network IDs, snapshot IDs, credentials, screenshots,
  or private row examples should be committed.

## Touched Surfaces

- `docs/03_Plans/active/2026-05-23-long-term-architecture-remaining-refactors.md`
- Related architecture docs:
  - `ARCHITECTURE.md`
  - `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
  - `docs/03_Plans/active/2026-05-23-long-term-scale-architecture-direction.md`
  - `docs/03_Plans/active/2026-05-23-architecture-state-and-remaining-work.md`
- Likely future implementation surfaces:
  - `forward_netbox/utilities/query_fetch_execution.py`
  - `forward_netbox/utilities/branch_budget.py`
  - `forward_netbox/utilities/execution_ledger*.py`
  - `forward_netbox/utilities/execution_ledger_metrics.py`
  - `forward_netbox/utilities/health_summary_blocks.py`
  - `forward_netbox/utilities/ingestion_merge.py`
  - `forward_netbox/utilities/sync.py`
  - `forward_netbox/utilities/apply_engine*.py`
  - `forward_netbox/tests/`

## Approach

Use this document as the decision checklist for future long-term architecture
work. A future change should fit one of the refactor lanes below, update the
status/evidence for that lane, and preserve the shared native workflow instead
of creating a separate sync path.

## Current Assessment

The current architecture is in the right shape. The major previous refactors
fixed the most important control-plane problems:

1. Execution is ledger-first instead of compatibility-JSON-first.
2. Fast bootstrap and Branching share the same NQE-shaped row contracts.
3. Shard-scoped fetch contracts and bounded partitioned fetch exist for the
   supported model set.
4. Recovery behavior is reason-coded and visible in support bundles.
5. Sync Health and support bundles expose diff, fallback, recovery, density,
   throughput, capacity, and large-run tuning signals.
6. Fast apply is treated as an engine boundary, with adapters remaining the
   correctness baseline.
7. NetBox version support is intended to stay on one branch through capability
   gates and CI matrix coverage.

The remaining work is not another broad rewrite. It is a set of targeted
refactors that reduce repeated runtime cost, prove faster write paths safely,
and make operator decisions self-service.

## Current Recommendation

Do not re-architect into a new sync product. The long-term architecture should
stay as one NetBox-native workflow with clear engine boundaries underneath it:

1. NQE remains the normalization and row-shaping layer.
2. The model contract registry becomes the single source for model-specific
   fetch, delete, apply, and diagnostic rules.
3. The execution ledger remains the only orchestration control plane.
4. Branching remains the reviewable steady-state path.
5. Fast bootstrap remains the trusted first-baseline path for very large
   initial imports.
6. Faster write paths (`bulk_orm`, future TurboBulk/parquet/native bulk
   primitives) plug into the apply-engine boundary only after parity gates
   pass.

The prior 0.8/0.9 architecture work fixed the control plane: resumability,
state reconstruction, support-bundle evidence, large-run health visibility,
Branching safety, and a shared fast-bootstrap/Branching row contract. The
remaining speed work is runtime economics:

- reduce repeated Forward query work after shard pushdown is attempted.
- reuse scoped fetch work for retry/resume only where it is safe.
- expand faster apply engines model by model after NetBox parity is proven.
- add scheduler overlap only when support evidence proves queue or merge wait
  dominates and capacity exists.
- turn Sync Health/support-bundle signals into practical capacity profiles.

This keeps the project positioned for NetBox 4.6+ capabilities and future bulk
interfaces without splitting into separate code paths for large users.

## Immediate Execution Sequence

1. Finish model contract registry call-site migration.
   - Replace duplicate model-rule reads with registry-backed helpers where the
     change is behavior-preserving and test-covered.
   - Do not add new per-model constants outside the registry unless there is a
     temporary compatibility reason documented in this plan.
2. Use runtime fallback evidence to pick the next fetch optimization.
   - If fallback is caused by query/operator mismatch, fix the NQE/Forward
     execution contract.
   - Run-local fetch artifact reuse now covers the retry/resume economics
     baseline; future fetch work should focus on reducing repeated fallback
     causes or calibrating artifact limits from field evidence.
3. Expand apply-engine acceleration only through parity lanes.
   - Promote one model family at a time.
   - Keep the adapter path as the correctness baseline and automatic fallback.
4. Add scheduler overlap only after evidence says it will help.
   - Required evidence: queue or merge wait dominates runtime across repeated
     runs and worker/database headroom exists.
   - Keep all in-flight state represented in the execution ledger.
5. Convert tuning evidence into self-service docs.
   - Document small, medium, large, and very-large profiles using the same
     metrics already exposed in Sync Health and support bundles.

## Remaining Refactors

### 1) Model Contract Registry

Status: `completed_current_baseline_with_call_site_migration_remaining`

Problem:
- Model behavior is now more structured, but fetch, planning, delete, apply,
  and diagnostics rules still risk spreading across implementation modules.

Target:
- Each supported model has one explicit contract record for:
  - NQE map identity and row shape expectations
  - coalesce identity
  - dependency order
  - safe shard filter fields
  - diff eligibility
  - local safety filter
  - delete behavior
  - apply-engine eligibility
  - safe diagnostic fields

Why:
- This prevents future features from recreating per-model special cases in
  query fetch, planner, sync adapters, and health output.

Implemented baseline:
- `forward_netbox.utilities.model_contracts` now composes each supported
  model's sync row contract, shard fetch contract, delete dependency order,
  apply-engine classification, apply-engine blocker, and support-safe
  diagnostic fields.
- `forward_architecture_audit` now emits `model_contract_registry` and fails
  `--fail-on-gap` when registry gaps are present.
- `forward_architecture_completion_audit` now includes
  `model_contract_registry_complete` as a repo-level architecture check.
- The first call-site migration is complete:
  - architecture audit fetch-contract coverage reads through
    `architecture_fetch_contracts()`.
  - Sync Health fetch-contract summaries read through
    `architecture_fetch_contract_for_model()` and expose registry status/gap
    counts.
- The second call-site migration is complete:
  - architecture audit now reports `bulk_orm_safe_models`,
    `adapter_required_models`, and `adapter_blockers` from registry helpers.
  - apply-engine gap checks still use the authoritative apply-engine gap
    constants, preserving the existing failure behavior.
- The third call-site migration is complete:
  - query registry built-in defaults now read fallback coalesce fields through
    `architecture_default_coalesce_fields_for_model()`.
  - sync execution fallback model coalesce fields now read through the same
    registry helper.
  - query fetch preflight/planning fallback coalesce fields now read through
    the same registry helper.
  - validation and branch-budget shard-key behavior remain in their owning
    modules, preserving the existing behavior boundary.
- The fourth call-site migration is complete:
  - architecture audit classification gap reads now use registry helpers for
    unclassified supported models, adapter-required models without blocker
    codes, and bulk-ORM-safe models without implemented specs.
  - model contract gap detection now derives adapter blocker coverage from the
    contract classification.
  - apply-engine runtime selection remains unchanged.

Remaining migration:
- Move future model-specific logic toward this registry rather than adding new
  independent constants in fetch, planning, apply, delete, or health modules.
- Gradually replace duplicate read paths with registry-backed helpers where the
  change is behavior-preserving and test-covered.

Validation:
- architecture audit fails when a supported model lacks a complete contract.
- targeted tests prove every model resolves fetch, delete, apply, and
  diagnostic behavior through the contract layer.

### 2) Run-Local Fetch Artifact Boundary

Status: `completed_current_baseline`

Problem:
- Shard-scoped fetch exists, but retries can still be expensive when a model or
  partition falls back to broader Forward query execution.

Target:
- Add a run-local, row-safe fetch artifact boundary for retry/resume reuse.
- Store only bounded execution artifacts needed to avoid repeated query work.
- Keep row data out of durable support bundles and do not introduce
  Python-side normalization.

Use carefully:
- This is a runtime cache for repeated fetch economics, not a second source of
  truth.
- The source of truth remains NQE plus the selected Forward snapshot.

Implemented baseline:
- `forward_netbox.utilities.fetch_artifacts` provides runtime-only artifact
  save/load/prune behavior with TTL and byte-limit guards.
- shard-scoped fetches compute artifact identity from execution run, query
  identity, snapshot, baseline snapshot, shard keys, fetch/query parameters,
  column filters, and hashed device-tag scope.
- retried shard fetches can return from the artifact boundary without
  re-running the same Forward query.
- support-safe fetch metadata exposes only artifact key, run ID, status,
  row/delete counts, byte size, expiration, max bytes, and reason.
- artifacts are pruned on normal ledger completion, reconcile-time completion,
  and branch-run failure.

Validation:
- retrying a failed shard does not re-run a full expensive query when a valid
  scoped artifact exists.
- support bundles report whether query work was reused, retried, broadened, or
  discarded.
- artifact cleanup is deterministic at run completion, cancel, and failure.

### 3) Apply Engine Promotion Lanes

Status: `in_progress`

Problem:
- Adapter apply remains correct but slower for large simple models. `bulk_orm`
  can help, but only when parity is proven.

Target:
- Promote faster apply one model family at a time.
- Keep adapter fallback automatic and visible.
- Keep future TurboBulk/parquet/native bulk primitives behind the same engine
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

Validation:
- architecture audit reports the model as parity-proven before enablement.
- synthetic and live smoke evidence shows equal correctness and better or equal
  runtime.

### 4) Evidence-Gated Scheduler Overlap

Status: `planned`

Problem:
- The scheduler is intentionally conservative. That protects correctness, but
  can leave throughput on the table when queue or merge wait dominates.

Target:
- Add bounded prefetch or prestage overlap only when support evidence proves:
  - wait pressure dominates runtime
  - worker and database capacity are available
  - dependency order is explicit
  - every in-flight action is represented in the execution ledger

Non-goals:
- no side queues outside the ledger
- no unbounded concurrent mutations for the same dependency chain
- no branch-budget widening to hide native Branching pressure

Validation:
- worker death during overlapped work reconciles from ledger state.
- support bundles prove overlap was indicated before it is enabled.
- disabling overlap does not change row contracts or apply behavior.

### 5) Delete And Dependency Planning

Status: `completed_current_baseline`

Problem:
- Filtered imports and delete-heavy runs can create large delete waves,
  reference blockers, and confusing progress if deletes are planned like normal
  source rows.

Target:
- Plan delete-heavy work using expected change density and dependency risk.
- Surface preflight delete estimates and likely reference blockers.
- Keep skip/issue aggregation consistent with create/update paths.
- Keep destructive changes visible before merge.

Implemented baseline:
- branch planning separates mixed workloads into apply then delete phases.
- delete workloads execute in dependency order.
- delete-heavy device workloads use conservative row budgets.
- plan previews include `delete_dependency_plan` with delete rows, delete
  shards, delete share, max delete shard size, dependency-ordered model
  execution, per-model dependency risk, and warning codes.

Validation:
- filtered syncs shard delete work predictably.
- reference blockers become row issues or preflight risks, not opaque shard
  failures.
- delete counters reflect actual NetBox operations rather than duplicated
  planning estimates.

### 6) Baseline And Diff Guarantees

Status: `completed_current_baseline_with_calibration_remaining`

Problem:
- Operators need to know whether a run is creating a baseline, doing a first
  reconciliation, or using API diffs.

Current baseline:
- support bundles include `diff_baseline_transition`.
- Sync Health shows `Baseline to diff`.
- scale benchmark reports include a baseline/diff transition check.

Remaining:
- compare repeated large-run support bundles and tighten benchmark thresholds
  only after the transition codes match operator expectations in real runs.

Validation:
- a support bundle can answer:
  - whether a baseline exists
  - whether query identity is diff-capable
  - whether the snapshot pair is eligible
  - whether this run used API diffs or fell back

### 7) Capacity Profiles And Self-Service Operations

Status: `completed_current_baseline`

Problem:
- Large sync performance depends on workers, RQ timeout, Postgres, disk,
  container placement, Forward query runtime, query page size, and query fetch
  concurrency.

Target:
- Document practical small, medium, large, and very-large deployment profiles.
- Tie each recommendation to observable Sync Health/support-bundle signals.
- Keep backend choice separate from capacity advice:
  - Fast bootstrap is for trusted first baselines.
  - Branching is for reviewable steady-state diffs.

Implemented baseline:
- `docs/01_User_Guide/configuration.md` now includes small, medium, large, and
  very-large runtime sizing profiles.
- the profile table ties backend choice, query fetch concurrency, page size,
  worker timeout, and first health signals to the existing Sync Health/support
  bundle workflow.
- the guidance keeps NQE as source of truth and treats backend selection as
  review/write mechanics, not a separate correctness path.

Validation:
- an operator can decide whether to fix diffs, reduce fallback, add capacity,
  tune query concurrency, use Fast bootstrap, or stay on Branching without
  sending screenshots first.

### 8) Capability-Gated NetBox 4.6+ And Future Bulk Features

Status: `planned`

Problem:
- NetBox 4.6+ and future NetBox/TurboBulk capabilities can improve speed, but
  divergent long-lived branches would increase maintenance risk.

Target:
- Keep one code path.
- Add runtime capability probes for version-specific behavior.
- Keep CI matrix coverage for supported NetBox minors.
- Treat future bulk primitives as apply engines under the existing workflow.

Validation:
- supported NetBox versions pass the same behavioral tests with only explicit
  capability-gated differences.
- future fast apply surfaces can be enabled or disabled without changing sync
  definitions or NQE contracts.

## Execution Order

1. Finish any current release-stabilization testing and support-bundle evidence
   review.
2. Implement the model contract registry before adding more model-specific
   performance logic.
3. Use support-bundle fallback metrics to decide whether the next speed tranche
   should be NQE/contract fallback reduction or apply-engine expansion.
4. Promote one additional apply-engine model family only after parity gates
   pass.
5. Add scheduler overlap only if repeated evidence proves wait pressure and
   capacity headroom.
6. Convert capacity and backend tuning guidance into user-facing docs once the
   health/support signals are stable.

## Validation

For this planning artifact:

```bash
git diff --check -- docs/03_Plans/active/2026-05-23-long-term-architecture-remaining-refactors.md
poetry run invoke harness-check
poetry run invoke docs
```

For future implementation tranches:

```bash
poetry run invoke harness-check
poetry run invoke lint
poetry run invoke docs
poetry run invoke architecture-audit-check
poetry run invoke architecture-completion-audit
poetry run invoke check
```

Runtime behavior changes must also include targeted Django tests and support
bundle/Sync Health evidence for the changed surface.

## Rollback

This document is planning state only. Rollback is a normal git revert or file
deletion.

Runtime work derived from this plan must remain independently reversible:

- fetch artifact reuse must preserve existing direct fetch/fallback behavior.
- apply-engine promotion must keep adapter fallback.
- scheduler overlap must be disableable without data migration.
- capability-gated behavior must default to the existing path when a capability
  is absent.

## Decision Log

- The architecture should not be reworked into a second sync product. The next
  improvements should reduce repeated work inside the current native workflow.
- A model contract registry is the cleanest way to prevent future
  per-model behavior from scattering across fetch, planning, apply, and health
  code.
- Faster write paths are useful only when parity is proven; correctness and
  native NetBox behavior remain the release gate.
- Scheduler concurrency should follow evidence. If query runtime or database
  apply dominates, overlap will not solve the real bottleneck.
- Field self-service depends on health/support evidence first and docs second;
  documentation should explain the signals already present in NetBox.
