# 2026-05-23 Architecture State and Remaining Work

## Goal

Capture a clear snapshot of architecture progress and list the remaining
high-impact work for speed, resilience, and operator self-service.

## Constraints

- Keep NQE as the single source of truth for normalization/model shaping.
- Keep NetBox-native mutation paths and Branching-native behavior.
- Avoid customer identifiers and environment-specific sensitive data.

## Touched Surfaces

- `docs/03_Plans/active/2026-05-23-architecture-state-and-remaining-work.md`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- `docs/03_Plans/active/2026-05-23-remaining-architecture-execution-summary.md`

## Approach

### Current State

Architecture work is active and materially advanced. Recent completed tranches:

1. Recovery automation at enqueue/reconcile paths:
   - enqueue-time reconciliation before stage queue decisions
   - stale-step auto-reset/auto-requeue for no-branch/no-live-job conditions
2. Deterministic escalation policy for repeated stale branch conditions:
   - recommendation escalates to `manual_intervention` at threshold
3. Bounded merge-timeout auto-requeue in auto-merge runs:
   - strict retry budget to prevent infinite loops
4. Run-level no-progress watchdog:
   - reason-coded watchdog events and escalation thresholding
5. Per-model fallback budget guardrails:
   - explicit warning signals and tuning guidance in support metrics
6. Architecture completion audit refresh:
   - `13` checks completed
   - `0` failed checks
   - `3` external/runtime evidence items still open
   - destructive worker-kill runtime evidence is fresh
   - field-scale runtime matrix, fallback reduction, and scheduler readiness
     still require larger approved runtime evidence
7. Single-branch multi-version CI matrix:
   - same GitHub validation job now runs against NetBox `v4.5.9` and `v4.6.0`
8. Throughput smoothing instrumentation:
   - support bundles now expose queue/wait/apply/merge timing summaries
   - tuning guidance flags `throughput_wait_pressure` when wait time dominates
9. Runtime fallback reason aggregation:
   - support bundles and Sync Health now count fallback reasons globally and by
     model
10. Large-run tuning summaries:
   - support bundles now include `operator_tuning_summary`
   - Sync Health now includes `large_run_tuning`
   - Sync Health checks now include `Large-run tuning`
   - both surfaces rank first-order actions from diff utilization, fallback
     fetch, timeout/capacity, throughput, bottleneck, and concurrency signals
11. Confidence-informed branch budget policy:
   - branch planning and overflow re-splitting now consume density confidence,
     not just raw learned density
   - high-confidence density drives row budgets, medium-confidence density is
     blended, and low-confidence density falls back to baseline
   - budget policy/rationale is exposed in sync summaries and density telemetry
12. Runtime fallback remediation evidence:
   - fallback summaries now include remediation actions that classify the
     likely fix layer for model-contract fallback, shard pushdown fallback,
     diff fallback, timeout pressure, parameter-contract problems, and unknown
     fallback exceptions
13. Apply-engine parity expansion evidence:
   - Sync Health and architecture audit now expose `bulk_orm_expansion`
   - the expansion summary lists safe models, blocked models, blocker reasons,
     required parity gates, and the next action before any future model
     promotion
14. Scheduler-overlap readiness evidence:
   - support bundles now classify whether scheduler overlap is ready,
     not indicated, missing evidence, or only a candidate after capacity review
   - the readiness summary records dominant wait component and required
     preconditions before future overlap work is enabled
15. Native single-shard filter fallback reduction:
   - full and diff shard fetch paths now retry equivalent single-value
     column-filter operator forms before escalating to full/model fallback
   - this keeps the fix in Forward query execution instead of Python-side row
     mutation
   - count-only retry metadata is retained for support bundles without storing
     row data
   - support bundles and Sync Health aggregate retry counts and avoided
     fallback counts across the run
16. Partition retry tuning guidance:
   - successful partition retries now emit `partition_retry_avoided_fallback`
     guidance so operators can recognize healthy retry recovery
   - failed partition retry attempts now emit `partition_retry_pressure`
     guidance so support can target NQE filter contracts or Forward query
     pressure instead of changing branch budgets first
17. Apply-engine promotion lane planning:
   - `bulk_orm_expansion` now reports deterministic promotion lanes for blocked
     adapter models
   - safe next parity targets are separated from high-impact performance
     targets, keeping speed planning actionable without enabling unproven bulk
     writes
18. Large-run backend advice:
   - Sync Health now includes explicit execution backend advice under Large Run
     Tuning
   - timeout-risk Branching baselines point to Fast bootstrap only for trusted
     first baselines, while active Fast bootstrap points back to Branching for
     steady-state diff review
19. Scale benchmark reporting:
   - `forward_scale_benchmark` and `invoke scale-benchmark` now turn
     execution-run support-bundle metrics into a reusable pass/warn/fail
     benchmark report
   - the report is sanitized by construction because it keeps only run IDs,
     counters, statuses, rates, and first-order actions, not row examples or
     customer identifiers
20. Baseline-to-diff transition evidence:
   - support bundles now expose `diff_baseline_transition`
   - Sync Health shows `Baseline to diff` under Query Runtime & Pushdown
   - scale benchmark reports include a dedicated transition check so operators
     can distinguish active API diffs from Fast bootstrap baseline creation,
     missing query identity, missing/ineligible baselines, or diff fallback
21. Model contract registry baseline:
   - each supported model now has an audited architecture contract that composes
     sync row fields, coalesce identity, shard fetch behavior, delete
     dependency order, apply-engine classification/blockers, and support-safe
     diagnostic fields
   - architecture audit and completion audit now fail when registry coverage is
     incomplete
22. Model contract registry call-site migration, first pass:
   - architecture audit fetch-contract coverage now reads through the registry
   - Sync Health fetch-contract summaries now read through the registry and
     report registry status/gap counts
23. Model contract registry call-site migration, second pass:
   - architecture audit safe bulk-ORM model lists, adapter-required model
     lists, and adapter blocker-code maps now read through registry helpers
   - apply-engine gap detection remains wired to the authoritative
     apply-engine gap checks
24. Model contract registry call-site migration, third pass:
   - query registry built-in defaults now read fallback coalesce fields through
     the registry helper
   - sync execution and query fetch execution now use the same registry helper
     for fallback model coalesce fields
   - row-shape validation remains in `sync_contracts`, preserving the existing
     validation boundary while centralizing row identity fallback reads
25. Model contract registry call-site migration, fourth pass:
   - architecture audit classification gaps now read through registry helpers
     for unclassified supported models, adapter-required models without blocker
     codes, and bulk-ORM-safe models without specs
   - model contract gap detection now derives adapter blocker coverage from the
     contract classification
   - apply-engine runtime selection remains unchanged
26. Capacity profile documentation baseline:
   - `docs/01_User_Guide/configuration.md` now includes small, medium, large,
     and very-large runtime sizing profiles
   - the guidance maps backend choice, query fetch concurrency, page size,
     worker timeout, and first health signals to the existing Sync
     Health/support-bundle workflow
   - NQE remains the source of truth; backend choice only changes review/write
     mechanics
27. Run-local fetch artifact baseline:
   - shard-scoped fetches can now write bounded temporary runtime artifacts for
     retry/resume reuse
   - artifact identity includes execution run, query identity, snapshot,
     baseline snapshot, shard keys, fetch/query parameters, column filters,
     and hashed device-tag scope
   - support metadata includes only artifact status/count/size fields, not row
     payloads
   - artifacts are pruned on normal ledger completion, reconcile-time
     completion, and branch-run failure
28. Delete/dependency planning baseline:
   - plan previews now include `delete_dependency_plan`
   - delete-heavy plans expose delete rows, delete shards, delete share,
     max delete shard size, model execution order, dependency rank, dependent
     model count, and reference-blocker risk
   - warning codes identify delete waves, near-budget delete shards, and
     dependency-anchor models before merge

### What This Means Operationally

- Better resilience against stalled or orphaned stage/merge states.
- Better support-bundle diagnostics for field triage.
- Clearer signals when shard pushdown/fallback behavior regresses.
- Lower operator guesswork for long-running sync failure modes.
- A clearer first diagnostic answer for slow large runs: fix diffs, reduce
  fallback fetch, tune timeout/capacity, inspect workers/DB, adjust query
  concurrency, or choose the correct baseline/review backend.
- Learned branch-density data is safer to act on because confidence now gates
  whether it can tune future shard sizing.
- Fallback-heavy runs now tell operators where to start fixing instead of only
  reporting that fallback happened.
- Faster apply engines have a clear promotion contract; no model moves into
  `bulk_orm` without validation, object-change, Branching, row-issue, and
  runtime evidence, and the next candidate lanes are now ranked in health/audit
  output.
- Built-in query defaults, sync execution, and fetch planning now read row
  identity fallback from the same model contract registry, reducing the chance
  that future model additions drift between planning and apply paths.
- Architecture audit gap reporting now reads classification status from the
  registry, so future model additions have one clearer place to fail fast when
  apply-engine eligibility or blocker metadata is incomplete.
- Operators now have a documented starting point for capacity sizing instead of
  needing support to infer profile class from screenshots or raw row counts.
- Retried shard fetches can avoid repeating the same Forward query when the
  current run already has a valid scoped fetch artifact.
- Filtered syncs now expose delete-wave and dependency-anchor risk in the plan
  preview before operators merge destructive branch work.
- Scheduler overlap is no longer a vague optimization idea; it now has a
  support-bundle decision gate that must show repeated wait pressure and
  capacity headroom before implementation.
- Some avoidable shard-fetch fallbacks now stay shard-scoped when only the
  single-value column-filter operator shape was rejected.
- Operators can see whether shard partition retries are helping from native
  health/support surfaces instead of reading per-step JSON manually, and failed
  retry pressure now has an explicit remediation signal.
- Recovery automation is now a maintained gate for the current architecture
  baseline, not the primary remaining unknown.
- Release candidates now have a repeatable scale-readiness report that uses the
  same evidence operators already export for troubleshooting.
- Operators can now answer why a Branching run did not use API diffs without
  manually correlating query mode, baseline snapshot IDs, and sync mode.
- Developers now have an audited model-contract registry to consult before
  adding speed work or model-specific exceptions.
- The first registry call-site migration proves the intended cleanup path:
  existing health/audit behavior can move behind registry helpers without
  changing NQE contracts, planner semantics, or NetBox mutation behavior.
- The second registry migration moves audit-facing apply-engine contract facts
  behind the same surface while preserving the original failure checks for
  unclassified or underspecified models.

### Remaining Priority Work

1. Model contract registry call-site migration
   - keep the new registry as the first place for model-specific behavior
   - migrate duplicate fetch/delete/apply/diagnostic rule reads into registry
     helpers only when the change is behavior-preserving and test-covered
2. Runtime fallback reduction after attempted shard pushdown
   - use support bundles and trend exports to identify repeated fallback causes
   - keep row semantics in NQE and avoid Python-side mutation workarounds
3. Apply engine parity expansion
   - expand `bulk_orm` only where parity tests prove behavior correctness
4. Throughput smoothing
   - use the queue/wait metrics and scheduler-overlap readiness gate before
     adding bounded overlap or adaptive ceilings
5. Capacity profiles and capability gates
   - turn health/support tuning signals into practical deployment profiles
   - keep NetBox 4.6+ and future bulk features capability-gated on one branch
7. Release gate maintenance
   - keep chaos/support-bundle/architecture audit checks green as new recovery
     or execution behavior is added
   - keep supported NetBox versions in the CI matrix rather than splitting
     branches or workflows

### Monolith/Boundary Cleanup Follow-On

Keep this incremental and test-guarded:

1. Continue tightening contracts between:
   - query fetch
   - planning/budgeting
   - execution/recovery
   - apply adapters
   - delete/dependency planning
   - health/support diagnostics
2. Remove compatibility/legacy shims only after upgrade/runtime evidence proves
   they are no longer needed.
3. Prefer a model contract registry over more per-module special cases.
4. Keep observability surfaces first-class (health + support bundle) for every
   architecture change.

## Validation

Recent architecture tranches were validated with targeted and broader suites,
plus harness gates (`harness-check`, `check`). The latest completion audit was:

```bash
poetry run invoke architecture-completion-audit
```

Result summary:
- completed checks: `13`
- failed checks: `0`
- missing external evidence: `3`

Remaining items should keep the same bar: targeted tests for changed behavior,
then gate-level validation.

## Rollback

This document is status-only. If roadmap and summary diverge, refresh this file
from:

- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- `docs/03_Plans/active/2026-05-23-remaining-architecture-execution-summary.md`

## Decision Log

- Prioritized durable recovery + observability hardening before deeper speed
  tuning because field runs showed stall/diagnostic pain first.
- Kept architecture changes native to NetBox + Branching and NQE-shaped data.
- Recovery behavior is stable enough for the current baseline; the next
  speed-focused risk is avoiding fallback-heavy refetch and proving additional
  apply-engine parity without weakening NetBox semantics.
- Version support should stay matrix-driven in CI on one branch; future NetBox
  minors should be added as capability-gated matrix entries.
- Shard-contract coverage is complete for the current supported model set; the
  remaining speed work is runtime fallback reduction, apply parity, and measured
  scheduler smoothing.
- The model contract registry now exists and is audited; remaining cleanup is
  continued incremental call-site migration rather than a missing architecture
  surface.
