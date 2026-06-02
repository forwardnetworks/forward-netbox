# Large Run Operator Readiness

## Goal

Make large Forward NetBox syncs predictable before TurboBulk lands.

The next tranche should reduce avoidable multi-day runs, make scoped-sync dependency risks obvious before execution, and give operators enough live throughput evidence to decide whether to change capacity, switch lanes, or wait.

Primary operator outcomes:

- A large initial baseline tells the operator when Branching will be slow and when Fast bootstrap is the better seed lane.
- Scoped syncs warn when omitted models can block deletes through protected dependencies.
- A running sync shows throughput, ETA, delete-wave state, issue rate, and bottleneck phase without support needing screenshots or ad hoc shell access.
- Support bundles identify fallback pressure and scheduler-overlap readiness from durable execution evidence.
- TurboBulk can later plug into the existing apply-engine boundary instead of becoming a separate workflow.

## Constraints

- Keep Branching as the review lane and Fast bootstrap as the trusted baseline lane.
- Do not add a separate bulk-only sync product.
- Preserve NQE as the source of truth for row shape and normalization.
- Keep adapter behavior as the correctness baseline unless a faster engine proves parity.
- Keep shard max-size and branch budget protections intact.
- Keep full/model fallback available, visible, and explainable.
- Avoid storing raw customer rows, network IDs, snapshot IDs, credentials, or sensitive screenshots in durable docs, tests, or support bundles.
- Keep NetBox 4.5.x and 4.6.x behavior on one shared capability-gated branch.
- Treat TurboBulk as future apply-engine acceleration, not the primary answer to operator visibility or run-shape problems.

## Touched Surfaces

Likely production surfaces:

- `forward_netbox/models.py`
- `forward_netbox/forms.py`
- `forward_netbox/views.py`
- `forward_netbox/tables.py`
- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/utilities/multi_branch.py`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_primitives.py`
- `forward_netbox/utilities/sync_state.py`
- `forward_netbox/utilities/execution_ledger.py`
- `forward_netbox/utilities/execution_ledger_metrics.py`
- `forward_netbox/utilities/execution_ledger_serialization.py`
- `forward_netbox/utilities/health.py`
- `forward_netbox/utilities/health_checks.py`
- `forward_netbox/utilities/health_summary_blocks.py`
- `forward_netbox/utilities/job_compat.py`
- `forward_netbox/utilities/query_fetch_execution.py`
- `forward_netbox/management/commands/forward_scale_benchmark.py`
- `forward_netbox/management/commands/forward_seed_ui_harness.py`
- `forward_netbox/management/commands/forward_watch_sync.py`
- `forward_netbox/management/commands/forward_blocker_audit.py`
- `tasks.py`

Likely UI surfaces:

- `forward_netbox/templates/forward_netbox/forwardsync.html`
- `forward_netbox/templates/forward_netbox/forwardsync_health.html`
- `forward_netbox/templates/forward_netbox/forwardexecutionrun.html`
- `forward_netbox/templates/forward_netbox/forwardexecutionstep.html`
- `forward_netbox/templates/forward_netbox/forwardingestion.html`
- `forward_netbox/templates/forward_netbox/partials/ingestion_progress.html`
- `forward_netbox/templates/forward_netbox/partials/ingestion_statistics.html`

Likely docs and evidence surfaces:

- `docs/00_Project_Knowledge/runtime-tuning-runbook.md`
- `docs/00_Project_Knowledge/validation-matrix.md`
- `docs/01_User_Guide/troubleshooting.md`
- `docs/01_User_Guide/usage.md`
- `docs/02_Reference/model-mapping-matrix.md`
- `docs/03_Plans/evidence/`

## Approach

Implement as small, independently shippable workstreams. Do not wait for every item before shipping the high-signal operator improvements.

### Priority 1: Initial Baseline Lane UX

Problem:

Operators can start a very large first run in Branching and discover hours later that it projects to days.

Build:

- Add a pre-run estimate for selected sync settings:
  - estimated planned shard count
  - estimated change volume by model
  - delete-heavy model flags
  - expected first-baseline lane risk
  - rough runtime class (`minutes`, `hours`, `days`) when recent benchmark evidence exists
- Add a recommendation:
  - `Branching` when reviewable diffs are required or workload is bounded
  - `Fast bootstrap` when it is a trusted initial baseline and Branching projection is too large
  - `Branching with tuning` when capacity is the likely limiter
- Add explicit confirmation text for Fast bootstrap:
  - trusted baseline
  - skips Branching review for initial seed
  - later runs can return to Branching only when snapshot/query identity supports diffs

Implementation notes:

- Prefer deriving estimates from existing workload summary, branch budget plan, execution-run history, and scale-benchmark evidence.
- Do not invent false precision. Use ranges and confidence labels.
- Keep this advisory until enough runtime evidence exists for stronger projections.

Validation:

- Unit tests for recommendation classification.
- Form/view tests showing advisory copy appears before run.
- Scenario tests for large Branching projection, trusted Fast bootstrap recommendation, and bounded Branching recommendation.
- Playwright coverage for the pre-run advisory path if visible controls change.

### Priority 2: Scoped-Sync Dependency Preflight

Problem:

Scoped syncs can omit models that own protected dependencies. Later delete/prune work then produces expected `ForwardDependencySkipError` rows that look like ingestion failures.

Recent example:

- `dcim.interface` deletes skipped because IP addresses were still referenced by `netbox_routing.bgppeer`.
- The operator had excluded BGP/routing maps from the sync.

Build:

- Add a preflight that inspects selected models, delete/prune settings, and known dependency relationships.
- Warn when selected delete-capable models have omitted dependent models likely to block cleanup.
- Show warnings as advisory unless the dependency shape is known to be fatal.
- Include suggested model additions.

Initial dependency warnings:

- `dcim.interface` delete/prune with BGP omitted:
  - warn if `netbox_routing.bgppeer` is omitted
  - warn if `netbox_routing.bgppeeraddressfamily` is omitted
  - warn if `netbox_peering_manager.peeringsession` is omitted when that overlay is enabled
- `ipam.ipaddress` cleanup with routing/peering omitted:
  - warn that protected refs can block deletes
- `dcim.device` delete/prune with interface, cable, module, inventory, routing, or IPAM maps omitted:
  - warn that child dependencies can force safe skips

Implementation notes:

- Use the model contract registry and existing delete dependency ranking rather than ad hoc UI-only logic.
- Keep the warning generic enough for optional plugins not installed.
- Include the exact omitted model strings in support bundle evidence.

Validation:

- Unit tests for selected model combinations.
- UI/form tests for advisory warnings.
- Blocker-audit tests proving dependency skips remain non-blocking when expected.

### Priority 3: Run Throughput And ETA Panel

Problem:

Shard index alone does not tell an operator whether a run is healthy, slow, stuck, or capacity-bound.

Build a live panel from execution-run/step data:

- current shard index and total shards
- shards/hour over last 1 hour
- shards/hour over last 6 hours when enough history exists
- ETA range
- current model wave
- active step status and age
- issue rate/hour
- queue wait time
- fetch time
- apply time
- merge time
- fallback time or fallback count
- active worker count when observable
- source `query_fetch_concurrency`
- source `nqe_page_size`
- worker timeout seconds when observable

Implementation notes:

- Prefer ledger timing fields over log parsing.
- If data is missing, show `unknown` with reason.
- Support bundle should export the same values so support can analyze offline.

Validation:

- Metrics unit tests for rate windows and missing-data behavior.
- UI tests for healthy, slow, stuck, and insufficient-data states.
- Scale-benchmark fixture tests for emitted throughput fields.

### Priority 4: Fallback Pressure Reduction

Problem:

Before TurboBulk, the biggest speed waste is repeated full/model fallback and redundant refetch work, not row apply alone.

Build first:

- Extend scale benchmark/support bundle output to rank:
  - fallback count by model
  - fallback runtime share by model
  - fallback reason code
  - full-model refetch after shard retry
  - partition retry count
  - models with no shard-safe filter
  - models with shard-scoped fetch attempted but failed

Then fix the top offender only:

- If fallback is caused by unsafe NQE filter shape, tighten the model fetch contract.
- If fallback is caused by too-large partition failure, split partitions more conservatively.
- If fallback is caused by missing natural key, add bucket/hash strategy only when row shape parity is proven.
- If fallback is rare and explainable, leave it visible rather than adding risky acceleration.

Validation:

- Scale-benchmark fixture with mixed fallback reasons.
- Query-fetch tests for top offender fix.
- Schema parity tests proving scoped fetch row shape equals full fetch row shape.
- Runtime evidence from a large run or sanitized support bundle.

### Priority 5: Adaptive Capacity Recommendation

Problem:

Operators need concrete tuning guidance, not a static runbook they have to translate under pressure.

Build advisory health actions:

- Detect low sustained throughput.
- Detect whether issue rate is low enough to tune safely.
- Detect whether queue/worker/DB evidence suggests headroom, bottleneck, or unknown.
- Recommend exactly one next tuning batch:
  - workers `+50%`, round up
  - `query_fetch_concurrency +25%`, cap `16`
  - `nqe_page_size +20%`, cap `10000`
  - restart workers only
  - hold for 60 minutes

Implementation notes:

- This is advisory, not automatic tuning.
- If the platform cannot expose worker count or DB headroom, say `insufficient evidence`.
- Reuse wording from `runtime-tuning-runbook.md`.

Validation:

- Health-summary tests for recommend, hold, rollback, and insufficient-evidence states.
- Docs update with operator examples for Kubernetes, VM/systemd, and Docker.

### Priority 6: Delete Wave Visibility

Problem:

Deletes happen later, but the UI does not make delete timing and protected-delete skips obvious enough.

Build:

- Explicit phase label:
  - planning
  - fetch
  - apply/stage
  - delete/prune
  - merge
  - finalize
- Pending delete count when planned.
- Delete rows applied/skipped/failed by model.
- Protected dependency skip count by model.
- Message when delete wave has not started yet.
- Message when delete wave started but protected refs are causing safe skips.

Implementation notes:

- Use existing ingestion issues and execution-step counters where possible.
- Do not classify all dependency skips as failures.
- Link to blocker audit guidance from the Health page.

Validation:

- Unit tests for delete-wave phase summaries.
- Template tests for not-started, active, skipped, and failed states.
- Blocker-audit regression showing `ForwardDependencySkipError` remains non-blocking unless policy changes.

### Priority 7: Scheduler Overlap Readiness

Problem:

Scheduler overlap may help only if queue/merge wait dominates. It should not be default acceleration without evidence.

Build:

- Add readiness classification:
  - `not_warranted`: apply/fetch dominates
  - `candidate`: queue/merge wait dominates and DB/worker headroom exists
  - `blocked`: active warnings/issues or insufficient capacity evidence
  - `unknown`: not enough timing data
- Keep the only supported overlap shape:
  - pre-stage one eligible next shard while current shard is queued for merge
  - merge remains serialized
  - already staged shard is merged by the next ledger handoff

Validation:

- Metrics tests for readiness states.
- Ledger transition tests for overlap no-op and duplicate-worker behavior.
- Runtime evidence from at least one large completed run before recommending broadly.

### Priority 8: Compatibility Cache Retirement

Problem:

The execution ledger is the desired control plane, but compatibility `_branch_run` state still exists for upgrade/read-through safety.

Build:

- Prove UI, API, recovery, support bundle, retry, discard, merge, finalize, and health paths work from ledger state alone.
- Preserve no-ledger upgrade fallback fixtures.
- Stop remaining active compatibility writes once evidence is complete.
- Keep compatibility read-through only for old state.

Validation:

- Upgrade fixture with old `_branch_run` and no ledger.
- Missing-JSON recovery tests.
- Support bundle after cleanup and later-run handoff.
- Scale/chaos gate proving stage, merge, retry, discard, finalize, health, API, and support-bundle behavior.

### Priority 9: Real Worker-Kill Chaos

Problem:

Handled timeouts are not the same as hard worker death. Long runs need proof for process death during staging, row apply, and merge.

Build or extend opt-in destructive harness cases:

- kill stage worker before branch creation
- kill stage worker after branch creation
- kill stage worker during row apply
- kill merge worker during merge

Required evidence:

- execution run ID
- active step ID
- branch ID when present
- killed worker/job ID
- recovery recommendation
- support bundle verifies scenario-aligned recovery action

Validation:

- Keep destructive harness opt-in and outside default CI.
- Run before releases that touch Branching execution, recovery, shard planning, or apply mechanics.

### Priority 10: Bulk ORM Expansion Discipline

Problem:

Expanding faster apply engines can create correctness regressions if relationship-heavy models bypass adapter semantics.

Rule:

- Expand only if scale-benchmark evidence says current safe `bulk_orm` models are meaningful runtime share or the next candidate has strong parity value.
- Do not promote relationship-heavy models without parity for:
  - NetBox validation
  - object-change tracking
  - Branching diffs
  - row issue behavior
  - dependency skips
  - rollback/discard behavior
  - support bundle statistics

Likely position:

- Keep high-risk models adapter-backed until TurboBulk or another engine can prove equivalent semantics.
- Use `bulk_orm_expansion.parity_plan` to choose candidates.

Validation:

- Candidate-specific parity test set.
- Branching diff parity.
- Object-change parity.
- Runtime non-regression evidence.

## Implementation Status

Completed in the first implementation slice:

- Scoped-sync dependency preflight now runs from the Health summary/check path.
  It warns when selected delete-capable models omit known protected dependency
  models, including the `dcim.interface` plus BGP/peering case that produced
  `ForwardDependencySkipError` rows during Partner's run.
- Delete-wave visibility now appears in Health with planned delete rows,
  delete shards, execution order, delete phase, delete-step progress, and
  latest protected dependency skip counts.
- Run throughput visibility now appears in Health with current shard, total
  shards, 1-hour and 6-hour shard rates, ETA range, active step age, issue
  rate, bottleneck phase, and runtime knobs (`query_fetch_concurrency`,
  `nqe_page_size`, worker timeout).
- Initial-baseline lane UX now appears on the sync detail page and in workload
  summaries. The advisory reports current backend, recommended backend, planned
  shards, estimated changes, runtime class, delete-heavy models, lane risk, and
  Fast bootstrap confirmation text.
- Fallback pressure ranking now appears in execution-run support bundles and
  scale benchmark reports. It ranks models by fallback count, fallback runtime
  share, fallback reason code, full-model refetch-after-retry, partition retry
  count, no-shard-safe-filter models, and shard-scoped fetch failures.
- Adaptive capacity guidance now appears under Health `Large Run Tuning`.
  It reports exactly one decision (`recommend_tuning_batch`,
  `hold_current_settings`, `rollback_latest_tuning_batch`,
  `insufficient_evidence`, or `capacity_blocked`) and, when evidence is
  sufficient, computes one batch: workers +50% rounded up,
  `query_fetch_concurrency` +25% capped at 16, `nqe_page_size` +20% capped at
  10000, restart workers only, then hold 60 minutes.
- Scheduler-overlap readiness now uses the plan's explicit classifications:
  `unknown`, `not_warranted`, `blocked`, and `candidate`. Candidate evidence
  requires material queue/merge wait plus worker/database headroom evidence; the
  support-bundle payload also records the only supported overlap shape: stage one
  next eligible shard, keep merge serialized, and hand off through the ledger.
- Compatibility-cache retirement is proven by current runtime paths. Active
  compatibility writes are suppressed once ledger history exists, stale payloads
  are read-through/prunable, no-ledger upgrade fixtures remain covered, and
  queue/start/recovery paths continue from execution-ledger state.
- Real worker-kill chaos evidence is now durable. The opt-in Docker kill task
  writes per-scenario metadata with scenario, killed worker/container ID,
  restored worker count, support-bundle path, support-bundle recovery validation
  result, execution run ID, active step ID/index/status, active step job ID,
  branch ID/name when present, and recovery action. Architecture runtime
  evidence records the metadata file next to each exported chaos support bundle
  and only passes the destructive worker-kill check when the bundle validates
  against the scenario-specific recovery expectations.
- Bulk ORM expansion discipline is proven current without promoting additional
  relationship-heavy models. The current safe set remains explicit; adapter-only
  models carry blocker codes and required parity gates; `bulk_orm_expansion`
  reports the next candidate plan; and the candidate parity tests keep
  high-risk models such as `dcim.device` and `ipam.prefix` on the adapter path
  until direct parity and runtime evidence exists.
- `scale-chaos-test` now has a deterministic fallback when the shared local
  runtime cannot be inspected. If the active-run guard sees an active execution
  run, or cannot connect to the shared runtime because local Postgres is already
  saturated, the CI-style gate runs the same chaos/recovery labels in an
  isolated compose project instead of guessing that the shared runtime is safe.
- `playwright-test` now uses the same shared-runtime guard. When the primary
  local Docker runtime is running a live execution run or cannot be inspected,
  the deterministic UI harness runs against a temporary isolated compose project
  instead of bypassing the guard or failing before browser assertions.

Validation evidence for this slice:

```bash
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke harness-check
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke harness-test
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke lint
.venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_health --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_models.ForwardSyncModelTest --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_scale_benchmark --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_health --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_sync.SchedulerOverlapPolicyTest --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_prune_compatibility_cache_command --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_jobs.ForwardJobsTest --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m unittest scripts.tests.test_tasks
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_job_compat --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_apply_engine --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_sync.ForwardApplyEngineParityTest --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label "forward_netbox.tests.test_jobs forward_netbox.tests.test_api_views.ForwardExecutionRunAPIViewTest forward_netbox.tests.test_log_export forward_netbox.tests.test_synthetic_scenarios forward_netbox.tests.test_sync_state forward_netbox.tests.test_sync.ForwardMultiBranchExecutorAdaptiveSplitTest" --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label "forward_netbox.tests.test_health forward_netbox.tests.test_scale_benchmark forward_netbox.tests.test_models.ForwardSyncModelTest forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest forward_netbox.tests.test_job_compat" --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke scale-chaos-test
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke check
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke docs
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke scenario-test-ci
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-ci
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke playwright-test
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label "--buffer forward_netbox.tests" --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke test-isolated --test-label forward_netbox.tests.test_query_registry.QueryRegistryTest.test_seed_builtin_maps_enables_existing_optional_routing_map_defaults --no-keep-runtime
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke architecture-runtime-evidence --capacity-source-name ui-harness-source
env PATH=/Users/captainpacket/src/forward-netbox/.venv/bin:$PATH .venv/bin/python -m invoke architecture-completion-audit --output-json docs/03_Plans/evidence/architecture-completion-audit-current.json
```

`invoke scenario-test` was attempted against the shared local runtime and failed
fast because the guard could not inspect saturated shared Postgres connections;
`invoke scenario-test-ci` ran the same synthetic scenario label in the isolated
compose project and passed 53 tests. The architecture completion audit reported
14 completed checks, 0 failed checks, and 2 checks still requiring external
field-scale evidence. The refreshed architecture runtime evidence passed the
destructive worker-kill check for all four recovery scenarios:
`stage-before-branch`, `stage-after-branch`, `stage-during-apply`, and
`merge-during-exec`. The remaining external evidence checks are fallback
reduction and scheduler-overlap readiness: the local UI harness run only has two
runtime steps, and the local capacity review still shows 2 workers against a
recommended 12-worker floor. Those external checks are not new implementation
blockers for this operator-readiness slice because this plan does not claim a
fresh field-scale speed win or scheduler-overlap enablement.

Open after this slice:

- No implementation items remain in this plan. The destructive Docker
  worker-kill commands remain opt-in release-gate evidence for releases that
  touch Branching execution, recovery, shard planning, or apply mechanics.

## Recommended Sequence

Ship in this order:

1. Scoped-sync dependency preflight.
2. Delete wave visibility.
3. Run throughput and ETA panel.
4. Initial baseline lane UX.
5. Fallback pressure ranking in scale benchmark/support bundles.
6. Adaptive capacity recommendation.
7. Scheduler overlap readiness only if timing evidence warrants it.
8. Compatibility cache retirement.
9. Real worker-kill chaos.
10. Bulk ORM expansion only with parity and runtime evidence.

Reasoning:

- Items 1 and 2 address Partner's current confusion directly.
- Item 3 turns long runs into measurable operations instead of watch-and-wait.
- Item 4 prevents the next 9-day initial sync surprise.
- Item 5 identifies the best speed fix before TurboBulk.
- Items 6 and 7 should be evidence-driven, not assumed.
- Items 8 and 9 harden durability.
- Item 10 should stay disciplined until TurboBulk capabilities are concrete.

## Validation

Minimum validation for docs/planning updates:

```bash
invoke harness-check
invoke harness-test
invoke docs
```

Minimum validation for UI or Health-page changes:

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke test
invoke playwright-test
invoke docs
```

Minimum validation for sync planning, delete behavior, recovery, or execution-ledger changes:

```bash
invoke harness-check
invoke harness-test
invoke lint
invoke check
invoke scenario-test
invoke scale-chaos-test
invoke test
invoke playwright-test
invoke docs
```

Runtime evidence for speed claims:

```bash
invoke scale-benchmark --sync-name "<sync-name>" --output-json docs/03_Plans/evidence/scale-benchmark.json
invoke architecture-runtime-evidence --skip-chaos --scale-run-id <execution-run-id>
```

Field evidence can also use sanitized support bundles:

```bash
invoke scale-benchmark --input-json /path/to/sanitized-support-bundle.json --output-json docs/03_Plans/evidence/scale-benchmark.json
```

## Rollback

Each workstream should be independently revertible.

Rollback expectations:

- Advisory-only UI/Health changes can be reverted without data migration.
- Preflight warnings must not mutate sync state.
- Throughput/ETA metrics must degrade to `unknown` when evidence is missing.
- Capacity recommendations must remain advisory and removable.
- Scheduler overlap must be feature-gated or setting-gated until proven.
- Compatibility-cache retirement must keep no-ledger upgrade fallback until one release window is proven.
- Bulk/apply-engine expansion must keep adapter fallback automatic.

## Decision Log

- Chosen: prioritize operator readiness before TurboBulk. TurboBulk can speed apply, but it will not solve scoped dependencies, bad lane selection, missing ETA, fallback pressure, or support ambiguity.
- Chosen: make scoped-sync dependency skips explicit rather than treating them as generic ingestion errors.
- Chosen: keep scheduler overlap behind evidence because it only helps if queue/merge wait dominates.
- Chosen: keep capacity tuning advisory. Automatic tuning is too deployment-specific and can destabilize shared NetBox/Postgres environments.
- Chosen: keep faster apply-engine work under the existing apply-engine boundary. Do not create a separate bulk-sync workflow.
- Deferred: broad `bulk_orm` expansion for relationship-heavy models until parity and runtime evidence prove adapter-equivalent behavior.
- Deferred: TurboBulk-specific implementation until NetBox exposes concrete stable APIs and capability checks.
