# 2026-05-23 Long-Term Architecture Roadmap

## Goal

Define the remaining long-term architecture work needed to maximize scale,
speed, resilience, and operator self-service for large Forward-to-NetBox
syncs while staying native to NetBox and Branching.

## Constraints

- NQE remains the single source of truth for normalization and model shaping.
- NetBox native model mutations remain the only write path.
- Branching remains the native staged review path for steady-state sync.
- Fast bootstrap remains explicit and optional.
- No customer identifiers, snapshot IDs, credentials, or tenant-specific data
  in committed artifacts.

## Touched Surfaces

- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-position.md`
- `docs/03_Plans/active/2026-05-23-long-term-scale-architecture-direction.md`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-remaining-refactors.md`
- `docs/01_User_Guide/troubleshooting.md`
- `forward_netbox/utilities/query_fetch_execution.py`
- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/utilities/density_learning.py`
- `forward_netbox/utilities/execution_ledger_metrics.py`
- `forward_netbox/utilities/health_summary_blocks.py`
- `forward_netbox/utilities/health_checks.py`
- `forward_netbox/utilities/health.py`
- `forward_netbox/utilities/scale_benchmark.py`
- `forward_netbox/templates/forward_netbox/forwardsync_health.html`
- `forward_netbox/views.py`
- `forward_netbox/utilities/sync_state.py`
- `forward_netbox/utilities/execution_ledger.py`
- `forward_netbox/utilities/execution_ledger_reconciliation.py`
- `forward_netbox/utilities/execution_ledger_serialization.py`
- `forward_netbox/utilities/multi_branch_lifecycle.py`
- `forward_netbox/utilities/multi_branch_executor.py`
- `forward_netbox/utilities/resumable_branching.py`
- `forward_netbox/utilities/ingestion_merge.py`
- `forward_netbox/jobs.py`
- `forward_netbox/management/commands/forward_scale_benchmark.py`
- `forward_netbox/management/commands/forward_execution_run_recovery.py`
- `forward_netbox/management/commands/forward_prune_compatibility_cache.py`
- `forward_netbox/tests/test_log_export.py`
- `forward_netbox/tests/test_health.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_sync_state.py`
- `forward_netbox/tests/test_models.py`
- `forward_netbox/tests/test_jobs.py`
- `forward_netbox/tests/test_ingestion_merge.py`
- `forward_netbox/tests/test_synthetic_scenarios.py`
- `forward_netbox/tests/test_api_views.py`
- `forward_netbox/tests/test_scale_benchmark.py`
- `forward_netbox/tests/test_execution_run_recovery_command.py`
- `forward_netbox/tests/test_prune_compatibility_cache_command.py`
- `docs/00_Project_Knowledge/validation-matrix.md`
- `docs/00_Project_Knowledge/release-playbook.md`
- `scripts/tests/test_tasks.py`
- `tasks.py`
- `ARCHITECTURE.md`

## Approach

1. Keep the roadmap as a living execution ledger with explicit workstream state
   (`not_started` / `in_progress` / `completed` evidence).
2. Execute one architecture-safe speed tranche per cycle and record it in the
   status ledger.
3. Prioritize speed work that preserves existing row contracts and Branching
   semantics.
4. Require validation evidence (`targeted test + check + ci`) before advancing
   workstream status.
5. Keep proposed items ranked by speed/scale impact and implementation risk.

Long-term direction is captured in
`docs/03_Plans/active/2026-05-23-long-term-scale-architecture-direction.md`.
That document is the current north-star for future scale work: one native
NetBox workflow, ledger-first orchestration, NQE-shaped rows, bounded
shard-scoped fetch, parity-gated apply engines, evidence-led scheduling, and
self-service diagnostics.

Remaining structural refactors are captured in
`docs/03_Plans/active/2026-05-23-long-term-architecture-remaining-refactors.md`.
That document should be used when deciding whether a new speed or resilience
change belongs in the model contract layer, fetch engine, planning engine,
execution ledger, apply engine boundary, scheduler, or operations guidance.

The current architecture position is summarized in
`docs/03_Plans/active/2026-05-23-long-term-architecture-position.md`. That
document is the concise decision record for what the prior 0.8/0.9 refactors
solved, what remains a runtime-economics problem, and the recommended execution
order for future speed and hardening work.

The current next-tranche execution checklist is summarized in
`docs/03_Plans/active/2026-05-24-long-term-architecture-next-tranche.md`. That
document captures the remaining speed, stability, scale, and self-service work
without creating a second workflow or moving normalization out of NQE.

The remaining long-term architecture backlog is tracked in that same direction
document under `Long-Term Follow-On Backlog`. It covers the next durable speed
and hardening lanes: reducing repeated Forward query work, proving faster apply
engines model by model, making baseline-to-diff transitions self-evident,
adding evidence-gated scheduler overlap, hardening delete/dependency planning,
building a repeatable scale benchmark harness, documenting capacity profiles,
and keeping future NetBox/TurboBulk capability work on one code path.

## Status Ledger (2026-05-23)

### Overall

- `in_progress`: Roadmap execution is active.
- `completed_this_tranche`:
  - shard partition fetch parallelism for full + diff paths (bounded by
    `query_fetch_concurrency`) with deterministic partition-order merge.
  - support-bundle execution metrics now include per-model fetch-mode counters
    and per-model runtime/fetched-row summaries for faster fallback triage.
  - pushdown efficiency control loop (fallback-rate and pushdown-rate scoring)
    now emits advisory status/messages and hotspot models in both health summary
    and support-bundle execution metrics.
  - pushdown trend snapshots now include cross-run metrics for:
    - fallback rate
    - full-fallback runtime share
    - diff actual ratio (diff steps / diff-eligible steps)
    and are exposed in Sync Health + support-bundle execution metrics.
  - configurable pushdown alert thresholds are now source-managed and applied to
    fallback-rate, fallback-runtime-share, and diff-utilization warning
    decisions in both Sync Health and support-bundle execution metrics.
  - pushdown trend rows now include explicit diff-baseline correlation signals
    for diff-eligible non-diff steps (`non_diff_reason_counts` and
    `baseline_reason_summary`) so operators can separate baseline-ineligible
    runs from diff-request fallback failures.
  - added a dedicated sync-level pushdown trend export endpoint
    (`forwardsync_pushdown_trends`) for long-window retention and offline
    trend analysis beyond support-bundle snapshot limits.
  - added hotspot-aware operator tuning guidance in Sync Health and support
    bundle metrics so fallback/diff/runtime pressure symptoms map to concrete
    recommended actions.
  - runtime-aware branch budget shaping is now applied during stage planning
    and overflow re-splitting using bounded runtime-per-row pressure signals,
    while preserving delete-heavy conservative caps and hard branch limits.
  - adaptive density learning now includes guarded observation acceptance
    (outlier rejection), confidence metadata (sample count, variance, recency),
    and operator-visible learned-vs-default summaries in Sync Health and support
    telemetry surfaces.
  - confidence-informed branch budget auto-tuning is now active:
    - high-confidence learned density shapes branch row budgets directly
    - medium-confidence learned density is blended with the conservative
      baseline
    - low-confidence learned density falls back to baseline behavior
    - budget policy/rationale is exposed with density summaries and branch
      budget hints.
  - compatibility cache retirement is now runtime-complete: active shard/phase
    orchestration no longer mutates compatibility `_branch_run` payloads, and
    legacy payload continuation upgrades to execution-ledger first.
  - native prune tooling (`forward_prune_compatibility_cache` / `invoke prune-compat-cache`)
    is now validated for stale compatibility payload cleanup and reporting.
  - shard-scoped partition fetch now retries failed EQUALS_ANY column-filter
    partitions by splitting them into smaller deterministic batches before
    escalating to full/model fallback.
  - enqueue-time branch-stage reconciliation now runs before queue selection so
    stale execution-step state is corrected before next-shard dispatch.
  - reconcile-time automatic stale-step recovery now:
    - resets stale queued stage steps with no branch/live job to `pending`
      (auto-reset path), and
    - auto-requeues stale running stage steps with no branch/live job to
      `queued` (auto-requeue path),
    while preserving failure/discard paths for branch-associated stale steps.
  - execution support bundles now include explicit recovery-policy markers
    (`recovery_policy_summary`) with per-reason counts and latest event detail.
  - repeated branch-associated stale recovery signals now escalate
    deterministically once threshold is reached:
    - recovery recommendation switches to `manual_intervention`
    - support bundle exposes escalation counters/threshold and required state.
  - merge-timeout recovery now includes bounded automatic requeue in auto-merge
    runs:
    - timeout path attempts automatic merge requeue via native merge job enqueue
    - strict retry budget (`AUTO_MERGE_STALE_MERGE_REQUEUE_LIMIT`) prevents
      unbounded retry loops.
  - run-level no-progress watchdog is now active:
    - reconciliation records `stale_run_no_progress_watchdog` run events on
      stale run heartbeat conditions (with interval guard)
    - support bundle exposes watchdog counters/threshold state
    - recovery recommendation escalates to `manual_intervention` once watchdog
      threshold is reached.
  - orphaned queued-stage recovery is now native-ledger safe:
    - a queued stage with no queued job, branch, or ingestion is treated as an
      invalid in-flight state and reset to `pending` during reconciliation
    - recovery recommendations now flag that state as `reconcile` instead of
      telling operators to wait
    - `forward_execution_run_recovery` / `invoke execution-run-recovery`
      provides an operator-safe way to inspect, reconcile, and optionally
      enqueue the next shard through the native NetBox job path.
  - stale core-job recovery now checks RQ liveness when available:
    - a NetBox core job row marked `running` is no longer treated as live if
      the corresponding RQ job is absent from active queue/started/scheduled/
      deferred registries
    - if RQ state cannot be inspected, reconciliation preserves the previous
      NetBox job-row fallback behavior
    - this protects long dev/test runs from worker auto-restarts or process
      loss that leave stale core job rows behind.
  - per-model fallback budget guardrails are now active in pushdown efficiency:
    - explicit `model_fallback_guardrails` and budget counters are emitted in
      execution/support metrics
    - warning status is raised when a model exceeds fallback budget threshold
      with sufficient step sample depth
    - tuning guidance now includes `model_fallback_budget_guardrail` actions.
  - self-service large-run tuning summaries are now exposed in Sync Health and
    execution support bundles:
    - support bundles include `operator_tuning_summary` with first-order
      tuning actions from bottleneck, fallback, diff, throughput, and
      concurrency signals
    - Sync Health includes `large_run_tuning` so operators can see whether to
      fix diffs, reduce fallback fetch, tune timeout/capacity, or adjust query
      concurrency first
    - the top-level Sync Health check list now includes `Large-run tuning` so
      those actions are visible in the same pass/warn surface as source,
      diff, validation, recovery, capacity, timeout, and pushdown checks.
  - Sync Health large-run tuning now includes `execution_backend_advice`:
    - Branching runs projected near worker timeout recommend Fast bootstrap only
      for trusted first baselines, then Branching afterward
    - active Fast bootstrap runs explicitly recommend switching back to
      Branching after the baseline
    - fallback-heavy Branching runs recommend reducing pushdown fallback before
      changing backend or capacity.
  - runtime fallback reason summaries now include remediation actions, mapping
    common fallback reasons to the correct fix layer (query contract, Forward
    query runtime, diff execution, timeout pressure, or manual classification).
  - apply-engine expansion is now parity-gated in health/audit output:
    - `bulk_orm_expansion` reports safe models, blocked models, blocker codes,
      required parity gates, and the next action before any future model
      promotion
    - Sync Health displays expansion status, blocked-model count, and parity
      gate count.
  - apply-engine expansion now has deterministic promotion lanes:
    - `promotion_lanes` ranks blocked models by safest next parity lane
    - `recommended_next_models` identifies the next low-risk model family to
      prove without changing selected engines
    - `high_impact_blocked_models` separately ranks blocked models by expected
      performance payoff, so speed work can target high-volume models without
      weakening parity gates.
  - scheduler-overlap readiness is now evidence-gated in throughput smoothing:
    - support bundles classify overlap readiness as insufficient evidence, not
      indicated, needs more runtime evidence, or candidate after capacity review
    - readiness records the dominant wait component and required preconditions
      before any future scheduler overlap work is enabled.
  - shard-scoped runtime fallback reduction now retries equivalent
    single-value native column-filter operators before escalating to full/model
    fallback:
    - `DEFAULT` single-value filters retry as `EQUALS_ANY`
    - single-value `EQUALS_ANY` filters can retry as `DEFAULT`
    - full and diff shard fetch paths share the same retry behavior
    - fetch metadata records count-only `partition_retry_summary` evidence
    - support bundles and Sync Health aggregate retry attempts/successes so
      operators can see whether retry logic avoided broader fallback.
  - partition retry telemetry is now converted into operator tuning guidance:
    - successful retry recovery emits `partition_retry_avoided_fallback`
      guidance so operators know the retry path is working and should be
      monitored, not treated as a branch-budget signal
    - failed retry attempts emit `partition_retry_pressure` guidance so
      support can distinguish healthy retry recovery from shard-filter/query
      pressure that still needs remediation.
  - runtime Forward exception text in shard retry/fallback surfaces is now
    sanitized before it reaches NetBox job logs, model diagnostics, or fallback
    metadata:
    - network IDs, snapshot IDs, query IDs, commit IDs, long numeric identifiers,
      API path identifiers, and email addresses are redacted
    - operators still get the actionable error class/message needed to classify
      column-filter, timeout, parameter, or API-runtime fallback causes
    - this keeps support bundles useful without requiring private request
      identifiers in committed evidence.
  - repeatable scale benchmark reporting is now available from support-bundle
    metrics:
    - `forward_scale_benchmark` evaluates an execution run, sync latest run, or
      exported support bundle JSON without storing row data
    - `invoke scale-benchmark` writes a reusable benchmark report artifact
    - benchmark checks cover fallback rate/runtime share, diff utilization, row
      failures, partition retry pressure, throughput wait, and apply-engine
      evidence.
  - ledger completion consistency is now enforced in the merge path:
    - merging a final-index shard no longer marks a run completed if earlier
      stage steps remain pending, queued, running, staged, or waiting for merge
    - the run is pointed back at the earliest incomplete stage so auto-merge can
      continue instead of leaving completed-run evidence with non-terminal
      steps
    - final-ingestion baseline readiness now also requires the full run to be
      completion-safe
    - reconciliation can reopen a historical completed run that still has
      incomplete stage steps, clear baseline readiness, and record a
      `completed_run_reopened` event
    - `forward_scale_benchmark --reconcile` gives support an explicit live-run
      repair-and-export path before generating scale evidence.
  - runtime capacity review is now a first-class evidence artifact:
    - `invoke runtime-capacity-review` writes host CPU/memory, worker count,
      recommended PostgreSQL tuning, optional source query-fetch settings, and
      scheduler-overlap capacity-review status
    - `invoke architecture-runtime-evidence --capacity-source-name ...` embeds
      that capacity review beside the scale benchmark evidence so
      `candidate_after_capacity_review` is tied to concrete local capacity
      facts instead of a vague follow-up.
    - `invoke architecture-runtime-evidence --capacity-worker-replicas ...`
      scales `netbox-worker` before runtime probes and preserves that count
      through chaos setup/restore so evidence measures the intended tuned
      profile instead of the compose default.
    - latest refreshed local evidence with `--capacity-worker-replicas 4`
      recorded capacity review status `pass` and left four workers active after
      chaos probes.
    - `--capacity-query-fetch-concurrency` and `--capacity-nqe-page-size`
      reapply source fetch tuning after the local harness seed/reset so
      capacity review records the measured source profile instead of null
      defaults.
    - latest refreshed local evidence recorded
      `query_fetch_concurrency=6`, `nqe_page_size=10000`, and `timeout=1200`
      for the capacity source with `capacity_source_tuning_applied=true`.
  - baseline-to-diff transition visibility is now explicit in support and
    health surfaces:
    - support-bundle metrics include `diff_baseline_transition`
    - Sync Health shows `Baseline to diff` in Query Runtime & Pushdown
    - scale benchmark reports include a dedicated baseline/diff transition
      check
    - transition codes distinguish active API diffs, Fast bootstrap baseline
      mode, missing query identity, missing/ineligible baseline, full-mode
      fallback with a baseline, and failed diff-request fallback.
  - run-local fetch artifact reuse is now available for shard-scoped
    retry/resume economics:
    - artifacts are scoped to execution run, query identity, snapshot,
      baseline snapshot, shard keys, fetch parameters, query parameters,
      column filters, and device-tag scope hash
    - row data is stored only in temporary runtime files, while execution
      metadata records only safe artifact status/count/size fields
    - valid artifacts can satisfy a retried shard fetch without re-running the
      same Forward query
    - artifacts are pruned when ledger runs complete through normal completion
      or reconciliation and when active branch runs are marked failed.
  - delete/dependency planning now surfaces delete-heavy risk before merge:
    - plan previews include `delete_dependency_plan`
    - delete summaries report delete rows, delete shards, delete share,
      execution order, max delete shard size, per-model dependency rank,
      dependent-model count, and reference-blocker risk
    - warning codes call out delete waves, near-budget delete shards, and
      dependency-anchor models that may surface reference blockers.
- `remaining_refactor_plan`:
  `docs/03_Plans/active/2026-05-23-long-term-architecture-remaining-refactors.md`
  now captures the structural follow-on work needed before this architecture
  should be treated as fully stable: model contract registry call-site
  discipline, apply-engine promotion lanes, evidence-gated scheduler overlap,
  delete/dependency planning, baseline/diff calibration, capacity profiles, and
  capability-gated NetBox/future bulk behavior. Run-local fetch artifacts now
  have a current baseline and should be monitored through support evidence.
- `next_focus`: model contract registry, runtime fallback reduction,
  apply-engine parity acceleration, and scheduler throughput smoothing.
  Recovery automation, shard-contract coverage, delete/dependency planning,
  adaptive budget confidence policy, baseline/diff visibility, and
  apply-engine parity gating now have current baseline evidence, so future work
  should keep those gates green rather than treat them as unstarted.
- model contract registry baseline is now audited:
  - `forward_netbox.utilities.model_contracts` composes each supported model's
    sync row contract, shard fetch contract, delete dependency order,
    apply-engine classification, blocker reason, and support-safe diagnostic
    fields
  - `forward_architecture_audit` exposes `model_contract_registry`
  - `forward_architecture_completion_audit` includes
    `model_contract_registry_complete`
  - `forward_architecture_audit` now reads fetch-contract coverage through the
    registry helper instead of calling branch-budget model rules directly
  - Sync Health fetch-contract summaries now read through the registry and
    expose registry status/gap counts alongside fetch mode counters
  - `forward_architecture_audit` now reports safe bulk-ORM models,
    adapter-required models, and adapter blocker codes from registry helpers
    instead of rebuilding those lists directly from apply-engine constants
  - future work is call-site migration toward the registry, not proving that
    the registry exists.
- `current_tranche`: confidence-informed branch budget policy is implemented in
  the planner/hint path, fallback reason summaries now include remediation
  actions, apply-engine expansion now has explicit parity-gate evidence and
  ranked promotion lanes in health/audit output, and scheduler overlap is now
  guarded by measured throughput-readiness evidence. Density learning no longer merely displays
  confidence; confidence now determines whether learned density is used
  directly, blended, or ignored in favor of conservative baseline budgeting.
  Runtime fallback evidence now points operators toward the most likely fix
  layer instead of only reporting fallback counts. Bulk ORM promotion is now
  blocked by explicit parity gates rather than an informal TODO. Scheduler
  overlap remains a future performance lever, but it now has a concrete
  support-bundle gate so it will not be added just because a run is slow.
  Runtime fallback reduction has started with a conservative native
  column-filter retry path that avoids full/model refetch when Forward rejects
  only one equivalent single-value filter form. That retry evidence now feeds
  support-bundle and Sync Health tuning guidance, closing the loop from raw
  retry counters to first-order operator action. Apply-engine acceleration now
  has both a low-risk next parity lane and a high-impact performance target list
  without promoting any additional model before tests prove parity. Capacity and
  backend guidance now separates timeout-risk baseline decisions from
  steady-state Branching/diff tuning. The scale benchmark harness now gives
  release candidates a reusable, customer-safe way to compare fallback, diff,
  retry, throughput, row-failure, and apply-engine evidence from the same
  support-bundle fields used during field troubleshooting. Baseline-to-diff
  behavior is now visible as an explicit transition state instead of requiring
  support to infer it from raw sync mode and baseline fields. The remaining
  long-term work is now explicitly framed as targeted refactors, not another
  broad rewrite: keep model-specific behavior behind the contract registry,
  use measured support evidence to reduce fallback-heavy fetches, expand
  apply-engine acceleration only through parity gates, strengthen
  delete/dependency planning, and add bounded scheduler overlap only when wait
  pressure is proven. Apply-engine parity planning is now concrete instead of
  only directional: the same `bulk_orm_expansion` payload used by Sync Health
  and architecture audit includes candidate models, source of recommendation,
  blocker/lane/risk metadata, checklist gates, and required candidate-specific
  parity test IDs. No additional model is enabled by this planning surface.

Validation evidence for latest tranche:
- `python -m py_compile forward_netbox/utilities/fetch_artifacts.py forward_netbox/utilities/query_fetch_execution.py forward_netbox/utilities/execution_ledger.py forward_netbox/utilities/sync_state.py forward_netbox/utilities/execution_ledger_reconciliation.py forward_netbox/tests/test_sync.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_reuses_run_local_artifact_for_shard_retry forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_artifacts_are_pruned_when_execution_run_completes forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_artifacts_are_pruned_when_execution_run_fails forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_reports_fetch_metadata_for_column_filter_scope forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_marks_full_fallback_when_shard_fetch_fails`
- `poetry run invoke lint`
- `poetry run invoke harness-check`
- `poetry run invoke docs`
- `poetry run invoke architecture-audit-check`
- `poetry run invoke architecture-completion-audit`
- `poetry run invoke check`
- `python -m py_compile forward_netbox/utilities/density_learning.py forward_netbox/utilities/branch_budget.py forward_netbox/utilities/multi_branch_planner.py forward_netbox/utilities/multi_branch_executor.py forward_netbox/utilities/multi_branch_lifecycle.py forward_netbox/utilities/sync_state.py forward_netbox/utilities/execution_telemetry.py forward_netbox/tests/test_sync.py forward_netbox/tests/test_sync_state.py`
- `python -m py_compile forward_netbox/utilities/execution_ledger_metrics.py forward_netbox/tests/test_log_export.py forward_netbox/tests/test_health.py forward_netbox/tests/test_synthetic_scenarios.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest forward_netbox.tests.test_models.ForwardSyncModelTest forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest forward_netbox.tests.test_health.ForwardSyncHealthTest forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_duplicate_stage_job_cannot_reclaim_terminal_step`
- `python -m py_compile forward_netbox/utilities/apply_engine_decision.py forward_netbox/utilities/apply_engine.py forward_netbox/utilities/health_apply_fetch.py forward_netbox/management/commands/forward_architecture_audit.py forward_netbox/tests/test_architecture_audit_command.py forward_netbox/tests/test_health.py forward_netbox/tests/test_sync.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_architecture_audit_command.ForwardArchitectureAuditCommandTest.test_architecture_audit_outputs_apply_engine_matrix forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_view_renders_diagnostics forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_engine_classifies_all_supported_models forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_engine_classifies_all_supported_models_when_bulk_orm_enabled forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_bulk_orm_expansion_summary_requires_parity_for_blocked_models`
- `python -m py_compile forward_netbox/utilities/apply_engine_decision.py forward_netbox/utilities/apply_engine.py forward_netbox/tests/test_sync.py forward_netbox/tests/test_architecture_audit_command.py forward_netbox/tests/test_health.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_bulk_orm_expansion_summary_requires_parity_for_blocked_models forward_netbox.tests.test_architecture_audit_command.ForwardArchitectureAuditCommandTest.test_architecture_audit_outputs_apply_engine_matrix`
- `python -m py_compile forward_netbox/utilities/execution_ledger_metrics.py forward_netbox/tests/test_synthetic_scenarios.py forward_netbox/tests/test_log_export.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_support_bundle_exposes_throughput_smoothing_metrics forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle`
- `python -m py_compile forward_netbox/utilities/query_fetch_execution.py forward_netbox/tests/test_sync.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_partitions_large_column_filter_batches forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_partitions_large_column_filter_diff_batches forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_recovers_failed_full_partition_by_splitting forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_recovers_failed_diff_partition_by_splitting forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_retries_single_default_filter_as_equals_any_before_fallback forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_retries_single_diff_filter_before_full_fallback forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_marks_full_fallback_when_shard_fetch_fails`
- `python -m py_compile forward_netbox/utilities/execution_ledger_metrics.py forward_netbox/utilities/health_summary_blocks.py forward_netbox/tests/test_log_export.py forward_netbox/tests/test_health.py forward_netbox/tests/test_synthetic_scenarios.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_duplicate_stage_job_cannot_reclaim_terminal_step`
- `python -m py_compile forward_netbox/utilities/execution_ledger_metrics.py forward_netbox/utilities/health_summary_blocks.py forward_netbox/tests/test_log_export.py forward_netbox/tests/test_health.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state`
- `python -m py_compile forward_netbox/utilities/health_summary_blocks.py forward_netbox/tests/test_health.py`
- `python -m py_compile forward_netbox/utilities/apply_engine_decision.py forward_netbox/utilities/apply_engine.py forward_netbox/tests/test_sync.py forward_netbox/tests/test_architecture_audit_command.py forward_netbox/tests/test_health.py`
- `git diff --check -- forward_netbox/utilities/apply_engine_decision.py forward_netbox/tests/test_sync.py forward_netbox/tests/test_architecture_audit_command.py forward_netbox/tests/test_health.py forward_netbox/templates/forward_netbox/forwardsync_health.html`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_bulk_orm_expansion_summary_requires_parity_for_blocked_models forward_netbox.tests.test_architecture_audit_command.ForwardArchitectureAuditCommandTest.test_architecture_audit_outputs_apply_engine_matrix forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_view_renders_diagnostics`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state forward_netbox.tests.test_health.ForwardSyncHealthTest.test_large_run_tuning_advises_fast_bootstrap_on_timeout_risk forward_netbox.tests.test_health.ForwardSyncHealthTest.test_large_run_tuning_advises_switch_back_after_fast_bootstrap forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_view_renders_diagnostics`
- `python -m py_compile forward_netbox/utilities/scale_benchmark.py forward_netbox/management/commands/forward_scale_benchmark.py forward_netbox/tests/test_scale_benchmark.py scripts/tests/test_tasks.py tasks.py`
- `python -m unittest scripts.tests.test_tasks.ScaleBenchmarkTaskTest`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_scale_benchmark`
- `python -m py_compile forward_netbox/utilities/execution_ledger_run_store.py forward_netbox/utilities/ingestion_merge.py forward_netbox/tests/test_ingestion_merge.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_ingestion_merge.ForwardIngestionMergeHelperTest.test_sync_merge_ingestion_does_not_complete_out_of_order_final_step forward_netbox.tests.test_ingestion_merge.ForwardIngestionMergeHelperTest.test_sync_merge_ingestion_marks_final_ledger_step_baseline_ready forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest.test_mark_run_completed_does_not_complete_when_stage_steps_unfinished`
- `python -m py_compile forward_netbox/utilities/execution_ledger_reconciliation.py forward_netbox/management/commands/forward_scale_benchmark.py forward_netbox/tests/test_synthetic_scenarios.py forward_netbox/tests/test_scale_benchmark.py tasks.py scripts/tests/test_tasks.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_reconcile_reopens_completed_run_with_incomplete_steps forward_netbox.tests.test_scale_benchmark.ForwardScaleBenchmarkCommandTest.test_command_reconcile_reopens_completed_run_with_incomplete_steps`
- `python -m unittest scripts.tests.test_tasks.ArchitectureRuntimeEvidenceTaskTest scripts.tests.test_tasks.RuntimeOptimizationTaskTest scripts.tests.test_tasks.ScaleBenchmarkTaskTest`
- `poetry run invoke runtime-capacity-review --source-name ui-harness-source`
- `python -m py_compile forward_netbox/utilities/execution_ledger_metrics.py forward_netbox/utilities/health_summary_blocks.py forward_netbox/utilities/scale_benchmark.py forward_netbox/tests/test_log_export.py forward_netbox/tests/test_health.py forward_netbox/tests/test_scale_benchmark.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_view_renders_diagnostics forward_netbox.tests.test_scale_benchmark`
- `poetry run invoke lint`
- `poetry run invoke docs`
- `poetry run invoke harness-check`
- `poetry run invoke harness-test`
- `poetry run invoke architecture-audit-check`
- `poetry run invoke architecture-completion-audit`
- `poetry run invoke check`
- `python -m py_compile forward_netbox/utilities/model_contracts.py forward_netbox/utilities/health_apply_fetch.py forward_netbox/management/commands/forward_architecture_audit.py forward_netbox/tests/test_health.py forward_netbox/tests/test_architecture_audit_command.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state forward_netbox.tests.test_architecture_audit_command`
- `poetry run invoke lint`
- `poetry run invoke harness-check`
- `poetry run invoke docs`
- `poetry run invoke architecture-audit-check`
- `poetry run invoke architecture-completion-audit`
- `poetry run invoke check`
- `python -m py_compile forward_netbox/utilities/model_contracts.py forward_netbox/management/commands/forward_architecture_audit.py forward_netbox/management/commands/forward_architecture_completion_audit.py forward_netbox/tests/test_architecture_audit_command.py forward_netbox/tests/test_architecture_completion_audit_command.py`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_architecture_audit_command forward_netbox.tests.test_architecture_completion_audit_command`
- `poetry run invoke lint`
- `poetry run invoke harness-check`
- `poetry run invoke docs`
- `poetry run invoke architecture-audit-check`
- `poetry run invoke architecture-completion-audit`
- `poetry run invoke check`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_view_renders_diagnostics forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle forward_netbox.tests.test_forms.ForwardSyncFormTest.test_source_form_persists_pushdown_alert_thresholds forward_netbox.tests.test_models.ForwardSyncModelTest.test_source_rejects_invalid_pushdown_alert_threshold`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_view_renders_diagnostics forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_pushdown_trends_downloads_long_window_history`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_view_renders_diagnostics forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_pushdown_trends_downloads_long_window_history forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest forward_netbox.tests.test_health.ForwardSyncHealthTest forward_netbox.tests.test_models.ForwardSyncModelTest`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_jobs forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest forward_netbox.tests.test_ingestion_merge forward_netbox.tests.test_log_export forward_netbox.tests.test_prune_compatibility_cache_command`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardSyncRunnerTest`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_jobs forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_jobs forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest forward_netbox.tests.test_ingestion_merge`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_synthetic_scenarios`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_jobs forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest forward_netbox.tests.test_ingestion_merge forward_netbox.tests.test_api_views.ForwardExecutionRunAPIViewTest forward_netbox.tests.test_log_export`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_recovery_recommendation_escalates_repeated_branch_stale_signals forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_hard_kill_after_branch_creation_reconciles_to_discardable_step forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_synthetic_scenarios forward_netbox.tests.test_jobs forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest forward_netbox.tests.test_ingestion_merge forward_netbox.tests.test_api_views.ForwardExecutionRunAPIViewTest forward_netbox.tests.test_log_export`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_ingestion_merge.ForwardIngestionMergeHelperTest.test_maybe_enqueue_next_branch_stage_auto_requeues_merge_timeout_within_budget forward_netbox.tests.test_ingestion_merge.ForwardIngestionMergeHelperTest.test_maybe_enqueue_next_branch_stage_skips_merge_timeout_auto_requeue_over_budget forward_netbox.tests.test_jobs.ForwardJobsTest.test_merge_forwardingestion_timeout_auto_requeues_merge_within_budget`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_ingestion_merge forward_netbox.tests.test_jobs forward_netbox.tests.test_synthetic_scenarios forward_netbox.tests.test_log_export`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_reconcile_stale_run_heartbeat_records_watchdog_event forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_recovery_recommendation_escalates_repeated_run_watchdog_signals forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_recovery_recommendation_flags_stale_run_heartbeat_for_reconcile forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_ingestion_merge forward_netbox.tests.test_jobs forward_netbox.tests.test_synthetic_scenarios forward_netbox.tests.test_log_export forward_netbox.tests.test_api_views.ForwardExecutionRunAPIViewTest forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_pushdown_efficiency_reports_model_fallback_guardrail forward_netbox.tests.test_log_export.ForwardIngestionLogExportViewTest.test_execution_run_support_bundle_downloads_json_bundle forward_netbox.tests.test_health.ForwardSyncHealthTest.test_sync_health_summary_reports_local_state`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_health forward_netbox.tests.test_log_export forward_netbox.tests.test_synthetic_scenarios forward_netbox.tests.test_ingestion_merge forward_netbox.tests.test_jobs`
- `poetry run invoke harness-check`
- `poetry run invoke check`

### Workstream Status

1. True Shard-Scoped NQE Execution: `completed_current_baseline`
   - contracts and column-filter pushdown are broadly in place.
   - partition retry splitting now reduces fallback escalation for failed
     shard-scope EQUALS_ANY partitions.
   - per-model fallback budget guardrails now surface explicit warning signals
     and tuning actions in support/health metrics.
   - architecture audit reports zero fetch-contract coverage gaps for supported
     models.
   - future work is runtime fallback reduction for failed pushdown attempts, not
     missing contract coverage.
2. Apply Engine Expansion With Parity Gates: `in_progress`
   - model eligibility classification covers every supported model.
   - current safe `bulk_orm` set is proven for the scalar/simple hierarchy
     models (`dcim.site`, `dcim.manufacturer`, `dcim.devicerole`,
     `dcim.platform`, `dcim.devicetype`, `ipam.vlan`, `ipam.vrf`).
   - health/audit output now exposes `bulk_orm_expansion` with safe models,
     blocked models, parity gates, and next action.
   - expansion beyond current set remains gated on parity + runtime evidence.
3. Compatibility Cache Retirement: `completed`
   - ledger-first runtime behavior is active and runtime compatibility write
     fallbacks are retired.
   - legacy compatibility payloads are now upgrade-to-ledger/read-through only.
   - stale payload cleanup/reporting is available through native prune command
     and task wrappers.
4. Adaptive Branch Budgeting: `completed_current_baseline`
   - density-driven, delete-aware, and runtime-aware budgeting is active.
   - adaptive learning hardening now includes confidence metadata and guarded
     outlier rejection.
   - confidence-informed policy now controls learned-density use in planning
     and branch budget hints.
   - future work is calibration from repeated field evidence, not missing
     planner integration.
5. Recovery Automation: `completed_current_baseline`
   - reconcile and stale-step flows are present.
   - enqueue-time reconciliation is now validated for stage queue decisions.
   - stale queued/running no-branch/no-live-job paths now auto-reset/requeue
     with explicit reason-coded reconciliation events.
   - support bundle now exposes auto-recovery policy evidence for operator
     triage.
   - repeated branch-associated stale events now have deterministic escalation
     guidance (`manual_intervention`) with threshold evidence in support output.
   - bounded automatic merge-timeout requeue is now active for auto-merge runs
     with strict retry budget.
   - run-level no-progress watchdog automation is now active with deterministic
     escalation threshold.
   - fresh completion audit evidence reports destructive worker-kill scenarios
     passed for the current recovery baseline.
   - future work is gate maintenance and adding scenario coverage when new
     recovery transitions are introduced.
6. Operator Observability Hardening: `completed`
   - support bundle + health surfaces now include explicit pushdown-efficiency
     advisories and hotspot models.
   - source-configurable threshold guidance is now surfaced for operators.
   - hotspot-aware actionable tuning guidance and long-window trend retention
     export are now implemented.
7. Single-Branch Multi-Version Strategy: `completed_current_baseline`
   - architecture direction is one code path with capability/version gates.
   - local audit and docs gates are present.
   - GitHub CI now runs the validation job across `v4.5.9` and `v4.6.0`.
   - future supported versions should be added to the same matrix instead of
     creating release branches with divergent product behavior.
8. Model Contract Registry: `completed_current_baseline_with_call_site_migration_remaining`
   - one explicit architecture contract record now exists per supported model.
   - each contract composes NQE row contract, coalesce identity, shard fetch
     contract, delete dependency order, apply-engine eligibility/blocker, and
     support-safe diagnostic fields.
   - architecture audit and completion audit now fail when registry coverage is
     missing.
   - future work is migrating model-specific read paths toward the registry
     where that reduces duplication without changing behavior.
9. Run-Local Fetch Artifact Boundary: `completed_current_baseline`
   - shard-scoped retry/resume can now reuse bounded temporary runtime
     artifacts instead of repeating the same Forward query.
   - support metadata reports artifact status/count/size only; row data stays
     out of durable support bundles and ledger state.
   - future work is field calibration of artifact size/TTL defaults and
     cleanup behavior as additional terminal run paths are added.
10. Capacity Profiles And Capability Gates: `completed_current_capacity_profiles_with_future_capability_gates_planned`
   - capacity profiles now exist in user-facing configuration guidance.
   - NetBox 4.6+ and future bulk features should stay capability-gated on the
     same branch and plug into the existing apply-engine boundary.

## Workstreams

### 1) True Shard-Scoped NQE Execution

Move remaining model paths from model/full fallback to deterministic shard pushdown
where contract-safe.

- Persist and enforce shard predicates/bucket identities per step.
- Expand query pushdown coverage for models still using fallback fetch modes.
- Keep full/model fallback for unsupported models with explicit reason codes.

### 2) Apply Engine Expansion With Parity Gates

Increase `bulk_orm` coverage only where parity is proven.

- Add model-by-model eligibility with contract tests.
- Require parity evidence against adapter behavior before enablement.
- Preserve automatic adapter fallback for any mismatch/unsupported conditions.

### 3) Compatibility Cache Retirement

Finish migration to ledger-only active orchestration.

- Remove remaining compatibility `_branch_run` write paths after migration window.
- Keep read-through compatibility only where explicitly required for upgrades.
- Maintain prune/audit tooling until compatibility retirement is complete.

### 4) Adaptive Branch Budgeting

Tune shard sizing using observed execution behavior.

- Use live run metrics (change density, row/runtime mix) to recommend/auto-tune
  model budgets.
- Keep guardrails for branch-change guidance and delete-heavy paths.
- Surface budget rationale in health and support-bundle output.

### 5) Recovery Automation

Reduce manual intervention for long-running runs.

- Add safe automatic reconciliation for stale claims/steps where evidence is clear.
- Strengthen idempotent retry/requeue flows for stage and merge.
- Expand chaos/runtime evidence coverage for recovery scenarios.

Current implemented baseline:
- stage enqueue now reconciles active execution-ledger state before deciding the
  next queued shard.
- reconciliation treats a running core job without a live RQ queue/started/
  scheduled/deferred entry as stale when RQ state is inspectable, while
  preserving the historical row-based fallback when RQ cannot be inspected.
- targeted tests validate:
  - no duplicate queueing when another stage shard is already running
  - reuse of existing queued shard jobs instead of duplicate enqueue
  - ledger-first reconcile before stage enqueue
  - legacy compatibility state upgrade continuity
  - live-job stale-heartbeat protection when RQ confirms the job is active
  - stale core-job auto-requeue when RQ confirms the job is no longer active
- persisted shard resume now tolerates deterministic re-planning that splits
  the claimed ledger shard into smaller candidate items. The executor
  recombines subset candidates under the original persisted shard boundary
  instead of failing with an unresolved claimed index.

### 6) Operator Observability Hardening

Improve one-click diagnosis for field issues.

- Keep support bundles as canonical export for run + step + fallback evidence.
- Add explicit per-model counters for full vs diff vs fallback fetch decisions.
- Continue improving health summaries to explain execution decisions and blockers.

### 7) Single-Branch Multi-Version Strategy

Maintain one architecture surface across supported NetBox versions.

- Use capability/version gates instead of diverging workflow behavior.
- Keep feature parity policy documented for 4.5/4.6+.
- Validate version-gated behavior via targeted CI matrix checks.

## Proposed Architecture Items (Speed / Scale Priority)

### P0: Model Contract Registry

Status: `completed_current_baseline_with_call_site_migration_remaining`

- Consolidate per-model execution rules into one contract surface:
  - NQE map identity and row shape expectations
  - coalesce identity
  - dependency order
  - safe shard filter fields
  - diff eligibility
  - local safety filter
  - delete behavior
  - apply-engine eligibility
  - support-safe diagnostic fields
- Wire the architecture audit to fail when a supported model lacks a complete
  contract.

Implemented now:
- added `forward_netbox.utilities.model_contracts`.
- `forward_architecture_audit` now emits registry status and gap detail.
- `forward_architecture_completion_audit` now includes a registry completion
  check.
- first behavior-preserving call-site migration is in place:
  - architecture audit fetch-contract coverage uses
    `architecture_fetch_contracts()`
  - Sync Health fetch-contract summaries use
    `architecture_fetch_contract_for_model()` and report registry status/gap
    counts.
- second behavior-preserving call-site migration is in place:
  - architecture audit now derives `bulk_orm_safe_models`,
    `adapter_required_models`, and `adapter_blockers` from registry helpers.
  - original apply-engine gap checks remain intact, so the audit still catches
    unclassified supported models, adapter models without blockers, and
    bulk-safe models without specs.
- third behavior-preserving call-site migration is in place:
  - query registry built-in defaults now read fallback coalesce fields through
    `architecture_default_coalesce_fields_for_model()`.
  - sync execution fallback coalesce fields now read through the same registry
    helper.
  - query fetch preflight/planning fallback coalesce fields now read through
    the same registry helper.
  - row-shape validation still stays in `sync_contracts`, preserving the
    existing validation boundary while moving row identity fallback reads to
    the architecture registry.
- fourth behavior-preserving call-site migration is in place:
  - architecture audit classification gap reads now use registry helpers for
    unclassified supported models, adapter-required models without blocker
    codes, and bulk-ORM-safe models without implemented specs.
  - model contract gap detection now checks adapter blocker coverage from the
    contract classification instead of a separate adapter-required constant.
  - apply-engine runtime selection remains unchanged; this pass only
    centralizes audit/status reads behind the model contract registry.

Remaining:
- migrate future model-specific rule reads to the registry instead of adding
  more separate per-module constants.
- keep behavior-preserving call-site migration test-covered and incremental.

Why this remains first for future model work:
- It prevents future speed work from adding model-specific rules separately in
  query fetch, branch planning, apply adapters, delete handling, and Sync
  Health.

### P0: Run-Local Fetch Artifact Boundary

Status: `completed_current_baseline`

- Add a runtime-only artifact boundary that can reuse scoped fetch results for
  retry/resume when that avoids repeating expensive Forward query work.
- Keep NQE plus the selected Forward snapshot as the source of truth.
- Store only bounded runtime artifacts needed for retry economics.
- Keep row data out of durable support bundles and do not introduce
  Python-side normalization.

Implemented now:
- added `forward_netbox.utilities.fetch_artifacts` for bounded runtime-only
  artifact save/load/prune behavior.
- shard-scoped `_fetch_spec_rows()` computes a deterministic artifact key from
  run, model, query identity, snapshot/baseline snapshot, shard keys, fetch
  parameters, query parameters, column filters, and hashed device-tag scope.
- successful shard fetches store rows in the temporary artifact directory when
  JSON-serializable and under the configured byte limit.
- retried shard fetches reuse a valid artifact and report
  `fetch_parameters.fetch_artifact.status = hit` without calling Forward again.
- support-safe metadata is limited to artifact key, run ID, status, row/delete
  counts, byte size, expiration, max bytes, and reason.
- artifact directories are pruned on normal ledger completion, reconcile-time
  completion, and branch-run failure.

Completion signal:
- retrying a failed shard does not rerun the same full expensive query when a
  valid scoped artifact exists.
- support bundles state whether query work was reused, retried, broadened, or
  discarded.

### P0: Pushdown Efficiency Control Loop

Status: `completed_current_baseline`

- Add run-level pushdown effectiveness thresholds:
  - fallback-step rate by model
  - diff-eligible vs diff-actual ratio
  - full-fallback runtime share
- Emit advisory flags when a model repeatedly misses pushdown expectations.
- Store trend snapshots in run/support-bundle artifacts for before/after
  regression analysis.

Implemented now:
- fallback-step rate + pushdown-rate advisory status/message
- hotspot model identification
- full-fallback runtime share and fallback runtime share summaries
- diff actual ratio summary
- recent-run trend snapshots in health/support metrics
- configurable source-managed alert thresholds for fallback rate, runtime share,
  and diff-utilization warning decisions
- explicit diff-baseline reason correlation in trend rows for diff-eligible
  non-diff steps
- sync-level long-window trend export endpoint for historical retention

### P0: Query Runtime Budgeting For Stage Planning

Status: `completed`

- Add optional query-runtime-aware plan shaping:
  - cap hot-model shard fanout when query runtime dominates
  - prefer wider shards for low-density/low-query-cost models
  - prefer narrower shards where merge/apply dominates and overflow risk is high
- Keep NetBox branch budget as the non-negotiable upper bound.

Implemented now:
- runtime-per-row-based budget shaping with bounded factors (`0.75x` to `1.25x`)
  applied after density/delete weighting.
- minimum-row threshold guard to avoid overreacting to small samples.
- delete-heavy guardrail that prevents runtime-driven budget widening for delete
  shards.
- planner + overflow re-split parity by using the same
  `effective_workload_row_budget()` path.

### P1: Adaptive Density Learning Hardening

Status: `completed`

- Add confidence tracking for learned model densities:
  - sample count
  - recency
  - variance
- Use guarded updates to avoid overreacting to one anomalous run.
- Expose learned vs default density in health/support bundles.

Implemented now:
- introduced explicit density profile persistence alongside learned densities.
- added guarded density observation updates with outlier rejection for warmed-up
  models (`ratio_outlier` / `zscore_outlier` paths).
- added confidence scoring/bucketing using sample count, variance stability, and
  recency weighting.
- exposed density-learning summaries in:
  - Sync display/workload/execution summaries
  - Sync Health (`Density Learning` card)
  - support-bundle execution metrics.
- added confidence-informed budget policy:
  - high confidence uses learned density
  - medium confidence blends learned density with baseline
  - low confidence uses baseline density
  - branch budget hints and summaries expose policy/rationale.

### P1: Partition Retry Split Hardening

Status: `completed`

- Retry failed shard partitions by recursively splitting EQUALS_ANY filters
  before escalating to full/model fallback.
- Preserve deterministic result order and existing fallback behavior when
  split retries are exhausted.

Implemented now:
- `_fetch_partitioned_rows()` now wraps partition fetches with split-retry
  behavior for partition-scoped query failures.
- full and diff shard-fetch paths both benefit because they share the same
  partition execution helper.
- regression coverage confirms split retries keep shard-scoped fetch mode on
  recoverable partition failures.

### P1: Apply-Engine Expansion Lane

Status: `in_progress`

- Add parity harness templates for candidate models (one model per tranche).
- Gate enablement on:
  - deterministic create/update/delete parity
  - no increase in row-level failures
  - equal or better runtime on synthetic + live smoke baselines

Implemented now:
- every supported model is classified for apply-engine eligibility.
- `bulk_orm` safe set is explicit and validated by the architecture completion
  audit.
- CI includes `architecture_audit_check` so unclassified model additions fail
  early.
- Sync Health and architecture audit now expose `bulk_orm_expansion` evidence:
  - safe models
  - blocked models and blocker reasons
  - required parity gates
  - next action for any future candidate family
- `bulk_orm_expansion.parity_plan` now gives the next implementation tranche a
  concrete, support-safe workplan:
  - candidate models from the lowest-risk lane and highest-impact list
  - candidate source labels (`lowest_risk_lane`, `highest_impact_model`)
  - promotion lane, priority, risk, blocker code, and lane-specific gate
  - generic parity checklist for create/update/delete, validation failure, row
    issues, dependency behavior, object-change tracking, Branching semantics,
    support-bundle statistics, and runtime non-regression
  - candidate-specific test IDs such as
    `ForwardApplyEngineParityTest.test_dcim_virtualchassis_create_parity`.
- first-candidate parity guard coverage now exists for `dcim.virtualchassis`:
  - create/update/delete adapter behavior
  - validation/duplicate-position failure behavior
  - missing-dependency row issue behavior
  - dependency-failed skip behavior
  - object-change/Branching blocker guard
  - support statistics behavior
  - runtime non-regression guard that keeps the model on adapter until a real
    faster engine proves parity.

Remaining:
- evaluate the next candidate family only after it can prove NetBox validation,
  object-change tracking, Branching behavior, row-level issue parity,
  dependency behavior, support-bundle statistics parity, and runtime
  non-regression.
- keep complex relationship-heavy models on the adapter path until a faster
  engine proves equivalent semantics.

### P1: Delete And Dependency Planning

Status: `completed_current_baseline`

- Plan delete-heavy work using expected change density and dependency risk.
- Surface preflight delete estimates and likely reference blockers.
- Keep skip/issue aggregation consistent with create/update paths.
- Keep destructive changes visible before merge.

Implemented now:
- branch planning already separates mixed workloads into apply then delete
  phases and executes deletes in dependency order.
- delete-heavy device workloads use conservative row budgets.
- plan previews now include `delete_dependency_plan`:
  - total delete rows
  - delete shard count
  - delete model count
  - delete share of planned changes
  - max delete shard size
  - dependency-ordered delete model execution order
  - per-model dependency rank, dependent-model count, first/last plan index,
    max delete shard size, and reference-blocker risk
  - warnings for delete waves, near-budget delete shards, and dependency-anchor
    reference-blocker risk.

Remaining:
- calibrate warning thresholds from repeated field runs.
- use row issue and merge evidence to tune which dependency-anchor warnings
  should be warning vs informational.

### P1: Compatibility Cache Retirement Completion

Status: `completed`

- Remove active compatibility `_branch_run` mutations from runtime orchestration
  surfaces.
- Keep compatibility payload support as read-through/upgrade bridge only.
- Keep native stale-payload prune + report tooling for operations.

Implemented now:
- runtime phase/progress and plan-item updates no longer write compatibility
  state when no execution run exists.
- legacy continuation queueing now upgrades `_branch_run` state into execution
  ledger before staging.
- compatibility prune command/task coverage and targeted regression tests are
  in place.

### P2: Version-Matrix Consolidation

Status: `completed_current_baseline`

- Add CI lanes for supported NetBox minors using one architecture path with
  capability flags.
- Keep feature behavior consistent unless explicitly version-gated with tests.

Implemented now:
- architecture docs and runtime guidance define one capability-gated path.
- completion audit verifies repo-level architecture gates.
- `.github/workflows/ci.yml` runs `validate` against:
  - `NETBOX_VER=v4.5.9`
  - `NETBOX_VER=v4.6.0`

Remaining:
- GitHub Actions must prove the expanded matrix after push/PR.
- add future supported NetBox minors to this matrix only after local capability
  checks pass.

### P2: Capacity Profiles And Future Capability Gates

Status: `completed_current_capacity_profiles_with_future_capability_gates_planned`

- Convert Sync Health and support-bundle tuning signals into user-facing
  deployment profiles for small, medium, large, and very-large imports.
- Document worker count, RQ timeout, database/storage expectations, query page
  size, query fetch concurrency, branch budget guidance, and when to choose
  Fast bootstrap versus Branching.
- Keep NetBox 4.6+ and future TurboBulk/parquet/native bulk behavior behind
  runtime capability probes and the existing apply-engine boundary.

Implemented now:
- `docs/01_User_Guide/configuration.md` includes small, medium, large, and
  very-large runtime sizing profiles.
- profiles tie backend choice, query fetch concurrency, page size, worker
  timeout, and first health signals to the existing Sync Health/support-bundle
  workflow.
- the docs preserve the architecture boundary: NQE remains source of truth, row
  validation stays shared, and backend choice only changes review/write
  mechanics.

Completion signal:
- an operator can use Sync Health to decide whether to fix diffs, reduce
  fallback, add capacity, tune query concurrency, use Fast bootstrap, or stay
  on Branching without support needing screenshots first.
- future NetBox 4.6+/TurboBulk/parquet/native bulk behavior remains planned
  behind runtime capability probes and apply-engine gates.

### P0: Shard-Scoped Coverage Completion

Status: `completed_current_baseline`

- Reduce remaining model-level fallback fetch contracts by converting eligible
  models to deterministic shard pushdown predicates.
- Add per-model fallback budget guardrails to fail fast into explicit warnings
  when a model repeatedly exceeds acceptable fallback-share thresholds.
- Tie fallback-share alerts to specific recommended operator actions in health
  and support exports.

Implemented now:
- architecture audit reports no `fetch_contract_coverage_gaps` for supported
  models.
- every supported model has an explicit fetch contract with reason code,
  schema contract, local safety filter, and bucket strategy.
- per-model fallback budget guardrails are emitted in pushdown efficiency
  (`model_fallback_guardrails`, threshold/min-step metadata, exceeded counts).
- warnings and dedicated tuning action code are present when guardrails trip.

Remaining:
- reduce runtime fallback events caused by Forward/API/query failures after
  pushdown is attempted.
- use live trend exports to verify fallback guardrail counts stay low across
  repeated large runs.

### P0: Runtime Fallback Reduction Evidence

Status: `in_progress`

- Count fallback reasons by model so runtime pushdown failures can be fixed by
  evidence rather than by broad query rewrites.
- Keep reason aggregation in support/health metrics; do not persist row data or
  invent a second query state store.

Implemented now:
- execution-run support bundle metrics include `fallback_reason_summary`:
  - total fallback steps
  - top fallback reasons
  - per-model fallback reason counts
- fallback summaries now include remediation actions:
  - `model_fetch_contract_fallback` -> planner/query contract work
  - `shard_pushdown_failed_full_fallback` -> Forward query execution/runtime
    investigation
  - `diff_pushdown_failed_full_fallback` -> diff baseline/query execution
    investigation
  - timeout/parameter/unknown reasons receive explicit action codes instead of
    generic fallback counts
- Sync Health query-pushdown summary includes the same fallback reason summary.
- default reasons are assigned for model/full/diff fallback modes when the
  runtime did not record a concrete exception reason.
  - single-value shard column filters now retry the equivalent native operator
    before full/model fallback:
  - `DEFAULT` -> `EQUALS_ANY`
  - single-value `EQUALS_ANY` -> `DEFAULT`
  - the retry is shared by full and diff partition fetch paths and preserves
    local shard safety filtering
  - count-only `partition_retry_summary` metadata records split retries,
    alternate-operator retries, and successful retry counts without storing row
    data
  - execution support bundles and Sync Health aggregate
    `partition_retry_summary` across stage steps:
    - total retry step count
    - split retry attempts/successes
    - alternate-operator retry attempts/successes
    - avoided broader fallback count
    - per-model and per-operation counts.
  - Forward exception text used in retry warnings, fallback reasons, and
    query-validation diagnostics is sanitized before persistence. This is a
    supportability hardening item, not a row-shaping change: NQE remains the
    source of truth and fallback/retry behavior is unchanged.
  - non-retryable shard-filter failures now fail fast into the existing
    full/model fallback path instead of recursively splitting the same bad
    Forward API request into many smaller failed requests. This preserves the
    safe fallback behavior while avoiding retry storms for HTTP 400 partition
    failures.

Remaining:
- use live support bundles/trend exports to identify repeated fallback reasons.
- fix repeated fallback reasons at the query/Forward API layer where safe; the
  first reduction paths are now in place for single-value column-filter
  operator mismatches and non-retryable HTTP 400 partition failures. Remaining
  causes should be driven by support-bundle reason counts.
- mark this complete only after repeated large runs show low fallback reason
  counts or explainable residual fallback causes.

### P1: Execution Throughput Smoothing

Status: `in_progress`

- Add bounded overlap for fetch planning and branch apply preparation so stage
  workers spend less idle time between shards.
- Add adaptive stage worker concurrency ceilings from observed DB contention
  signals (without violating branch-change guardrails).
- Keep the existing ledger as the source of truth for all in-flight state.

Implemented now:
- support-bundle execution metrics include `throughput_smoothing` with:
  - stage queue seconds
  - stage duration seconds
  - merge queue seconds
  - merge wait seconds
  - merge duration seconds
  - per-model wait-share hotspots
  - scheduler-overlap readiness status, dominant wait component, and required
    preconditions.
- runtime bottleneck scoring can identify `queue_or_merge_wait`.
- tuning guidance emits `throughput_wait_pressure` when wait share is material.
- execution support bundles include `operator_tuning_summary`, which ranks
  first-order actions from diff utilization, fallback fetch, queue/merge wait,
  primary bottleneck, and query fetch concurrency.
- Sync Health includes `large_run_tuning` with the same operator-facing
  decision shape for large-run troubleshooting.
- top-level Sync Health checks include `Large-run tuning`, preserving the
  native NetBox health-check workflow rather than requiring operators to inspect
  raw JSON first.
- scheduler overlap now has an explicit readiness gate in support bundles:
  - `insufficient_evidence` when timing evidence is missing
  - `not_indicated` when wait share is below the overlap threshold
  - `needs_more_runtime_evidence` when one run shows pressure but evidence is
    not yet deep enough
  - `candidate_after_capacity_review` only when wait pressure is material and
    runtime evidence is sufficient to justify capacity review.
- long-running Branching merge jobs now have a merge-loop progress hook:
  - execution-ledger heartbeat is refreshed periodically while native Branching
    changes are applied.
  - sparse NetBox job-log progress is emitted at large row/time intervals.
  - the hook is deliberately inside the existing per-change merge loop; it does
    not add a side queue, alter Branching merge semantics, or widen shard
    budgets.

Proposed implementation shape:
- keep current worker-per-stage semantics as the correctness baseline.
- add a ledger-derived scheduler window that can prefetch/prepare the next
  eligible shard only when dependency order, branch budget, and DB headroom are
  clear.
- expose observed queue latency, fetch latency, apply latency, and merge wait
  time per model so concurrency changes are evidence-led.
- make `query_fetch_concurrency`, worker replicas, and Postgres tuning the
  supported first tuning knobs before adding deeper execution overlap.

Do not implement:
- unbounded concurrent branch mutations for the same model family.
- hidden branch-budget widening to mask slow planning/apply behavior.
- non-ledger side queues that cannot be reconstructed from support bundles.

Remaining:
- implement scheduler overlap only after the readiness gate reports repeated
  queue or merge-wait pressure and worker/database headroom is confirmed.
- keep any overlap bounded by ledger step status, dependency order, branch
  budget, and DB headroom.

### P1: Recovery Policy Automation Depth

Status: `completed_current_baseline`

Implemented now:
- reconcile-time automatic retry policy for stale no-branch stage states:
  - `stale_queued_without_branch_auto_reset`
  - `stale_stage_without_branch_auto_requeue`
- guardrail: policy never auto-requeues branch-associated stale steps; those
  remain failure/discard-driven.
- support-bundle recovery markers:
  - `recovery_policy_summary.auto_policy_event_count`
  - `recovery_policy_summary.auto_policy_reasons`
  - `recovery_policy_summary.last_auto_policy_event`
- deterministic no-progress escalation policy for repeated branch-associated
  stale signals:
  - `recovery_recommendation.action = manual_intervention`
  - `recovery_recommendation.escalation_reason`
  - `recovery_recommendation.escalation_count`
  - `recovery_policy_summary.escalation_required` and threshold counters

Remaining:
- keep the destructive chaos gate in release validation for future recovery
  changes.
- add new chaos scenarios only when a new recovery state transition or merge
  behavior is introduced.

Fresh audit evidence:
- `forward_architecture_completion_audit` now separates completed baselines
  from remaining roadmap gates instead of reporting old coverage as total
  architecture completion.
- completed baseline checks still cover model classification, model contracts,
  current safe `bulk_orm` set, compatibility-cache pruning, support-bundle
  compatibility evidence, and destructive recovery/chaos evidence.
- open gates are now explicit:
  - `bulk_orm_candidate_parity_tests_complete` now passes for the first
    parity-plan candidate (`dcim.virtualchassis`) while keeping the model on
    adapter.
  - `field_scale_runtime_matrix_verified` now passes with an approved live
    smoke matrix artifact.
  - `runtime_fallback_reduction_evidence_verified` requires repeated large-run
    support bundles showing low fallback reason counts or explainable residual
    fallback causes.
  - `scheduler_overlap_readiness_evidence_verified` requires repeated large-run
    support bundles showing whether scheduler overlap is not indicated, still
    needs evidence, or is a candidate after capacity review.
- runtime evidence path remains:
  `docs/03_Plans/evidence/architecture-runtime-evidence.json`.
- latest completion audit result: `14` completed, `0` failed,
  `2` needing external/runtime evidence.
- latest refreshed runtime evidence timestamp:
  `2026-05-24T12:01:19Z`.
- latest scale-benchmark evidence path:
  `docs/03_Plans/evidence/scale-runtime-evidence.json`.
- latest capacity-review evidence path:
  `docs/03_Plans/evidence/runtime-capacity-review.json`.
- the latest local scale benchmark against execution run `119` is useful
  diagnostic evidence but not completion evidence: it inspected `166` steps,
  reached shard `81`, and still reports the run as `running`. Fallback and
  scheduler evidence gates correctly remain open until a complete large-run
  artifact is available.
- run `119` currently shows no row failures across `532592` attempted rows,
  fallback below warning thresholds, fallback runtime share around `0.5%`, no
  partition retry pressure, and scheduler overlap status
  `candidate_after_capacity_review` with high queue wait share.
- shard `74/166` completed native Branching merge after staging cleanly with
  `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries.
- shard `75/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- shard `76/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- shard `77/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- shard `78/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- shard `79/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9794` attempted rows, `9794` applied rows, `9794` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- shard `80/166` staged cleanly with `nqe_column_filter`, one column filter,
  `9794` attempted rows, `9794` applied rows, `9794` actual changes, `0`
  failed rows, and `0` retries, then completed native Branching merge.
- shard `81/166` is now running stage with `nqe_column_filter`, one column
  filter, and estimated `9794` changes. The latest recovery snapshot shows no
  failures or last error.
- run `119` also exposed stale core-job liveness after dev-worker auto-restart.
  The RQ-aware liveness check reset shard `46` from stale `running` to
  `pending`, then `invoke execution-run-recovery --enqueue-next` requeued it as
  a native NetBox job. Shard `46/166` then staged and merged successfully with
  `9295` attempted rows, `9295` applied rows, `9295` actual changes, and `0`
  row failures.
- the same shard also exposed two scale-path hardening items:
  - non-retryable Forward HTTP 400 partition failures should fall back once
    instead of recursively splitting into a large retry storm.
  - persisted shard resume must tolerate a deterministic re-plan that splits
    the claimed shard into smaller candidates.
  Both fixes are implemented, covered by targeted regressions, and carried
  through the next full stage/merge cycle without repeating the retry storm.
- run `119` later exposed a second stale-job edge case: an old claimed shard
  job for a previously recovered index can finish after the run has already
  advanced and incorrectly mark the current shard failed. Stage-job exception
  handling is now shard-owned:
  - failures are recorded against the claimed shard, not whatever shard is
    current when the exception handler runs.
  - if the claimed shard is already staged, merge-queued, merged, skipped,
    cancelled, owned by a different job, or behind the run pointer, the job is
    treated as stale and the active execution run is preserved.
  - the regression
    `ForwardJobsTest.test_stage_forward_branch_item_stale_claim_failure_does_not_fail_current_step`
    covers this exact late-job pattern.
- after that fix, `invoke execution-run-recovery --run-id=119 --enqueue-next`
  resumed the run through the native NetBox job path. Shard `54/166` requeued
  as job `539`, returned to `running`, then merged cleanly. The next refreshed
  support-safe evidence showed `54` merged shards, shard `55/166` running,
  `111` pending shards, and `0` failed rows at the refresh point.
- the next stable refresh showed shard `55/166` merged cleanly with `9502`
  attempted rows, `9502` applied rows, `1281` actual changes, and `0` row
  failures. Shard `56/166` then staged cleanly and entered `merge_queued` with
  `9502` attempted rows, `9502` applied rows, `0` row failures, and `0` failed
  execution steps across the run.
- shard `56/166` then merged cleanly with `9502` attempted rows, `9502`
  applied rows, `1406` actual changes, and `0` row failures. The current
  non-terminal evidence has shard `57/166` running and no failed execution
  steps.
- shard `57/166` then staged cleanly and entered merge with `9501` attempted
  rows, `9501` applied rows, `1365` actual changes, and `0` row failures.
  After it merged, the run advanced into the next `ipam.prefix` section at
  shard `58/166`. The latest non-terminal evidence shows shard `58/166`
  running, `0` failed execution steps, and no prefix retry/fallback warnings in
  the active job snapshot.
- latest live runtime evidence now shows the same prefix recovery path has
  carried through shard `67/166` and advanced to shard `68/166`. The current
  active step is `ipam.prefix`, uses `nqe_column_filter`, has an estimated
  `9795` changes, has reached `9795` attempted rows, `9795` applied rows,
  `9795` actual changes, `0` row failures, and `0` step retries. It is now
  `merge_queued`, so the recovery command correctly recommends `wait` because
  the native NetBox merge job is live.
- shard `68/166` then completed its native Branching merge cleanly and the run
  advanced to shard `69/166`. The current active step remains `ipam.prefix`,
  uses `nqe_column_filter`, has reached `9795` attempted rows, `9795` applied
  rows, `9795` actual changes, `0` row failures, and `0` step retries. It is
  now `merge_queued`, so the recovery command correctly recommends `wait`
  because the native NetBox merge job is live.
- shard `69/166` then completed its native Branching merge cleanly and the run
  advanced to shard `70/166`. The current active step remains `ipam.prefix`,
  uses `nqe_column_filter`, has reached `9795` attempted rows, `9795` applied
  rows, `9795` actual changes, `0` row failures, and `0` step retries. It is
  now `merge_queued`, so the recovery command correctly recommends `wait`
  because the native NetBox merge job is live.
- shard `70/166` then completed its native Branching merge cleanly and the run
  advanced to shard `71/166`. The current active step remains `ipam.prefix`,
  uses `nqe_column_filter`, has reached `9795` attempted rows, `9795` applied
  rows, `9795` actual changes, `0` row failures, and `0` step retries. It is
  now `merge_queued`, so the recovery command correctly recommends `wait`
  because the native NetBox merge job is live.
- shard `71/166` then completed its native Branching merge cleanly and the run
  advanced to shard `72/166`. The current active step remains `ipam.prefix`,
  uses `nqe_column_filter`, has reached `9795` attempted rows, `9795` applied
  rows, `9795` actual changes, `0` row failures, and `0` step retries. It is
  now `merge_queued`, so the recovery command correctly recommends `wait`
  because the native NetBox merge job is live.
- shard `72/166` then completed its native Branching merge cleanly and the run
  advanced to shard `73/166`. The current active step remains `ipam.prefix`,
  uses `nqe_column_filter`, has reached `9795` attempted rows, `9795` applied
  rows, `9795` actual changes, `0` row failures, and `0` step retries. It is
  now `merge_queued`, so the recovery command correctly recommends `wait`
  because the native NetBox merge job is live.
- shard `73/166` then completed its native Branching merge cleanly and the run
  advanced to shard `74/166`. Shards `74/166` through `78/166` then staged and
  merged cleanly through the same `ipam.prefix` path with `nqe_column_filter`,
  one column filter, `0` row failures, and `0` step retries. The current active
  step remains `ipam.prefix` at shard `81/166`, has estimated `9794` changes,
  has no row failures or last error, and the recovery command correctly
  recommends `wait` because the native NetBox stage job is live.
- shard `58/166` then staged cleanly through the prefix path with `9795`
  attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is now `merge_queued`, so the first prefix
  shard has cleared stage/apply without reproducing the earlier prefix retry
  storm or unresolved-shard failure.
- shard `58/166` then completed the Branching merge cleanly. Native merge logs
  showed `5000/9795` and `9795/9795` progress, followed by `Merge completed:
  9795 applied, no failed`, then `merge_queued -> merged`. This verifies the
  sparse merge heartbeat/logging hook on a large prefix merge and advances the
  run to shard `59/166`.
- shard `59/166` then staged cleanly through the same prefix path with `9795`
  attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is now `merge_queued`; the merge job is
  running and had not emitted progress logs yet at the evidence refresh.
- shard `59/166` then completed the Branching merge cleanly. Native merge logs
  showed `5000/9795` and `9795/9795` progress, followed by `Merge completed:
  9795 applied, no failed`, then `merge_queued -> merged`. This provides a
  second consecutive large prefix shard with clean stage/apply/merge behavior
  and advances the run to shard `60/166`.
- shard `60/166` then staged cleanly through the same prefix path with `9795`
  attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is now `merge_queued`, giving a third
  consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- shard `60/166` then completed the Branching merge cleanly and the run
  advanced to shard `61/166`.
- shard `61/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is now `merge_queued`, giving a fourth
  consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- shard `61/166` then completed the Branching merge cleanly and the run
  advanced to shard `62/166`. The latest sanitized recovery snapshot shows
  shard `62/166` actively staging through the same `ipam.prefix` path with no
  row failures or last error. This keeps the prefix path moving through native
  stage and merge without reopening the earlier unresolved-shard issue.
- shard `62/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is now `merge_queued`, giving a fifth
  consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- shard `62/166` then completed the Branching merge cleanly and the run
  advanced to shard `63/166`. The latest sanitized recovery snapshot shows
  shard `63/166` actively staging through the same `ipam.prefix` path with no
  row failures or last error.
- shard `63/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is now `merge_queued`, giving a sixth
  consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- shard `63/166` then completed the Branching merge cleanly and the run
  advanced to shard `64/166`. The latest sanitized recovery snapshot shows
  shard `64/166` actively staging through the same `ipam.prefix` path with no
  row failures or last error.
- shard `64/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is now `merge_queued`, giving a seventh
  consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- shard `64/166` then completed the Branching merge cleanly and the run
  advanced to shard `65/166`. The latest sanitized recovery snapshot shows
  shard `65/166` actively staging through the same `ipam.prefix` path with no
  row failures or last error.
- shard `65/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is now `merge_queued`, giving an eighth
  consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- shard `65/166` then completed the Branching merge cleanly and the run
  advanced to shard `66/166`. The latest sanitized recovery snapshot shows
  shard `66/166` actively staging through the same `ipam.prefix` path with no
  row failures or last error.
- shard `66/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is now `merge_queued`, giving a ninth
  consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.
- shard `66/166` then completed the Branching merge cleanly and the run
  advanced to shard `67/166`. The latest sanitized recovery snapshot shows
  shard `67/166` actively staging through the same `ipam.prefix` path with no
  row failures or last error.
- shard `67/166` then staged cleanly through the same `ipam.prefix` path with
  `9795` attempted rows, `9795` applied rows, `9795` actual changes, `0` row
  failures, and `0` step retries. It is now `merge_queued`, giving a tenth
  consecutive prefix shard that cleared stage/apply without retry storms,
  unresolved-shard failures, or row failures.

## Execution Order

1. Runtime fallback reason remediation for models that still fall back after
   attempted pushdown.
2. Apply engine parity expansion (`bulk_orm`) for additional safe models.
3. Execution throughput smoothing with ledger-first scheduler evidence.
4. Maintain version-matrix, recovery/chaos, and observability gates as release
   requirements.

## Current Completion Audit Snapshot

Last refreshed during this tranche with:

```bash
poetry run invoke architecture-completion-audit
```

Result:
- completed checks: `14`
- failed checks: `0`
- external evidence gaps: `2`
- repo checks green: `true`

What this proves:
- apply-engine classification is complete for supported models.
- current `bulk_orm` safe set is explicit and audited.
- first-candidate `bulk_orm` parity guard tests exist for
  `dcim.virtualchassis` while the model remains adapter-backed.
- architecture audit gate is wired into local CI.
- destructive chaos harness is present.
- compatibility prune tooling and support-bundle evidence are present.
- fresh worker-kill recovery evidence exists for the local runtime harness.
- runtime evidence generation now includes scale-benchmark-derived fallback and
  scheduler-readiness checks.
- completed-run inconsistency is now both prevented on new final-shard merges
  and repairable through explicit live-run reconciliation before benchmark
  export.
- the approved field-scale runtime matrix artifact is fresh and passed.
- first-candidate `bulk_orm` parity guard tests exist and pass for
  `dcim.virtualchassis`, while keeping the model on the adapter engine until a
  faster implementation proves parity.

What it does not prove:
- every future fallback-heavy model has a shard-safe NQE contract.
- repeated live runs have no runtime fallback reasons.
- the latest local large-run benchmark is internally consistent enough to close
  the fallback or scheduler gates; after reconciliation it is correctly
  reported as an incomplete running run.
- additional relationship-heavy models are safe for `bulk_orm`.
- throughput smoothing should be enabled; current large-run evidence says
  scheduler overlap is a candidate after capacity review, but the run is not
  complete enough to close the scheduler gate.

Latest live-run verification:
- local execution run `119` was checked after RQ-aware stale-job recovery.
- run status is `running`, phase is `staging`, and current step is
  `48/166` for `ipam.prefix`.
- shard `46/166` merged after recovery with `9295` attempted rows, `9295`
  applied rows, `9295` actual changes, and `0` row failures.
- shard `47/166` then staged and merged with `9295` attempted rows, `9295`
  applied rows, `9295` actual changes, and `0` row failures, proving the
  recovery/fallback fix carried through a second native Branching merge.
- shard `48/166` then staged and merged with `9294` attempted rows, `9294`
  applied rows, `9294` actual changes, and `0` row failures. Its merge job
  emitted sparse progress messages at `5000/9294` and `9294/9294`, proving the
  new merge-progress hook is observable through native NetBox job logs.
- shard `49/166` then staged and merged with `9502` attempted rows, `9502`
  applied rows, `1265` actual changes, and `0` row failures.
- shard `50/166` then staged and merged with `9502` attempted rows, `9502`
  applied rows, `1289` actual changes, and `0` row failures.
- shard `51/166` also merged cleanly after the latest evidence refresh; the
  final live check shows `51` merged shards, no failed or timed-out shards, and
  the run waiting at shard `52/166`.
- shard `52/166` then staged and merged with `9502` attempted rows, `9502`
  applied rows, `1317` actual changes, and `0` row failures.
- the current step is `53/166` for `dcim.inventoryitem`; the refreshed recovery
  artifact shows `5000` attempted rows, `9502` estimated changes, and `0` row
  failures.
- this is valid recovery evidence, but not terminal large-run completion
  evidence.

## Field-Scale Evidence Runbook

The remaining architecture gates are intentionally runtime-evidence gates. They
should be closed with a field-scale sync or an exported support bundle, not by
weakening thresholds in the audit.

Required environment for live smoke:

```bash
export FORWARD_SMOKE_URL="https://fwd.app"
export FORWARD_SMOKE_USERNAME="<approved-user>"
export FORWARD_SMOKE_PASSWORD="<approved-password>"
export FORWARD_SMOKE_NETWORK_ID="<approved-network-id>"
export FORWARD_SMOKE_SNAPSHOT_ID="latestProcessed"
export FORWARD_SMOKE_SOURCE_NAME="<netbox-source-name>"
export FORWARD_SMOKE_SYNC_NAME="<netbox-sync-name>"
```

Recommended command when the local chaos sync and the field-scale benchmark sync
are different:

```bash
poetry run invoke field-scale-runtime-matrix --resume=True

poetry run invoke architecture-runtime-evidence \
  --sync-name ui-harness-sync \
  --capacity-worker-replicas 4 \
  --capacity-source-name "$FORWARD_SMOKE_SOURCE_NAME" \
  --capacity-query-fetch-concurrency 6 \
  --capacity-nqe-page-size 10000 \
  --scale-sync-name "$FORWARD_SMOKE_SYNC_NAME" \
  --run-field-scale

poetry run invoke architecture-completion-audit
```

Recommended command when support receives an exported large-run support bundle
instead of direct access to the NetBox run:

```bash
poetry run invoke architecture-runtime-evidence \
  --sync-name ui-harness-sync \
  --scale-input-json /path/to/sanitized-support-bundle.json

poetry run invoke architecture-completion-audit
```

Recommended command when the large execution run exists locally and should be
selected by run ID:

```bash
poetry run invoke architecture-runtime-evidence \
  --sync-name ui-harness-sync \
  --capacity-worker-replicas 4 \
  --capacity-source-name <netbox-source-name> \
  --capacity-query-fetch-concurrency 6 \
  --capacity-nqe-page-size 10000 \
  --scale-run-id <execution-run-id>

poetry run invoke architecture-completion-audit
```

If that live run was produced before the ledger completion invariant existed
and is marked completed with non-terminal steps, intentionally reconcile it
before benchmark export:

```bash
poetry run invoke architecture-runtime-evidence \
  --sync-name ui-harness-sync \
  --scale-run-id <execution-run-id> \
  --scale-reconcile
```

Evidence contract:

- `field_scale_runtime_matrix_verified` closes only when the approved live
  smoke matrix passes with sanitized evidence.
  The field-scale matrix can be run independently with
  `invoke field-scale-runtime-matrix --resume=True`, or one step at a time with
  `--step <matrix-step-name>`. Step-filtered runs intentionally write `partial`
  evidence until all required matrix steps have passed.
  Runtime evidence records per-step timeout details for the smoke matrix so a
  slow plan-only query leaves a diagnosable failed artifact instead of timing
  out the whole evidence run. Use `FORWARD_SMOKE_STEP_TIMEOUT_SECONDS` for
  approved long-running datasets. Use `FORWARD_SMOKE_MODELS` only for scoped
  exploratory evidence, not for claiming full field-scale completion.
  The matrix also writes incremental sanitized evidence to
  `docs/03_Plans/evidence/field-scale-runtime-matrix.json` after start and
  after each step, so an interrupted field-scale run still leaves the latest
  completed step status. The main runtime evidence references that artifact
  under `field_scale_runtime_matrix_verified.evidence.artifact_path`.
  When `--run-field-scale` is omitted, `architecture-runtime-evidence` reuses
  that artifact if it exists and is fresh, so a completed field-scale smoke can
  be folded into the completion audit without rerunning the smoke matrix.
  Latest refreshed runtime evidence reused the existing artifact and recorded
  `field_scale_status=artifact-passed`, closing the field-scale matrix gate.
  The approved smoke matrix durations were approximately 16 seconds for
  Branching validate-only, 180 seconds for Branching plan-only, and 17 seconds
  for fast-bootstrap validate-only.
- `runtime_fallback_reduction_evidence_verified` closes only when
  `forward_scale_benchmark` sees at least `FORWARD_ARCH_RUNTIME_MIN_STEPS`
  runtime steps, currently defaulting to `4`, core support-bundle checks pass,
  and fallback checks are pass/info.
- `scheduler_overlap_readiness_evidence_verified` closes only when the same
  scale benchmark has enough steps, core support-bundle checks pass, and the
  throughput-smoothing check reports either `not_indicated` or
  `candidate_after_capacity_review`.
- If the benchmark uses an exported support bundle instead of a live sync, keep
  the artifact sanitized and pass it through `--scale-input-json`; the runtime
  evidence task writes the normalized report to
  `docs/03_Plans/evidence/scale-runtime-evidence.json` before the completion
  audit.
- `--scale-reconcile` is only for live `--scale-run-id` or
  `--scale-sync-name` selectors. It is intentionally rejected for
  `--scale-input-json`, because offline support bundles are immutable evidence.
- `forward_scale_benchmark --input-json` rejects bundles that match configured
  sensitive-content patterns before they can be used as architecture evidence.
  Add local-only customer names, tenant labels, network IDs, or snapshot IDs to
  `.sensitive-patterns.local.txt` before validating field evidence.

## Validation Gates

- `invoke harness-check`
- `invoke harness-test`
- `invoke check`
- `invoke scenario-test`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback

- Revert per-workstream changes independently.
- Keep ledger/readability and current fallback behaviors intact during rollback.
- Do not remove fallback paths until replacement behavior is fully validated.

## Decision Log

- Keep this roadmap in `active/` while execution remains in progress; move to
  `completed/` only when all workstreams satisfy success criteria.
- Prefer deterministic, architecture-safe speed work (shard fetch efficiency,
  parity-gated apply acceleration) before introducing broader workflow changes.
- Keep observability work coupled to speed work so regressions are diagnosable
  from native health/support surfaces without ad hoc data collection.

## Success Criteria

- Lower median and tail runtime for large Branching runs.
- Fewer fallback fetch steps across repeated runs.
- No loss of row-shape parity or mutation correctness.
- Faster incident triage from support bundles and health surfaces.
- Single code path behavior across supported NetBox versions with explicit gates.
