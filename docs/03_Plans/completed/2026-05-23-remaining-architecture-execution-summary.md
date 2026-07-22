# 2026-05-23 Remaining Architecture Execution Summary

## Goal

Summarize current execution status from the active long-term roadmap and keep a
concise, template-compliant artifact for remaining architecture work.

## Constraints

- Keep NQE as the source of truth for normalization and row shaping.
- Keep NetBox-native mutation paths and Branching semantics intact.
- Do not include customer identifiers or sensitive environment data.

## Touched Surfaces

- `docs/03_Plans/active/2026-05-23-remaining-architecture-execution-summary.md`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`

## Approach

Current state:
- Long-term architecture remains `in_progress`.
- Completed in recent tranches:
  - shard partition fetch parallelism (full + diff)
  - support-bundle per-model fetch/runtime/row metrics
  - pushdown advisory scoring (fallback/pushdown rates + hotspots)
  - pushdown trend snapshots surfaced in Sync Health/support bundle
  - source-configurable pushdown alert thresholds
  - explicit diff-baseline correlation in trend snapshots for diff-eligible
    non-diff steps
  - sync-level long-window pushdown trend export endpoint for historical
    retention and offline analysis
  - hotspot-aware tuning guidance in Sync Health/support metrics
  - runtime-aware branch budget shaping (planning + overflow re-splitting)
  - adaptive density-learning hardening (outlier rejection + confidence metadata)
  - confidence-informed branch budget policy (high confidence uses learned
    density, medium blends, low falls back to baseline)
  - compatibility `_branch_run` runtime write-path retirement with ledger-first
    continuation and prune tooling
  - partition retry split hardening for shard-scoped fetch (`EQUALS_ANY`
    partition split retries before model/full fallback)
  - scheduler-overlap readiness evidence in support-bundle throughput
    smoothing metrics
  - single-value column-filter alternate-operator retry before full/model
    fallback
  - scale benchmark report generation from execution-run support-bundle metrics
  - baseline-to-diff transition evidence in support bundles, Sync Health, and
    scale benchmark output

Remaining workstreams:
1. Model contract registry call-site migration
2. Runtime fallback reduction after attempted shard pushdown
3. Apply engine expansion with parity gates
4. Delete/dependency planning for filtered-sync delete waves
5. Execution throughput smoothing
6. Capability gates for future NetBox/bulk behavior
7. Release-gate maintenance for recovery/version-matrix coverage

Completed current tranche:
1. enqueue-time recovery reconciliation in resumable branching
   - objective: reconcile stale/stranded execution-run state before next-step
     queue selection to reduce false stalls and manual intervention
   - implemented in:
     - `forward_netbox/utilities/resumable_branching.py`
       (`enqueue_branch_stage_job` now reconciles and refreshes ledger state
       before queue decision)
   - validation status:
     - targeted and broader suites passed:
       - `forward_netbox.tests.test_jobs`
       - `forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest`
       - `forward_netbox.tests.test_ingestion_merge`
     - gates passed:
       - `poetry run invoke harness-check`
       - `poetry run invoke check`
2. reconcile-time stale-step auto-recovery depth
   - implemented:
     - stale queued stage steps with no branch/live job auto-reset to `pending`
     - stale running stage steps with no branch/live job auto-requeue to
       `queued`
     - support bundle exposes `recovery_policy_summary` markers for auto-policy
       evidence
   - validation status:
     - targeted and broader suites passed:
       - `forward_netbox.tests.test_synthetic_scenarios`
       - `forward_netbox.tests.test_api_views.ForwardExecutionRunAPIViewTest`
       - `forward_netbox.tests.test_log_export`
       - `forward_netbox.tests.test_jobs`
       - `forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest`
       - `forward_netbox.tests.test_ingestion_merge`
     - gates passed:
       - `poetry run invoke harness-check`
       - `poetry run invoke check`
3. deterministic branch-stale escalation policy
   - implemented:
     - repeated branch-associated stale reconciliation reasons now trigger a
       deterministic escalation recommendation:
       - `recovery_recommendation.action = manual_intervention`
       - reason/count fields included for operator triage
     - support bundle now includes escalation counters and threshold flags under
       `recovery_policy_summary`
   - validation status:
     - targeted + broader suites passed:
       - `forward_netbox.tests.test_synthetic_scenarios`
       - `forward_netbox.tests.test_jobs`
       - `forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest`
       - `forward_netbox.tests.test_ingestion_merge`
       - `forward_netbox.tests.test_api_views.ForwardExecutionRunAPIViewTest`
       - `forward_netbox.tests.test_log_export`
     - gates passed:
       - `poetry run invoke harness-check`
       - `poetry run invoke check`
4. bounded auto-requeue for merge-timeout in auto-merge runs
   - implemented:
     - merge-timeout path now attempts automatic merge requeue using native
       merge job enqueue
     - automatic requeue is bounded by
       `AUTO_MERGE_STALE_MERGE_REQUEUE_LIMIT` to prevent unbounded loops
     - stage queueing is suppressed when execution run is not `running`
       (prevents timeout-state runs from queuing next stage shard)
   - validation status:
     - targeted + broader suites passed:
       - `forward_netbox.tests.test_ingestion_merge`
       - `forward_netbox.tests.test_jobs`
       - `forward_netbox.tests.test_synthetic_scenarios`
       - `forward_netbox.tests.test_log_export`
     - gates passed:
       - `poetry run invoke harness-check`
       - `poetry run invoke check`
5. run-level no-progress watchdog automation
   - implemented:
     - reconcile flow now emits `stale_run_no_progress_watchdog` run events for
       stale run-heartbeat conditions (interval guarded)
     - support bundle includes watchdog counters/threshold flags in
       `recovery_policy_summary`
     - recovery recommendation escalates to `manual_intervention` when watchdog
       threshold is reached
   - validation status:
     - targeted + broader suites passed:
       - `forward_netbox.tests.test_synthetic_scenarios`
       - `forward_netbox.tests.test_ingestion_merge`
       - `forward_netbox.tests.test_jobs`
       - `forward_netbox.tests.test_log_export`
       - `forward_netbox.tests.test_api_views.ForwardExecutionRunAPIViewTest`
       - `forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest`
     - gates passed:
       - `poetry run invoke harness-check`
       - `poetry run invoke check`
6. per-model fallback budget guardrails
   - implemented:
     - pushdown efficiency now emits per-model fallback budget guardrails and
       threshold metadata
     - warning status is raised when model-level fallback budget is exceeded
       with sufficient stage-step sample depth
     - tuning guidance now includes `model_fallback_budget_guardrail`
   - validation status:
     - targeted + broader suites passed:
       - `forward_netbox.tests.test_health`
       - `forward_netbox.tests.test_log_export`
       - `forward_netbox.tests.test_synthetic_scenarios`
       - `forward_netbox.tests.test_ingestion_merge`
       - `forward_netbox.tests.test_jobs`
     - gates passed:
       - `poetry run invoke harness-check`
       - `poetry run invoke check`
7. architecture completion audit refresh
   - implemented/validated:
     - repo architecture checks are green
     - apply-engine classification covers every supported model
     - current `bulk_orm` safe set is audited
     - destructive chaos harness exists and runtime evidence is fresh
     - compatibility prune/support-bundle evidence is present
   - validation status:
     - `poetry run invoke architecture-completion-audit`
     - result summary:
       - completed checks: `13`
       - failed checks: `0`
       - missing external evidence: `3`
8. single-branch multi-version CI matrix
   - implemented:
     - GitHub CI validation job now runs against `NETBOX_VER=v4.5.9`
     - GitHub CI validation job now runs against `NETBOX_VER=v4.6.0`
     - same job body is used for both versions to keep one product path
   - validation status:
     - local harness doc gate passes
     - GitHub Actions must prove the expanded matrix after push/PR
9. throughput smoothing instrumentation baseline
   - implemented:
     - support-bundle metrics include `throughput_smoothing`
     - per-step metrics include stage queue, merge queue, merge wait, stage
       duration, and merge duration timings
     - bottleneck scoring can report `queue_or_merge_wait`
     - tuning guidance emits `throughput_wait_pressure` when wait share is high
   - validation status:
     - targeted synthetic support-bundle test passed
     - execution-run support-bundle export test passed
10. runtime fallback reason aggregation
  - implemented:
    - execution-run support bundles include `fallback_reason_summary`
    - Sync Health query-pushdown summary includes the same reason aggregation
    - reasons are counted globally and per model
    - model/full/diff fallback modes get deterministic default reason codes
      when no concrete runtime exception reason was captured
  - validation status:
    - targeted synthetic support-bundle test passed
    - execution-run support-bundle export test passed
    - Sync Health summary test passed
11. large-run self-service tuning summaries
   - implemented:
     - execution-run support bundles include `operator_tuning_summary`
     - Sync Health includes `large_run_tuning`
     - top-level Sync Health checks include `Large-run tuning`
     - first-order actions are ranked from diff utilization, fallback fetch,
       timeout/capacity, throughput wait, bottleneck, and query fetch
       concurrency signals
   - validation status:
     - execution-run support-bundle export test passed
     - Sync Health summary/render tests passed
12. scheduler-overlap readiness gate
   - implemented:
     - support-bundle `throughput_smoothing` includes
       `scheduler_overlap_readiness`
     - readiness distinguishes insufficient evidence, not indicated,
       needs more runtime evidence, and candidate-after-capacity-review states
     - readiness records the dominant wait component and the preconditions that
       must be met before bounded scheduler overlap is implemented
   - validation status:
     - py_compile passed for execution metrics and support-bundle tests
     - focused synthetic support-bundle and log-export tests passed
13. single-value native column-filter fallback reduction
   - implemented:
     - full and diff shard partition fetch paths retry equivalent single-value
       column-filter operators before escalating to full/model fallback
     - `DEFAULT` can retry as `EQUALS_ANY`; single-value `EQUALS_ANY` can retry
       as `DEFAULT`
     - count-only `partition_retry_summary` metadata records retry attempts and
       successes without storing row data
     - execution support bundles and Sync Health aggregate retry attempts,
       successes, avoided fallback count, and per-model/per-operation counts
     - local shard safety filtering remains unchanged
   - validation status:
     - py_compile passed for query fetch execution and sync tests
     - focused full/diff partition and fallback tests passed
     - focused support-bundle, Sync Health, and synthetic support-bundle tests
     passed for run-level retry aggregation
14. scale benchmark report baseline
   - implemented:
     - `forward_scale_benchmark` evaluates support-bundle evidence from:
       - latest run for a sync
       - specific execution run ID
       - exported support-bundle JSON
     - `invoke scale-benchmark` writes a reusable JSON report artifact under
       `docs/03_Plans/evidence/` by default
     - checks summarize completion, row failures, fallback rate/runtime share,
       diff utilization, partition retry pressure, throughput wait, and
       apply-engine evidence without storing row data
   - validation status:
     - focused utility/management-command tests added
     - task wrapper tests added
15. baseline-to-diff transition evidence
   - implemented:
     - `diff_baseline_transition` is emitted from execution-run metrics
     - Sync Health renders the transition as `Baseline to diff`
     - scale benchmark checks the same transition evidence
     - transition codes distinguish active API diffs, Fast bootstrap baseline
       mode, missing diff-capable query identity, missing/ineligible baseline,
       baseline-present full mode, diff fallback, and mixed states
   - validation status:
     - focused support-bundle, Sync Health, and scale-benchmark tests passed
16. model contract registry baseline
   - implemented:
     - `forward_netbox.utilities.model_contracts` composes supported model
       sync row contracts, coalesce identity, shard fetch contracts, delete
       dependency order, apply-engine eligibility/blocker evidence, and
       support-safe diagnostic fields
     - architecture audit emits `model_contract_registry`
     - completion audit includes `model_contract_registry_complete`
     - `--fail-on-gap` now fails for model contract registry gaps
   - validation status:
    - py_compile passed for the registry, architecture audit command,
      completion audit command, and focused tests
    - focused architecture audit and completion audit Django tests passed
    - gates passed:
      - `poetry run invoke lint`
      - `poetry run invoke harness-check`
      - `poetry run invoke docs`
      - `poetry run invoke architecture-audit-check`
      - `poetry run invoke architecture-completion-audit`
      - `poetry run invoke check`
17. model contract registry call-site migration, first pass
   - implemented:
     - architecture audit fetch-contract coverage now reads through
       `architecture_fetch_contracts()`
     - Sync Health fetch-contract summaries now read through
       `architecture_fetch_contract_for_model()`
     - Sync Health now reports contract registry status/gap counts with fetch
       contract metrics
   - validation status:
     - py_compile passed for the registry, health fetch summary,
       architecture audit command, and focused tests
     - focused Sync Health and architecture audit Django tests passed
     - gates passed:
       - `poetry run invoke lint`
       - `poetry run invoke harness-check`
     - `poetry run invoke docs`
     - `poetry run invoke architecture-audit-check`
     - `poetry run invoke architecture-completion-audit`
     - `poetry run invoke check`
18. model contract registry call-site migration, second pass
   - implemented:
     - added registry helpers for safe bulk-ORM models, adapter-required
       models, and adapter blocker-code maps
     - architecture audit now reports `bulk_orm_safe_models`,
       `adapter_required_models`, and `adapter_blockers` through those helpers
     - apply-engine gap checks still use the authoritative gap constants, so
       failure behavior is unchanged
   - validation status:
     - py_compile passed for the registry, architecture audit command,
       architecture completion audit command, and focused tests
     - focused architecture audit and completion audit Django tests passed
     - `poetry run invoke lint` passed
19. model contract registry call-site migration, third pass
   - implemented:
     - added `architecture_default_coalesce_fields_for_model()` as the
       registry-backed fallback coalesce helper
     - query registry built-in defaults now use the helper
     - sync execution fallback model coalesce fields now use the helper
     - query fetch preflight/planning fallback coalesce fields now use the
       helper
     - row-shape validation remains in `sync_contracts`, preserving the
       existing validation boundary
   - validation status:
     - py_compile passed for the registry, query fetch execution, sync
       execution, and query registry
     - focused query registry, sync fetch, and architecture audit Django tests
       passed
     - `poetry run invoke lint` passed
     - gates passed:
       - `poetry run invoke harness-check`
       - `poetry run invoke docs`
       - `poetry run invoke architecture-audit-check`
       - `poetry run invoke architecture-completion-audit`
       - `poetry run invoke check`
20. model contract registry call-site migration, fourth pass
   - implemented:
     - added registry helpers for unclassified supported models,
       adapter-required models without blocker codes, and bulk-ORM-safe models
       without implemented specs
     - architecture audit classification gaps now read through those helpers
     - model contract gap detection now checks adapter blocker coverage from
       the contract classification
     - apply-engine runtime selection remains unchanged
   - validation status:
     - py_compile passed for the registry, architecture audit command, and
       architecture audit tests
     - focused architecture audit and completion audit Django tests passed
     - `poetry run invoke lint` passed
     - gates passed:
       - `poetry run invoke harness-check`
       - `poetry run invoke docs`
       - `poetry run invoke architecture-audit-check`
       - `poetry run invoke architecture-completion-audit`
       - `poetry run invoke check`
21. capacity profile documentation baseline
   - implemented:
     - added small, medium, large, and very-large runtime sizing profiles to
       `docs/01_User_Guide/configuration.md`
     - tied each profile to backend guidance, query fetch concurrency/page size,
       worker timeout posture, and first Sync Health/support-bundle signals
     - preserved the source-of-truth boundary: NQE shapes data, shared
       validation remains common, and backend selection changes only review or
       write mechanics
   - validation status:
     - `poetry run invoke docs` passed
22. run-local fetch artifact baseline
   - implemented:
     - added bounded runtime-only fetch artifacts for shard-scoped
       retry/resume reuse
     - artifact identity includes execution run, query identity, snapshot,
       baseline snapshot, shard keys, fetch/query parameters, column filters,
       and hashed device-tag scope
     - support metadata records only artifact status/count/size fields, not
       row payloads
     - artifact directories are pruned on normal ledger completion,
       reconcile-time completion, and branch-run failure
   - validation status:
     - focused shard artifact reuse and cleanup tests passed
23. delete/dependency planning baseline
   - implemented:
     - plan previews now include `delete_dependency_plan`
     - delete-heavy plans expose delete rows, delete shards, delete share,
       max delete shard size, model execution order, dependency rank,
       dependent-model count, and reference-blocker risk
     - warning codes identify delete waves, near-budget delete shards, and
       dependency-anchor models before merge
   - validation status:
     - focused branch-budget and plan-preview tests passed

Immediate next tranche:
1. model contract registry call-site migration, continued
   - use the new registry as the model-specific rule surface for future speed
     work
   - migrate duplicate rule reads only when behavior-preserving and
     test-covered
2. runtime fallback remediation for repeated fallback reasons after pushdown is
   attempted
   - use trend exports/support bundles to identify repeated fallback reasons
   - avoid changing NQE semantics unless the query-side contract is proven
3. parity-safe `bulk_orm` expansion where contract parity is proven
4. execution throughput smoothing scheduler work (bounded overlap + adaptive
   ceilings) only after metrics show queue/merge-wait pressure
5. recovery/version-matrix gate maintenance for future runtime changes

## Validation

Architecture release-readiness criteria:
- fallback-step rate trends down across repeated large runs
- diff utilization trends up where query-id is configured
- mutation parity stays intact for create/update/delete
- support bundle + health explain regressions without ad hoc logging
- one capability-gated code path works across supported NetBox versions
- model contracts become the single place to understand supported model fetch,
  delete, apply, and diagnostic behavior

Current-tranche validation evidence:
- `forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_spec_rows_reuses_run_local_artifact_for_shard_retry`
- `forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_artifacts_are_pruned_when_execution_run_completes`
- `forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_fetch_artifacts_are_pruned_when_execution_run_fails`
- `forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest.test_delete_dependency_summary_surfaces_delete_wave_risk`
- `forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest.test_plan_preview_includes_delete_dependency_plan`
- `forward_netbox.tests.test_jobs` (enqueue/requeue/reconcile behavior)
- `forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest`
- `forward_netbox.tests.test_ingestion_merge`
- `forward_netbox.tests.test_architecture_audit_command`
- `forward_netbox.tests.test_architecture_completion_audit_command`
- `poetry run invoke architecture-completion-audit`
- `poetry run invoke harness-check`
- `poetry run invoke check`

## Rollback

- Keep this document as status-only and non-invasive.
- If the summary diverges from roadmap truth, refresh from
  `2026-05-23-long-term-architecture-roadmap.md` and rerun `invoke harness-check`.

## Decision Log

- This file remains concise by design and points to the full roadmap for
  implementation detail.
- Summary uses plan-template headings to satisfy harness-check in `active/`.
- Recovery-automation work is prioritized over broad new feature work because
  it directly reduces long-run operational stalls seen in field runs.
- Recovery automation is no longer the largest open architecture risk for the
  current baseline; the next meaningful speed risk is reducing fallback-heavy
  model fetches and expanding apply acceleration only where parity is proven.
- Single-branch multi-version behavior is now represented in CI rather than
  only in docs; future support additions should extend that matrix.
- Shard-fetch contract coverage is complete for the current supported model set;
  the remaining pushdown work is runtime fallback reduction, not missing model
  contracts.
- The model contract registry is now present and audited; the remaining work is
  call-site consolidation and future model additions through that registry.
