# Forward NetBox Architecture

`forward_netbox` is a NetBox plugin with one primary workflow: fetch Forward Networks data through Forward API/NQE, transform rows into NetBox model operations, stage those operations in NetBox Branching branches, and optionally merge those branches.

## Runtime Flow

1. A `ForwardSource` stores Forward connection settings and resolves available networks.
2. A `ForwardSync` selects models, snapshot mode, branch budget, and auto-merge behavior.
3. The sync job resolves the Forward snapshot, validates query shape, fetches NQE rows, and builds either a Branching plan or a fast bootstrap workload.
4. A `ForwardValidationRun` records pre-branch drift/policy results. Blocking policies stop the sync before branch creation.
5. The selected execution backend applies rows through the same NetBox model adapters.
6. The Branching backend creates reviewable native Branching shards; the fast bootstrap backend writes directly after validation for large initial imports.
7. `ForwardIngestion`, `ForwardValidationRun`, and `ForwardIngestionIssue` retain run metadata, logs, statistics, and issues.

NQE remains the source of truth for normalization and model-shaped rows. Execution
backends may decide how validated rows are applied to NetBox, but they must not
introduce separate Python-side data mutation rules that diverge from the NQE map
contracts.

## Production Boundaries

- Plugin state and job entrypoints: `forward_netbox/models.py`
- UI workflow: `forward_netbox/views.py`, `forms.py`, `tables.py`, and templates
- REST API workflow: `forward_netbox/api/`
- Forward API client: `forward_netbox/utilities/forward_api.py`
- Sync contracts: `forward_netbox/utilities/sync_contracts.py`
- Sync validation: `forward_netbox/utilities/model_validation.py`
- Query registry and shipped query loading: `forward_netbox/utilities/query_registry.py` and `forward_netbox/queries/`
- Query fetch, snapshot resolution, NQE execution, and model-result reporting: `forward_netbox/utilities/query_fetch.py`
- Diagnostic synthesis for IPAM/routing query warnings: `forward_netbox/utilities/query_diagnostics.py`
- Branch planning and branch-budget behavior: `forward_netbox/utilities/branch_budget.py`
- Multi-branch planning: `forward_netbox/utilities/multi_branch_planner.py`
- Multi-branch execution and retry behavior: `forward_netbox/utilities/multi_branch_executor.py`
- Fast bootstrap direct-write execution: `forward_netbox/utilities/fast_bootstrap_executor.py`
- Direct sync-stage execution: `forward_netbox/utilities/sync_execution.py`
- Multi-branch lifecycle helpers for branch creation, overflow retry, and resume state: `forward_netbox/utilities/multi_branch_lifecycle.py`
- Ingestion merge orchestration and signal suppression: `forward_netbox/utilities/ingestion_merge.py`
- Sync job orchestration and failure capture: `forward_netbox/utilities/sync_orchestration.py`
- Validation and drift-policy evaluation: `forward_netbox/utilities/validation.py`
- Validation force-allow audit helper: `forward_netbox/utilities/validation.py`
- NetBox row application and model adapters: `forward_netbox/utilities/sync.py`
- Row reporting, issue capture, shard heartbeat logging, and per-row continue-on-error handling: `forward_netbox/utilities/sync_reporting.py`
- Generic coalesce, upsert, delete-by-coalesce, and model lookup primitives: `forward_netbox/utilities/sync_primitives.py`
- Sync state, progress heartbeat, stale-progress activity, and execution-summary helpers: `forward_netbox/utilities/sync_state.py`
- Sync event flushing and clear-events bridging: `forward_netbox/utilities/sync_events.py`
- Sync facade helpers for snapshot resolution, NQE map access, query parameters, and job enqueueing: `forward_netbox/utilities/sync_facade.py`
- Logging/statistics: `forward_netbox/utilities/logging.py`
- Sensitive-content guard: `forward_netbox/utilities/sensitive_content.py` and `scripts/check_sensitive_content.py`

## Overgrown But Stable Areas

The following modules are intentionally treated as stable boundaries until a dedicated refactor plan exists:

- `forward_netbox/utilities/sync.py`: model-adjacent helper and shim glue, coalesce behavior, dependency failure handling, and row application.
- `forward_netbox/utilities/sync_runner_contracts.py`: runner conflict-policy, coalesce-identity, and diff-splitting contract helpers extracted from `sync.py`.
- `forward_netbox/utilities/sync_runner_adapters.py`: runner adapter and model-specific apply/delete helper family extracted from `sync.py`.
- `forward_netbox/utilities/sync_cable.py`: cable adapter apply/delete lookup helpers extracted from the main sync module.
- `forward_netbox/utilities/sync_interface.py`: interface, MAC address, and feature-tag adapter entrypoints extracted from the main sync module.
- `forward_netbox/utilities/sync_routing.py`: routing and peering helper logic plus apply/delete entrypoints extracted from the main sync module.
- `forward_netbox/utilities/sync_reporting.py`: row-level issue recording, dependency tracking, shard heartbeat logging, and aggregated warning/reporting helpers extracted from the main sync module.
- `forward_netbox/utilities/sync_primitives.py`: generic coalesce, upsert, delete-by-coalesce, optional-model, and lookup helpers extracted from the main sync module.
- `forward_netbox/models.py`: persisted model behavior, job state transitions, validation state, and branch-run state; validation override writes now delegate to `forward_netbox/utilities/validation.py`.
- `forward_netbox/utilities/sync_state.py`: branch-run state helpers, progress heartbeat, stale-progress display, display parameters, and sync activity summaries.
- `forward_netbox/utilities/sync_events.py`: event queue flush helper extracted from the main sync module.
- `forward_netbox/utilities/sync_facade.py`: remaining `ForwardSync` helper behavior, including snapshot resolution, enabled-model access, and enqueue wrappers.
- `forward_netbox/utilities/ingestion_merge.py`: ingestion merge orchestration plus merge-job enqueueing, change-total persistence, and branch cleanup.
- `forward_netbox/utilities/model_validation.py`: sync/source/NQE validation contract plus scheduled-time and enabled-model checks.
- `forward_netbox/utilities/multi_branch_planner.py`: query fetch, preflight, plan assembly, and model-result capture.
- `forward_netbox/utilities/query_diagnostics.py`: IPAM/routing diagnostic synthesis and warning aggregation extracted from the fetcher.
- `forward_netbox/utilities/multi_branch_executor.py`: branch execution, auto-merge, resume state, and overflow retry; the main state machine is now split into smaller helpers.
- `forward_netbox/utilities/sync_execution.py`: direct query/apply/delete sync-stage execution for the legacy non-branch runner path.
- `forward_netbox/utilities/multi_branch_lifecycle.py`: branch creation, branch cleanup, overflow retry, resume-state updates, and per-shard ingestion wiring.
- `forward_netbox/utilities/sync_orchestration.py`: sync job orchestration, status transitions, and failure capture.
- `forward_netbox/utilities/multi_branch.py`: compatibility shim that re-exports the planner and executor surfaces.
- `forward_netbox/utilities/sync_runner_adapters.py`: runner adapter and model-specific apply/delete helper family extracted from `sync.py`.
- `forward_netbox/utilities/sync_runner_contracts.py`: runner conflict-policy, coalesce-identity, and diff-splitting contract helpers extracted from `sync.py`.

Do not move code out of these modules as drive-by cleanup. Refactors should first add or update tests that pin the existing behavior.

## Intended Future Layers

Current and future refactors should stay inside these smaller layers without changing public behavior:

- contracts: validation of row shape, model identity, and coalesce rules
- query fetch: snapshot resolution, query execution, pagination, diffs, and per-model results
- planning: workload grouping, shard sizing, and branch budget estimation
- execution: branch lifecycle, shard retries, merge handoff, and resume state
- validation: pre-branch policy decisions, drift summaries, and blocking reasons
- adapters: per-NetBox-model row apply/delete behavior
- reporting: logs, statistics, model results, issues, row failures, and operator-facing progress
- primitives: generic coalesce, lookup, and update-or-create behavior shared by adapters

## Non-Negotiable Constraints

- Keep the default sync and merge behavior NetBox-native and Branching-native.
- Preserve the UI/API sync workflow; large-dataset behavior must be selected through the sync execution backend, not a separate tool.
- Keep normalization and model shaping in NQE; Python execution paths should consume the same native NetBox-shaped row contracts.
- Keep branch budgets configurable and bounded according to NetBox Branching guidance.
- Never persist customer data, credentials, private network IDs, or snapshot IDs in committed tests/docs.
- Keep shipped query changes paired with tests and reference documentation.
