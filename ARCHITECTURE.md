# Forward NetBox Architecture

`forward_netbox` is a NetBox plugin with one primary workflow: fetch Forward Networks data through Forward API/NQE, transform rows into NetBox model operations, stage those operations in NetBox Branching branches, and optionally merge those branches.

## Runtime Flow

1. A `ForwardSource` stores Forward connection settings and resolves available networks.
2. A `ForwardSync` selects models, snapshot mode, branch budget, and auto-merge behavior.
3. The sync job resolves the Forward snapshot, validates query shape, fetches NQE rows, and builds a branch plan.
4. A `ForwardValidationRun` records pre-branch drift/policy results. Blocking policies stop the sync before branch creation.
5. Each branch shard applies rows through NetBox model adapters under an active Branching branch.
6. The branch is reviewed or merged through native NetBox Branching behavior.
7. `ForwardIngestion`, `ForwardValidationRun`, and `ForwardIngestionIssue` retain run metadata, logs, statistics, and issues.

## Production Boundaries

- Plugin state and job entrypoints: `forward_netbox/models.py`
- UI workflow: `forward_netbox/views.py`, `forms.py`, `tables.py`, and templates
- REST API workflow: `forward_netbox/api/`
- Forward API client: `forward_netbox/utilities/forward_api.py`
- Sync contracts: `forward_netbox/utilities/sync_contracts.py`
- Query registry and shipped query loading: `forward_netbox/utilities/query_registry.py` and `forward_netbox/queries/`
- Query fetch, snapshot resolution, NQE execution, and model-result reporting: `forward_netbox/utilities/query_fetch.py`
- Branch planning and branch-budget behavior: `forward_netbox/utilities/branch_budget.py`
- Multi-branch planning: `forward_netbox/utilities/multi_branch_planner.py`
- Multi-branch execution and retry behavior: `forward_netbox/utilities/multi_branch.py`
- Validation and drift-policy evaluation: `forward_netbox/utilities/validation.py`
- NetBox row application and model adapters: `forward_netbox/utilities/sync.py`
- Logging/statistics: `forward_netbox/utilities/logging.py`
- Sensitive-content guard: `forward_netbox/utilities/sensitive_content.py` and `scripts/check_sensitive_content.py`

## Overgrown But Stable Areas

The following modules are intentionally treated as stable boundaries until a dedicated refactor plan exists:

- `forward_netbox/utilities/sync.py`: model-adjacent helper and shim glue, coalesce behavior, dependency failure handling, and row application.
- `forward_netbox/utilities/sync_cable.py`: cable adapter apply/delete lookup helpers extracted from the main sync module.
- `forward_netbox/utilities/sync_interface.py`: interface, MAC address, and feature-tag adapter entrypoints extracted from the main sync module.
- `forward_netbox/utilities/sync_routing.py`: routing and peering helper logic plus apply/delete entrypoints extracted from the main sync module.
- `forward_netbox/models.py`: persisted model behavior, job state transitions, validation state, and branch-run state.
- `forward_netbox/utilities/multi_branch_planner.py`: query fetch, preflight, plan assembly, and model-result capture.
- `forward_netbox/utilities/multi_branch.py`: branch execution, auto-merge, resume state, and overflow retry.

Do not move code out of these modules as drive-by cleanup. Refactors should first add or update tests that pin the existing behavior.

## Intended Future Layers

Current and future refactors should stay inside these smaller layers without changing public behavior:

- contracts: validation of row shape, model identity, and coalesce rules
- query fetch: snapshot resolution, query execution, pagination, diffs, and per-model results
- planning: workload grouping, shard sizing, and branch budget estimation
- execution: branch lifecycle, shard retries, merge handoff, and resume state
- validation: pre-branch policy decisions, drift summaries, and blocking reasons
- adapters: per-NetBox-model row apply/delete behavior
- reporting: logs, statistics, model results, issues, and operator-facing progress

## Non-Negotiable Constraints

- Keep sync and merge behavior NetBox-native and Branching-native.
- Preserve the UI/API sync workflow; do not add a separate import path for large datasets.
- Keep branch budgets configurable and bounded according to NetBox Branching guidance.
- Never persist customer data, credentials, private network IDs, or snapshot IDs in committed tests/docs.
- Keep shipped query changes paired with tests and reference documentation.
