# Code Boundary Map

This map describes where changes belong. It is intentionally behavior-based rather than file-count based.

## Forward API Boundary

- Owner: `forward_netbox/utilities/forward_api.py`
- Responsibilities: authentication, NetBox proxy configuration, network/snapshot lookup, NQE pagination, NQE diff execution, and response parsing.
- Required tests: API parsing, proxy behavior, pagination, and error conversion.

## Query Registry Boundary

- Owner: `forward_netbox/utilities/query_registry.py` and `forward_netbox/queries/`
- Responsibilities: shipped NQE query discovery, raw query flattening, `query_id` support, coalesce maps, and model-to-query registration.
- Required tests: registry loading, import flattening, query ID behavior, and sensitive-content checks.

## Query Fetch Boundary

- Owner: `forward_netbox/utilities/query_fetch.py`
- Responsibilities: snapshot context resolution, preflight sample execution, full NQE execution, NQE diff fallback, row-shape validation handoff, and per-model result metadata.
- Required tests: preflight fail-fast, diff/full fallback behavior, model-result persistence, and smoke validation output.

## Branch Planning Boundary

- Owner: `forward_netbox/utilities/branch_budget.py`
- Responsibilities: workload grouping, deterministic shard creation, model-density row budgets, and branch-budget validation.
- Required tests: deterministic splitting, over-budget buckets, density-based budgets, and delete/upsert mix handling.

## Branch Execution Boundary

- Owner: `forward_netbox/utilities/multi_branch.py`
- Responsibilities: plan state, branch lifecycle, branch-budget overflow retry, auto-merge, validation handoff, and resume behavior.
- Required tests: preflight fail-fast, state transitions, retry behavior, auto-merge interaction, and stale branch recovery.

## Validation Boundary

- Owner: `forward_netbox/utilities/validation.py`
- Responsibilities: validation-run persistence, drift-policy checks, blocking reasons, and pre-branch sync gating.
- Required tests: policy validation, blocked-before-branch behavior, standalone validation jobs, and UI/API visibility.

## NetBox Adapter Boundary

- Owner: `forward_netbox/utilities/sync.py`
- Responsibilities: row validation handoff, coalesce upsert/delete, dependency failure handling, and per-model NetBox object application.
- Required tests: model-specific row behavior, dependency skip behavior, coalesce ambiguity, and field bounds.

## Plugin State Boundary

- Owner: `forward_netbox/models.py`
- Responsibilities: persisted plugin models, sync parameters, branch-run state, density persistence, validation/drift models, job enqueueing, and merge handoff.
- Required tests: parameter validation, state helpers, job enqueue behavior, and ingestion metadata.

## UI/API Boundary

- Owner: `forward_netbox/views.py`, `forms.py`, `api/`, templates, tables, and filtersets.
- Responsibilities: NetBox UI forms, object views, action buttons, API actions, and display of logs/statistics/issues.
- Required tests: form cleaning, API actions, permissions, and visible workflow docs/screenshots when changed.
