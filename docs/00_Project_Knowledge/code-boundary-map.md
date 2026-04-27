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

## Branch Planning Boundary

- Owner: `forward_netbox/utilities/branch_budget.py`
- Responsibilities: workload grouping, deterministic shard creation, model-density row budgets, and branch-budget validation.
- Required tests: deterministic splitting, over-budget buckets, density-based budgets, and delete/upsert mix handling.

## Branch Execution Boundary

- Owner: `forward_netbox/utilities/multi_branch.py`
- Responsibilities: snapshot context, preflight, plan state, branch lifecycle, branch-budget overflow retry, auto-merge, and resume behavior.
- Required tests: preflight fail-fast, state transitions, retry behavior, auto-merge interaction, and stale branch recovery.

## NetBox Adapter Boundary

- Owner: `forward_netbox/utilities/sync.py`
- Responsibilities: row validation handoff, coalesce upsert/delete, dependency failure handling, and per-model NetBox object application.
- Required tests: model-specific row behavior, dependency skip behavior, coalesce ambiguity, and field bounds.

## Plugin State Boundary

- Owner: `forward_netbox/models.py`
- Responsibilities: persisted plugin models, sync parameters, branch-run state, density persistence, job enqueueing, and merge handoff.
- Required tests: parameter validation, state helpers, job enqueue behavior, and ingestion metadata.

## UI/API Boundary

- Owner: `forward_netbox/views.py`, `forms.py`, `api/`, templates, tables, and filtersets.
- Responsibilities: NetBox UI forms, object views, action buttons, API actions, and display of logs/statistics/issues.
- Required tests: form cleaning, API actions, permissions, and visible workflow docs/screenshots when changed.
