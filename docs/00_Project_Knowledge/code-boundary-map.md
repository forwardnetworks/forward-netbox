# Code Boundary Map

This map describes where changes belong. It is intentionally behavior-based rather than file-count based.

## Forward API Boundary

- Owner: `forward_netbox/utilities/forward_api.py`
- Responsibilities: authentication, NetBox proxy configuration, network/snapshot lookup, NQE pagination, NQE diff execution, and response parsing.
- Required tests: API parsing, proxy behavior, pagination, and error conversion.

## Query Registry Boundary

- Owner: `forward_netbox/utilities/query_registry.py`,
  `forward_netbox/utilities/query_binding.py`, and `forward_netbox/queries/`
- Responsibilities: shipped NQE query discovery, raw query flattening, `query_id`
  support, repository-path binding, local and explicit live query-drift
  classification, pinned/latest commit guidance, coalesce maps, and
  model-to-query registration.
- Required tests: registry loading, import flattening, query ID behavior,
  repository-path binding, query-drift classification, commit-binding guidance,
  and sensitive-content checks.

## Query Fetch Boundary

- Owner: `forward_netbox/utilities/query_fetch.py`
- Responsibilities: snapshot context resolution, preflight sample execution, full NQE execution, NQE diff fallback, row-shape validation handoff, and per-model result metadata.
- Required tests: preflight fail-fast, diff/full fallback behavior, model-result persistence, and smoke validation output.

## Branch Planning Boundary

- Owner: `forward_netbox/utilities/branch_budget.py`
- Responsibilities: workload grouping, deterministic shard creation,
  model-density row budgets, branch-budget validation, and per-model
  fetch-contract capability/fallback reporting.
- Required tests: deterministic splitting, over-budget buckets, density-based
  budgets, delete/upsert mix handling, and fetch-contract reporting for fetch
  mode, schema contract, local safety-filter guarantee, and fallback reason.

## Branch Execution Boundary

- Owner: `forward_netbox/utilities/multi_branch.py`
- Responsibilities: compatibility exports for planning/execution surfaces,
  branch lifecycle, branch-budget overflow retry, auto-merge, validation
  handoff, and resume behavior.
- Required tests: preflight fail-fast, state transitions, retry behavior,
  auto-merge interaction, stale branch recovery, and compatibility imports.

## Execution Ledger Boundary

- Owner: `ForwardExecutionRun`, `ForwardExecutionStep`, and
  `forward_netbox/utilities/execution_ledger.py`
- Compatibility owner: `forward_netbox/utilities/resumable_branching.py` and
  `ForwardSync.parameters` while old execution-ledger state remains readable.
- Responsibilities: durable run/step state, atomic step claiming,
  job/branch/ingestion linkage, retry counts, fetch/apply metadata, stale-job
  recovery decisions, durable run evidence, guarded state transitions, and
  run-level support export. Fresh job-backed runs, stage enqueue, merge
  continuation, stage resume, runtime phase helpers, progress/failure helpers,
  and failure reconciliation should prefer ledger state whenever an execution
  run exists and only fall back to compatibility branch JSON when no run is
  available. Execution-run support export may include linked
  ingestion counters and sanitized issue summaries, but must not include raw
  row payloads, issue `raw_data`, or issue `defaults`.
- Required tests: idempotent job retry, duplicate enqueue prevention,
  hard-kill recovery, merge requeue, stale job reconciliation, explicit
  discard-branch retry, support bundle completeness after cleanup/state
  retirement, duplicate stage/merge/retry/finalize transition guards, and
  eventual compatibility-state retirement. Ledger-derived display/export state
  must be deterministic and explicitly labeled as synthesized evidence when it
  replaces missing compatibility JSON. Stage claim tests must prove duplicate
  workers cannot replace the recorded job owner, while a same/no-owner running
  step can still complete timeout/failure bookkeeping. At least one
  transaction-backed concurrency test must prove simultaneous stage, merge,
  retry, discard, and finalize attempts produce one owner or one effective
  state transition.
- Deprecation rule: no new feature should require the compatibility
  branch-state cache as the only source of truth. Add ledger-first behavior and
  keep JSON only as a compatibility cache until the documented retirement gates
  pass. Sync display and activity summaries should use ledger-derived state
  whenever the compatibility cache is absent.

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
- Responsibilities: persisted plugin models, sync parameters, execution-ledger state, density persistence, validation/drift models, job enqueueing, and merge handoff.
- Required tests: parameter validation, state helpers, ledger fallback in state
  helpers, job enqueue behavior, and ingestion metadata.

## UI/API Boundary

- Owner: `forward_netbox/views.py`, `forms.py`, `api/`, templates, tables, and filtersets.
- Responsibilities: NetBox UI forms, object views, action buttons, API actions, display of logs/statistics/issues, sync/run-level support bundle export, and read-only sync health diagnostics.
- Required tests: form cleaning, API actions, permissions, support bundle payload shape, health view payload/rendering, and visible workflow docs/screenshots when changed.
  Support/export tests should assert whether execution-ledger state came from the
  compatibility cache, the execution ledger, or no source.

## Health Diagnostics Boundary

- Owner: `forward_netbox/utilities/health.py`
- Responsibilities: read-only local-state diagnostics for sync/source health,
  query binding modes, local query-drift summaries, explicit live query-drift
  export with requested commit revision, pinned/latest commit guidance,
  explicit live source reachability export, explicit live data-file freshness
  export, diff eligibility, optional data-file map hints, latest
  validation/ingestion/execution state, timeout settings, ledger-derived
  capacity projection, fetch-contract capability/fallback reporting,
  apply-engine capability/fallback reporting, and recovery recommendation
  wiring.
- Required tests: summary shape, no live Forward calls on render, explicit live
  source/query/data-file export behavior, support bundle inclusion, and UI
  rendering through the native sync health tab.
