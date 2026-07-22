# Code Boundary Map

This map assigns production behavior to the modules that implement it in 2.6.

## Forward API Boundary

- Owner: `forward_netbox/utilities/forward_api.py`
- Responsibilities: authentication, NetBox proxy configuration, network and
  snapshot lookup, NQE pagination/diffs, async query polling, and error parsing.
- Required tests: proxy behavior, pagination, timeout/error conversion, snapshot
  selection, and redaction.

## Query Registry Boundary

- Owner: `forward_netbox/utilities/query_registry.py`, `query_binding.py`, and
  `forward_netbox/queries/`
- Responsibilities: shipped query discovery, imports, query IDs and repository
  paths, commit bindings, model registration, and query drift classification.
- Required tests: registry loading, import flattening, binding modes, drift
  classification, query contracts, and sensitive-content checks.

## Query Fetch Boundary

- Owner: `forward_netbox/utilities/query_fetch.py` and its focused fetch helpers
- Responsibilities: exact snapshot context, full/diff execution,
  bounded fetch concurrency, completion-order telemetry, parameterless diff
  enforcement, fallback, row-shape handoff, and model results.
- Required tests: one execution per map, full/diff parity, deterministic
  fallback, schema validation, and persisted fetch evidence.

## Workload Normalization Boundary

- Owner: `forward_netbox/utilities/workload_normalization.py`
- Responsibilities: use authoritative full device/interface workloads to
  exclude unrepresentable cable and OSPF-interface dependencies before branch
  planning, preserve exact existing cables, and select a deterministic
  one-cable-per-interface candidate graph.
- Required tests: scope completeness, missing parent coverage, existing cable
  preservation, deterministic candidate conflicts, routing interface aliases,
  and no filtering from partial/diff parent evidence.

## Durable Workload State Boundary

- Owner: `forward_netbox/utilities/workload_state.py`
- Persisted state: `ForwardWorkloadState` in `models.py`
- Responsibilities: canonical row identity, compressed checksummed full-query
  state, local upsert/delete derivation, successful-generation promotion,
  delete tombstones, enrichment-only model policy, and cross-sync/reference
  delete protection.
- Required tests: deterministic identity, payload corruption, parameter and
  contract reset, explicit and derived tombstones, peer/unseeded-peer delete
  protection, no-op promotion, successful merge promotion, and failed/staged
  non-promotion.

## Validation Boundary

- Owner: `forward_netbox/utilities/validation.py` and `model_validation.py`
- Responsibilities: source/sync configuration, migration-time retired-state removal,
  validation-run persistence, drift policy, and
  blocked-before-branch decisions.
- Required tests: invalid configuration, retired-key rejection, migration
  normalization, policy
  blocking, standalone preview/validation, and UI/API visibility.

## Branch Planning Boundary

- Owner: `forward_netbox/utilities/branch_budget.py`
- Responsibilities: dependency ordering, bounded progress units, change-density
  estimates, and fetch-contract reporting for a single target branch.
- Required tests: deterministic ordering/partitioning, delete-before-upsert
  requirements, budget telemetry, and fallback reporting.
- Constraint: a plan item is not a branch shard; every item in one sync targets
  the same branch.

## Branch Execution Boundary

- Owner: `forward_netbox/utilities/single_branch_executor.py`
- Staging primitive: `forward_netbox/utilities/branch_lifecycle.py`
- Responsibilities: provision exactly one branch and one ingestion, stage all
  dependency phases, preserve manual review, and hand auto-merge to the custom
  merge path.
- Required tests: validated workload before provisioning, one-branch identity, phased
  staging, branch-native bulk ObjectChanges, module-bay creation, manual review,
  and auto-merge.
- Construction and ingestion bookkeeping:
  `forward_netbox/utilities/executor_base.py`.

## Merge Boundary

- Owner: `forward_netbox/utilities/merge.py`, `bulk_merge.py`, and
  `ingestion_merge.py`
- Responsibilities: collapse and dependency-order ObjectChanges, batch apply,
  per-object savepoint fallback, issue capture, progress, strict partial-merge
  failure, retry counters, branch lifecycle, baseline advancement, and cleanup.
- Required tests: create/update/delete order, idempotent retry, partial failures
  retain a ready branch, no baseline on incomplete merge, branch cleanup only on
  success, and parent-created module bays satisfy duplicate side-effect changes.

## NetBox Adapter Boundary

- Owner: `forward_netbox/utilities/apply_engine.py`, `apply_engine_bulk.py`,
  `bulk_delete.py`, `sync.py`, `sync_runner_adapters.py`, `sync_primitives.py`,
  and model-family modules such as `sync_inventory_module.py`
- Responsibilities: apply-engine selection, batched mutation and matching
  branch ObjectChange/ChangeDiff evidence, Collector-safe batch deletion,
  row-level lookups and coalesce identity, dependency failures, model-specific
  fields, and exceptional module/module-bay or destructive side effects.
- Required tests: each model contract, ambiguity handling, field bounds,
  dependency skips, delete safety, bulk/adapter parity, and branch visibility.

## Ownership Boundary

- Owner: `forward_netbox/utilities/ownership.py` and `post_sync.py`
- Domain producers: `scope_reconciliation.py`, `vsys_parent.py`, and post-sync
  jobs in `forward_netbox/jobs.py`
- Persisted state: `ForwardManagedDeviceTag`, `ForwardDeviceTagClaim`,
  `ForwardManagedVirtualContext`, `ForwardVirtualParentClaim`, and
  `ForwardOwnershipReconciliation` in `models.py`
- Responsibilities: persist pending work before queueing, guard exact ingestion
  generations, serialize cross-source writes, replace per-sync claims,
  materialize the union of current claims, preserve conflicts as evidence, and
  release ownership on sync/source deletion.
- Required tests: non-branchable migrations, same-snapshot generation ordering,
  stale-worker no-op/catch-up, concurrent last-claim release, shared claims,
  parent conflict failure, pre-existing VDC preservation, deletion cleanup, and
  audit failure modes.

## Plugin State Boundary

- Owner: `forward_netbox/models.py`
- Responsibilities: sources, syncs, ingestions, validation, model results,
  issues, ownership rows, analysis rows, and thin job/merge entrypoints.
- Required tests: lifecycle transitions, deletion guards, model constraints,
  permissions, serialization, and migrations.

## Job Boundary

- Owner: `forward_netbox/jobs.py`, `utilities/job_queue.py`, and
  `sync_orchestration.py`
- Responsibilities: sync/merge job entrypoints, state/failure capture,
  transaction-safe Redis dispatch, post-merge pending registration and overlay
  queueing, stale-overlay catch-up, and scheduled work.
- Required tests: duplicate queue guards, merge error states, pending-before-
  enqueue ordering, stale overlay replacement, failed reconciliation evidence,
  and no overlay completion after incomplete merge.

## UI/API Boundary

- Owner: `forward_netbox/views.py`, `forms.py`, `api/`, templates, tables, and
  filtersets
- Responsibilities: native NetBox workflows, permissions, branch/ingestion
  actions, drift and health views, issue visibility, and sanitized support
  bundles.
- Required tests: forms, API actions, permissions, support payloads, health and
  drift rendering, ownership-incomplete presentation, and Playwright coverage
  for changed workflows.

## Health Diagnostics Boundary

- Owner: `forward_netbox/utilities/health.py`, `drift_report.py`, supporting
  summary modules, log export, and `forward_ownership_audit`
- Responsibilities: read-only runtime/configuration evidence, query bindings,
  latest validation and ingestion state, merge counts/issues, ownership
  finalization, branch migration readiness, and actionable recovery guidance.
- Required tests: no implicit live Forward calls on render, aggregate-only
  ownership evidence, support-bundle redaction, audit exit status, and drift
  refusal while merge or ownership work is incomplete.
