# Forward NetBox Architecture

`forward_netbox` synchronizes Forward inventory into NetBox through shipped or
published NQE maps. Version 2.6 has one production execution shape: validate and
stage a complete sync in one native NetBox Branching branch, then merge that
branch with the plugin's custom bulk merge.

The supported runtime is NetBox `4.6.5` with `netbox-branching` `1.1.1`.

## Runtime Flow

1. `ForwardSource` stores the Forward connection and network selection.
2. `ForwardSync` selects the snapshot policy, enabled maps, validation policy,
   and auto-merge behavior.
3. The query fetch layer resolves one snapshot, runs preflight, executes the
   selected NQE maps, and validates their NetBox-shaped rows.
4. `ForwardValidationRun` records pre-branch policy and drift evidence.
   Blocking validation stops the run before branch provisioning.
5. `ForwardSingleBranchExecutor` creates one `ForwardIngestion` and exactly one
   native Branching branch for the sync.
6. Dependency-ordered plan items are applied in that branch through the apply
   engine. `apply_engine_bulk.py` owns parity-tested batched mutations and the
   corresponding branch ObjectChanges; model-family adapters own exceptional
   rows and contracts that require row-level side effects.
7. Manual runs stop with the branch ready for review. Auto-merge runs invoke the
   custom bulk merge once for the complete branch.
8. A successful merge marks the ingestion baseline-ready and may remove the
   merged branch. An incomplete merge does neither.
9. Required post-merge overlays are recorded as pending before they are queued.
   Each overlay re-fetches its exact merged snapshot and reconciles ownership
   only if the ingestion is still the latest completed generation.
10. Drift, health, audit, ingestion issues, and support bundles report merge and
    ownership finalization independently so a staged or partially finalized run
    cannot appear converged.

NQE owns normalization and model-shaped row contracts. Python owns execution,
NetBox object resolution, branch application, merge, and explicit post-merge
ownership materialization. New code must not create a second normalization
contract beside the NQE maps.

## Execution Model

There is one branch per sync. `branch_budget.py` still orders dependency phases
and can partition work into bounded progress units, but those units all target
the same branch; they are not independent branch shards.

`ForwardSingleBranchExecutor` is the only executor. Migration `0037` removes
retired execution parameters from existing syncs, and runtime validation rejects
those keys if they are submitted again. Runtime truth comes from `ForwardSync`,
`ForwardIngestion`, its branch and jobs, validation rows, issues, model results,
device identities, and ownership reconciliation rows.

RQ delivers `JobTimeoutException` asynchronously in the worker process. Every
broad exception boundary reachable from a Forward job must therefore re-raise
that exception before fallback, row isolation, exception translation, or
best-effort logging. The job boundary may persist timeout and recovery state,
but it must then re-raise so RQ also records the deadline failure. The static
harness test in `scripts/tests/test_job_timeout_boundaries.py` enforces this
contract for job, model, and worker utility modules.

Forward jobs retain a 7,200-second minimum deadline. Merge jobs additionally
derive their RQ deadline from the branch's persisted unmerged change count and
the conservative runtime estimate used by capacity guidance. Large baselines
therefore receive their required deadline at dispatch; correctness does not
depend on an operator noticing a warning and increasing a global timeout.

## Merge Contract

The custom merge in `utilities/merge.py` and `utilities/bulk_merge.py` collapses
branch ObjectChanges, orders dependencies, applies supported batches, and uses
savepoint-isolated per-object fallback when needed. It records merge failures as
`ForwardIngestionIssue` rows and persists applied/failed counts.

The complete merge is deliberately strict:

- Any failed change raises `ForwardPartialMergeError`.
- The branch returns to `ready` for inspection and retry.
- The sync returns to `ready_to_merge`; it is not marked completed.
- The ingestion is not baseline-ready.
- The branch is not removed and post-merge ownership overlays are not accepted
  as complete.
- Retry reuses the remaining unmerged changes and the persisted counters.

The merge is not represented as transaction-atomic across every branch row.
Some rows can be present in main after an incomplete attempt. Correctness comes
from refusing completion/baseline advancement, retaining evidence, and making
retry idempotent for already-applied changes. Code that marks an incomplete
merge as merged or baseline-ready violates the release contract.

NetBox component replication stays branch-native. Device and module staging
creates or ensures module bays while the branch is active. During merge, a
module-bay create already materialized in main by its parent device is treated
as satisfied, not as a failed side-channel import. The regression suite proves
that a newly merged device has its expected module bays and no merge issue.

## Ownership Control Plane

Durable merge attestation, baseline advancement, device-identity finalization,
and ownership-pending registration share one locked finalization transaction.
Ownership materialization then runs as generation-guarded jobs with separate
evidence. Ownership rows live only in the main schema; migrations `0034` and
`0035` set `fake_on_branch = True` so branch schemas never acquire independent
claim sequences or reconciliation state.

The main-schema control plane consists of:

- `ForwardManagedDeviceTag`: tags whose assignments are controlled by claims.
- `ForwardPreservedDeviceTagAssignment`: operator assignments present before a
  managed tag was adopted, retained only while the assignment still exists.
- `ForwardDeviceTagClaim`: per-sync device/tag assertions for scope,
  backfilled, and out-of-scope tags.
- `ForwardDeviceIdentity`: exact per-sync Forward device identity to NetBox row
  mapping used by reconciliation and reviewed prune.
- `ForwardManagedVirtualContext`: virtual contexts actually created and owned
  by the plugin.
- `ForwardVirtualParentClaim`: per-sync virtual-device parent assertions.
- `ForwardOwnershipReconciliation`: pending, completed, or failed evidence for
  each sync and ownership domain.

Every claim and reconciliation is stamped with the `ForwardIngestion` primary
key as its generation and also records the snapshot ID. The ingestion ID
disambiguates repeated syncs of the same snapshot. Overlay workers acquire the
global ownership advisory lock, lock the source and sync, and verify the latest
completed ingestion before mutation. A stale worker mutates nothing and queues
catch-up against the current generation.

Materialized managed tags and virtual parents are computed from the union of
current per-sync claims. One sync cannot remove a tag or relationship still
claimed by another. Conflicting parent claims remain durable evidence, preserve
the current field value, and fail ownership finalization. Sync/source deletion
releases its claims and rematerializes the surviving union. Pre-existing
operator virtual contexts are never adopted as plugin-owned.

The upgrade migration registers recognizable managed tags but does not invent
historical per-sync claims or virtual-context ownership. Each relevant sync
must complete a current baseline and its exact overlays before ownership can be
reported complete.

## Auxiliary Read Model

`ForwardDeviceAnalysis` is a non-authoritative, per-sync cache for Forward
reachability, connectivity-degree, and CVE signals. It never creates or mutates
NetBox inventory and therefore does not belong in the Branching apply path. A
NetBox-native `DeviceAnalysisRefreshJob` fetches one resolved snapshot and
updates the cache transactionally under the same latest-ingestion generation
guard used by post-sync work. A stale refresh writes nothing and requests
catch-up. Analysis failure is visible on the job but does not alter merge or
ownership completion because the cache is an advisory read surface.

## Recovery And Evidence

Recovery uses persisted facts rather than an inferred run ledger:

1. Inspect the ingestion, branch status, jobs, merge issues, and change counts.
2. Retry a `ready` branch after resolving row failures; do not replace it with
   an unlinked branch.
3. Treat pending or failed ownership domains as incomplete even when the branch
   merge succeeded.
4. Run `python manage.py forward_ownership_audit --fail-on-inconsistent` to
   detect stale reconciliation, missing/unclaimed managed assignments, parent
   conflicts, orphan plugin-owned virtual contexts, and branch schemas pending
   migrations.
5. Use the sanitized support bundle for ingestion, drift, and ownership evidence
   without exporting raw customer rows or credentials.

## Production Boundaries

- Persisted plugin state and job entrypoints: `forward_netbox/models.py` and
  `forward_netbox/jobs.py`
- Forward API, proxy, pagination, and NQE execution:
  `forward_netbox/utilities/forward_api.py`
- Query registry and shipped maps: `forward_netbox/utilities/query_registry.py`,
  `query_binding.py`, and `forward_netbox/queries/`
- Snapshot/query fetch and row-shape handoff:
  `forward_netbox/utilities/query_fetch.py`
- Validation and configuration normalization:
  `forward_netbox/utilities/validation.py` and `model_validation.py`
- Single-branch planning and execution: `branch_budget.py`,
  `single_branch_executor.py`, `executor_base.py`, and `branch_lifecycle.py`
- NetBox model adapters and primitives: `sync.py`, `sync_runner_adapters.py`,
  `sync_inventory_module.py`, `sync_primitives.py`, and adjacent adapter modules
- Custom merge and ingestion lifecycle: `bulk_merge.py`, `merge.py`, and
  `ingestion_merge.py`
- Post-merge generation guard and ownership control plane: `post_sync.py`,
  `ownership.py`, `scope_reconciliation.py`, and `vsys_parent.py`
- Snapshot-guarded auxiliary analysis read model: `device_analysis.py`,
  `ForwardDeviceAnalysis`, and `DeviceAnalysisRefreshJob`
- Drift, health, and support evidence: `drift_report.py`, `health.py`, log export,
  views, API endpoints, and templates
- Sensitive-content guard: `sensitive_content.py` and
  `scripts/check_sensitive_content.py`

## Non-Negotiable Constraints

- Keep exactly one native Branching branch per sync.
- Keep NetBox `4.6.5` and `netbox-branching` `1.1.1` as the 2.6 runtime matrix.
- Never bypass pre-branch validation or mark an incomplete merge baseline-ready.
- Require an invoking user or persisted sync owner for every inventory-writing
  job; never substitute an arbitrary superuser for ObjectChange attribution.
- Keep post-merge ownership writes generation-guarded, serialized, and in the
  main schema.
- Preserve union/last-claim semantics across syncs and sources.
- Keep module bays in the branch-native inventory path.
- Do not add a second execution path or a second runtime state model.
- Keep normalization in NQE and object mutation in tested NetBox apply paths.
- Preserve `JobTimeoutException` identity through every worker exception
  boundary; never convert a worker deadline into an ingestion issue or ordinary
  fallback result.
- Never commit credentials, customer identifiers, network IDs, snapshot IDs,
  private communications, or raw support data.
- Create releases from a normal annotated version tag on validated `main` after
  live verification of repository controls. Keep version tags immutable and
  publish through the protected PyPI Trusted Publishing environment.
- Pair query, adapter, merge, ownership, and UI behavior changes with focused
  tests and operator/reference documentation.

The 2.6 architecture is complete only when the repository gates, independent
review, and customer-equivalent acceptance evidence in the release plan all
pass on the same final tree. Any new correctness, ownership, provenance, or
maintainability finding blocks release and is resolved in 2.6 rather than
assigned to a later version.
