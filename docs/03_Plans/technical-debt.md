# Technical Debt Tracker

This tracker records known follow-up work that should be handled through explicit plans rather than drive-by edits.

## Sync Module Boundaries

- `forward_netbox/utilities/sync.py` should eventually be split into contracts, adapter helpers, and per-model adapter modules.
- Cable adapter helpers now live in `forward_netbox/utilities/sync_cable.py`; interface, MAC, and feature-tag adapter helpers now live in `forward_netbox/utilities/sync_interface.py`; routing helper logic now lives in `forward_netbox/utilities/sync_routing.py`. The remaining `sync.py` surface is now mostly generic helper and shim glue, and any further split should be planned explicitly rather than treated as unfinished adapter extraction.
- `forward_netbox/utilities/sync_runner_adapters.py` now owns the bulk runner adapter family that used to live inline in `sync.py`.
- `forward_netbox/utilities/sync_runner_contracts.py` now owns the remaining runner contract helpers that used to live inline in `sync.py`.
- Required first step: add behavior-preserving tests around each adapter family.

## Row Reporting Boundary

- `forward_netbox/utilities/sync_reporting.py` now owns row-level issue capture, dependency failure tracking, and aggregated warning/reporting helpers.
- `forward_netbox/utilities/sync_reporting.py` also emits shard-heartbeat progress so long-running shard batches remain visibly alive in the UI and job logs.
- `forward_netbox/utilities/sync.py` now focuses more narrowly on adapters and compatibility shims.
- Required first step: keep the row-reporting contract pinned in tests before any future cleanup touches issue capture or aggregated warning behavior.

## Sync Primitive Boundary

- `forward_netbox/utilities/sync_primitives.py` now owns reusable coalesce, upsert, delete-by-coalesce, optional-model, and lookup helpers.
- `forward_netbox/utilities/sync.py` now delegates those generic primitives through thin compatibility wrappers.
- Required first step: keep the coalesce and lookup helpers pinned in tests before any future cleanup moves adapter code around them.

## Query Diagnostics Boundary

- `forward_netbox/utilities/query_diagnostics.py` now owns the IPAM and routing diagnostic synthesis that used to live inline in `query_fetch.py`.
- `forward_netbox/utilities/query_fetch.py` now focuses more narrowly on context resolution, preflight, workload assembly, and model-result capture.
- Required first step: keep the diagnostic warning shape pinned in tests before any future cleanup touches the fetcher or diagnostic query behavior.

## Intended Harness Layers

The current architecture has explicit boundaries for contracts, query fetch, planning, validation, execution, adapters, and reporting. Future refactors should keep changes inside these layers without changing public behavior:

- `contracts`: validation of row shape, model identity, and coalesce rules.
- `query fetch`: snapshot resolution, query execution, pagination, diffs, and model-result metadata.
- `planning`: workload grouping, shard sizing, and branch budget estimation.
- `execution`: branch lifecycle, shard retries, merge handoff, and resume state.
- `validation`: drift policy decisions and pre-branch gating.
- `adapters`: per-NetBox-model row apply/delete behavior.
- `reporting`: logs, statistics, model results, issues, row failures, and operator-facing progress.
- `primitives`: generic coalesce, lookup, and update-or-create behavior shared by adapters.

Required first step: add behavior-preserving tests around the current module boundaries before moving code.

## Model State Boundaries

- `forward_netbox/utilities/sync_state.py` now owns branch-run state helpers, density helpers, shard progress heartbeat state, stale-progress display, and sync activity summaries.
- `forward_netbox/utilities/sync_events.py` now owns the event flush thresholding and clear-events bridge that used to live inline in `sync.py`.
- `forward_netbox/utilities/sync_facade.py` now owns the remaining `ForwardSync` helper behavior for snapshot resolution, enabled-model access, and enqueue wrappers.
- `forward_netbox/utilities/ingestion_merge.py` now owns merge orchestration and branch signal suppression.
- `forward_netbox/utilities/sync_orchestration.py` now owns sync job orchestration and failure capture.
- `forward_netbox/utilities/validation.py` now owns the force-allow audit helper for blocked validation runs.
- `forward_netbox/utilities/model_validation.py` now owns the full sync validation contract, including scheduled-time and enabled-model checks.
- `forward_netbox/models.py` now focuses more narrowly on persisted models and thin wrappers.
- Covered: tests now pin queued, syncing, ready-to-merge, merging, failed, and completed sync transitions across sync and merge flows.
- Remaining follow-up: split persisted state from orchestration only when the lifecycle assertions stay stable.

## Direct Sync Execution Boundary

- `forward_netbox/utilities/sync_execution.py` now owns the direct query/apply/delete sync-stage loop that `ForwardSyncRunner.run()` delegates to.
- `forward_netbox/utilities/sync.py` now keeps that path as a compatibility wrapper instead of carrying the full orchestration inline.
- Required first step: keep the direct sync runner tests pinned before any further cleanup changes how query fetch, row application, or sync-mode selection are staged.

## Branch Execution Boundaries

- `forward_netbox/utilities/multi_branch_planner.py` now owns the planning boundary.
- `forward_netbox/utilities/multi_branch_executor.py` now owns branch lifecycle, resume state, overflow retry, and merge orchestration. Its main `run()` state machine is now split into smaller helpers, but the execution contract remains the same.
- `forward_netbox/utilities/multi_branch_lifecycle.py` now owns the reusable branch creation, cleanup, overflow retry, and per-shard ingestion wiring that used to live inline in the executor.
- `forward_netbox/utilities/multi_branch.py` is now a compatibility shim that re-exports the planning and execution surfaces.
- Required first step: keep adding focused tests around resume, stale branches, overflow retry, and auto-merge interaction before any further execution split.

## Local Test Repeatability

- Repeated local UI sync tests can still leave stale Branching branches behind, but branch names are now unique per ingestion so reruns do not collide on the same name.
- Remaining follow-up: only add cleanup automation if it can prove it will not mask a failed or partially merged run.

## IPAM Row Failure Visibility

- `ipam.ipaddress` already records row-level lookup failures as ingestion issues and continues later rows in the same batch.
- Keep that behavior pinned during the 0.7.0 refactor so missing-interface rows and timeout issues remain visible instead of collapsing the shard.
- Required first step: add regression coverage before any reporting or adapter cleanup that touches the row-failure path.

## Roadmap Discipline

- New roadmap work should update or complete a plan in `docs/03_Plans/active/`.
- Broad follow-up lists belong here only when they are not yet ready for an implementation plan.
