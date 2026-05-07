# Technical Debt Tracker

This tracker records known follow-up work that should be handled through explicit plans rather than drive-by edits.

## Sync Module Boundaries

- `forward_netbox/utilities/sync.py` should eventually be split into contracts, adapter helpers, and per-model adapter modules.
- Cable adapter helpers now live in `forward_netbox/utilities/sync_cable.py`; interface, MAC, and feature-tag adapter helpers now live in `forward_netbox/utilities/sync_interface.py`; the remaining `sync.py` split still needs behavior-preserving extraction.
- Required first step: add behavior-preserving tests around each adapter family.

## Intended Harness Layers

The current architecture has explicit boundaries for contracts, query fetch, planning, validation, execution, adapters, and reporting. Future refactors should keep changes inside these layers without changing public behavior:

- `contracts`: validation of row shape, model identity, and coalesce rules.
- `query fetch`: snapshot resolution, query execution, pagination, diffs, and model-result metadata.
- `planning`: workload grouping, shard sizing, and branch budget estimation.
- `execution`: branch lifecycle, shard retries, merge handoff, and resume state.
- `validation`: drift policy decisions and pre-branch gating.
- `adapters`: per-NetBox-model row apply/delete behavior.
- `reporting`: logs, statistics, model results, issues, and operator-facing progress.

Required first step: add behavior-preserving tests around the current module boundaries before moving code.

## Model State Boundaries

- `forward_netbox/models.py` combines persisted models, state helpers, and job orchestration.
- Covered: tests now pin queued, syncing, ready-to-merge, merging, failed, and completed sync transitions across sync and merge flows.
- Remaining follow-up: split persisted state from orchestration only when the lifecycle assertions stay stable.

## Branch Execution Boundaries

- `forward_netbox/utilities/multi_branch.py` combines context build, preflight, planning, branch lifecycle, resume state, and merge orchestration.
- Required first step: keep adding focused tests around resume, stale branches, overflow retry, and auto-merge interaction.

## Local Test Repeatability

- Repeated local UI sync tests can still leave stale Branching branches behind, but branch names are now unique per ingestion so reruns do not collide on the same name.
- Remaining follow-up: only add cleanup automation if it can prove it will not mask a failed or partially merged run.

## Roadmap Discipline

- New roadmap work should update or complete a plan in `docs/03_Plans/active/`.
- Broad follow-up lists belong here only when they are not yet ready for an implementation plan.
