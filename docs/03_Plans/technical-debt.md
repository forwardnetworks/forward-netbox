# Technical Debt Tracker

This tracker records known follow-up work that should be handled through explicit plans rather than drive-by edits.

## Sync Module Boundaries

- `forward_netbox/utilities/sync.py` should eventually be split into contracts, adapter helpers, and per-model adapter modules.
- Required first step: add behavior-preserving tests around each adapter family.

## Model State Boundaries

- `forward_netbox/models.py` combines persisted models, state helpers, and job orchestration.
- Required first step: document and test state transitions for queued, syncing, ready-to-merge, merging, failed, and completed syncs.

## Branch Execution Boundaries

- `forward_netbox/utilities/multi_branch.py` combines context build, preflight, planning, branch lifecycle, resume state, and merge orchestration.
- Required first step: keep adding focused tests around resume, stale branches, overflow retry, and auto-merge interaction.

## Local Test Repeatability

- Repeated local UI sync tests can collide with stale Branching branch names.
- Required first step: design an operator-safe cleanup or branch naming strategy that does not hide production failures.
