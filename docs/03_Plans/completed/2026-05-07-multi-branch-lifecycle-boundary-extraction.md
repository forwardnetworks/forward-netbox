# Multi-Branch Lifecycle Boundary Extraction

## Goal

Move branch lifecycle, resume state, overflow retry, and branch cleanup helpers out of `forward_netbox/utilities/multi_branch_executor.py` into a dedicated helper boundary while preserving the current multi-branch behavior.

## Constraints

- Preserve the existing branch plan and sync-state behavior.
- Keep branch creation, merge handoff, and overflow retry NetBox-native and Branching-native.
- Do not change auto-merge semantics or the resume contract.
- Keep customer-specific data out of examples and tests.

## Touched Surfaces

- `forward_netbox/utilities/multi_branch_executor.py`
- `forward_netbox/utilities/multi_branch_lifecycle.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_synthetic_scenarios.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach

Move the branch lifecycle helpers into `forward_netbox/utilities/multi_branch_lifecycle.py` and keep `ForwardMultiBranchExecutor` as the coordinator. The executor still owns orchestration, but the helper module owns branch creation, ingestion creation, resume state updates, overflow retry handling, and plan reindexing.

## Validation

- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback

Restore the helper methods inside `forward_netbox/utilities/multi_branch_executor.py` and remove the lifecycle module if the extraction changes behavior.

## Decision Log

- Rejected changing branch retry behavior during the split because the 0.7.0 work is about boundary cleanup, not semantics.
- Rejected moving branch planning again because the planner boundary already exists and is not the next hotspot.
