# Multi-Branch Execution State Split

## Goal

Reduce the remaining branch-execution complexity in `forward_netbox/utilities/multi_branch_executor.py` by splitting `run()` into smaller helpers without changing branch planning, overflow retry, resume, or merge behavior.

## Constraints

- Preserve the current multi-branch UI/API contract.
- Keep native Branching behavior intact.
- Do not change the branch budget semantics or the overflow split behavior.
- Do not add a new user-facing toggle or workflow.
- Keep the current resume and awaiting-merge behavior stable.

## Touched Surfaces

- `forward_netbox/utilities/multi_branch_executor.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_synthetic_scenarios.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach

Split the executor state machine into smaller helpers for run initialization, execution-state persistence, overflow handling, and item iteration. Keep the public `ForwardMultiBranchExecutor.run()` entrypoint stable so the existing sync workflow does not change.

## Validation

- `invoke lint`
- `invoke test`
- `invoke docs`

## Rollback

Revert the helper extraction and restore the previous `run()` body if the refactor changes branch state transitions, resume behavior, or overflow split handling.

## Decision Log

- Rejected: moving more logic into `ForwardSync` model methods | that would reverse the direction of the 0.7.0 boundary cleanup.
- Rejected: changing branch retries or overflow thresholds as part of the refactor | this tranche is structural only.
- Rejected: introducing a new planning/execution toggle | the contract should remain native and defaulted.
