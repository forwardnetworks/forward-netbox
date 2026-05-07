# Multi-Branch Executor Boundary Extraction

## Goal
Move the branch execution state machine out of `forward_netbox/utilities/multi_branch.py` into a dedicated module while preserving the current import surface and behavior.

## Constraints
- Keep existing imports working through a compatibility shim.
- Do not change sync semantics, branch creation behavior, or resume state handling.
- Keep the planner/executor split aligned with the existing architecture notes.

## Touched Surfaces
- `forward_netbox/utilities/multi_branch.py`
- `forward_netbox/utilities/multi_branch_executor.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach
- Move `BranchBudgetExceeded` and `ForwardMultiBranchExecutor` into `multi_branch_executor.py`.
- Leave `multi_branch.py` as a thin re-export layer for compatibility.
- Update the architecture and debt notes so the new execution boundary is explicit.

## Validation
- `python -m compileall forward_netbox/utilities/multi_branch.py forward_netbox/utilities/multi_branch_executor.py forward_netbox/utilities/multi_branch_planner.py`

## Rollback
- Restore `forward_netbox/utilities/multi_branch.py` from version control and delete `multi_branch_executor.py`.
- No database or external state changes are introduced by this refactor.

## Decision Log
- The executor split is the next clean boundary after the planner extraction.
- A compatibility shim keeps the public import path stable while reducing module size.
- The extracted module keeps branch lifecycle and retry behavior in one place for the next tranche of cleanup.
