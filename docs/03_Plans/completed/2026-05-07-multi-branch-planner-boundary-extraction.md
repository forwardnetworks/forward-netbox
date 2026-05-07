# Multi-Branch Planner Boundary Extraction

## Goal

Extract the planning boundary from `forward_netbox/utilities/multi_branch.py` into a dedicated module while preserving the existing executor behavior and public API.

## Constraints

- Keep the public executor API intact for existing tests and UI flows.
- Preserve query fetch, preflight, and model-result behavior.
- Do not change NetBox-native branching behavior.

## Touched Surfaces

- `forward_netbox/utilities/multi_branch.py`
- `forward_netbox/utilities/multi_branch_planner.py`
- `forward_netbox/tests/test_sync.py`
- `docs/03_Plans/technical-debt.md`
- `ARCHITECTURE.md`

## Approach

Move the planning class out of `forward_netbox/utilities/multi_branch.py` into `forward_netbox/utilities/multi_branch_planner.py` and keep the executor as the owner of runtime state, overflow retries, and branch lifecycle.

## Validation

- `invoke lint`
- `invoke test`
- `invoke ci`

## Rollback

Restore the planning class inside `forward_netbox/utilities/multi_branch.py` and remove `forward_netbox/utilities/multi_branch_planner.py`.

## Decision Log

- Chose the planner first because it is the cleanest low-risk boundary inside the overgrown multi-branch module and already has direct test coverage.
- Rejected a broader executor split in the same pass because the runtime state machine is still the larger, riskier boundary and deserves its own tranche.
