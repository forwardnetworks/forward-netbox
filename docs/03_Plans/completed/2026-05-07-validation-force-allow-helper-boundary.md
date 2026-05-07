# Validation Force-Allow Helper Boundary

## Goal
Move the audited validation force-allow logic out of `forward_netbox/models.py` and into `forward_netbox/utilities/validation.py` while keeping the UI/API behavior and audit trail identical.

## Constraints
- Keep `ForwardValidationRun.force_allow()` as the public model entrypoint.
- Preserve the exact force-allow semantics, including audit fields and blocked-run preconditions.
- Do not change the validation override contract or policy reuse logic.
- Keep the NetBox-native UI/API flow unchanged.

## Touched Surfaces
- `forward_netbox/models.py`
- `forward_netbox/utilities/validation.py`
- `forward_netbox/tests/test_models.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach
1. Extract the force-allow implementation into `utilities/validation.py`.
2. Keep the model method as a thin delegating wrapper.
3. Add/adjust tests to pin the helper boundary.
4. Update the architecture and debt notes if the model surface is now thinner.

## Validation
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback
- Restore the original `ForwardValidationRun.force_allow()` body in `models.py`.
- Remove the helper from `utilities/validation.py`.

## Decision Log
- Chosen: keep `ForwardValidationRun.force_allow()` as the public entrypoint so UI/API callers do not change.
- Chosen: move only the audited override implementation into `utilities/validation.py`, leaving the model method as a thin wrapper.
- Rejected: moving override handling into the API or views because the audit state belongs with the validation record itself.

## Validation Result
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`
