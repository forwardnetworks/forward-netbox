# Sync Validation Boundary Extension

## Goal
Move the remaining `ForwardSync.clean()` runtime validation checks out of `forward_netbox/models.py` and into `forward_netbox/utilities/model_validation.py` while preserving the existing validation contract and messages.

## Constraints
- Keep `ForwardSync.clean()` as the public entrypoint.
- Preserve the current validation messages and normalization behavior.
- Do not change save behavior, enqueue behavior, or branch orchestration.
- Keep the NetBox-native UI/API workflow unchanged.

## Touched Surfaces
- `forward_netbox/models.py`
- `forward_netbox/utilities/model_validation.py`
- `forward_netbox/tests/test_models.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach
1. Extend the sync validation helper so it owns the scheduled-time and enabled-model checks.
2. Keep `ForwardSync.clean()` as a thin wrapper.
3. Add regression tests for the helper behavior.
4. Update architecture and debt notes if the model surface is thinner.

## Validation
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback
- Restore the original scheduled-time and enabled-model checks in `models.py`.
- Remove the helper changes from `utilities/model_validation.py`.

## Decision Log
- Chosen: keep the validation contract in the existing validation utility instead of adding another model helper file.
- Chosen: fold the scheduled-time and enabled-model checks into the same helper so `ForwardSync.clean()` becomes a thin wrapper.
- Rejected: leaving runtime validation inside `models.py` because it is still a contract boundary that belongs with the other validation helpers.

## Validation Result
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`
