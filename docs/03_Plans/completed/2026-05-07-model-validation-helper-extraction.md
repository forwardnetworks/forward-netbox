# Model Validation Helper Extraction

## Goal
Move `ForwardSource.clean`, `ForwardNQEMap.clean`, and `ForwardSync.clean` validation rules into a dedicated utility module while preserving the model APIs and validation semantics.

## Constraints
- Keep the model `clean()` methods as public entrypoints.
- Preserve all current validation messages and constraints.
- Do not change save behavior, sync behavior, or branch orchestration in this tranche.
- Keep the change compatible with the current NetBox-native workflow.

## Touched Surfaces
- `forward_netbox/models.py`
- `forward_netbox/utilities/model_validation.py`
- `forward_netbox/tests/test_models.py`
- `forward_netbox/tests/test_model_validation.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach
- Extract validation rule checks into `model_validation.py`.
- Leave the model `clean()` methods as thin delegators.
- Keep the validation messages and normalization behavior identical.
- Add focused regression tests for the helper module and the model wrappers.
- Update architecture and debt notes so the contracts boundary is explicit.

## Validation
- `invoke harness-check`
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback
- Restore `forward_netbox/models.py` from version control and remove `model_validation.py`.
- No data migration or persistent state change is expected.

## Decision Log
- Validation logic is still the most concentrated remaining contract surface in `models.py`.
- Keeping the wrappers avoids a public API break while making validation rules easier to test in isolation.
- This tranche is limited to validation and normalization only, not sync or merge orchestration.
