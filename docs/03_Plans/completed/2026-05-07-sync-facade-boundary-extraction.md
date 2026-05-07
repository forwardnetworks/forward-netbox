# Sync Facade Boundary Extraction

## Goal
Move the remaining `ForwardSync` helper behavior out of `forward_netbox/models.py` and into a dedicated utility boundary while keeping the public model methods as thin wrappers.

## Constraints
- Keep existing model methods and their call sites stable.
- Preserve snapshot resolution, query parameter, display, and enqueue behavior exactly.
- Do not alter branch orchestration, validation policy, or row application semantics in this tranche.
- Keep the NetBox-native UI/API workflow unchanged.

## Touched Surfaces
- `forward_netbox/models.py`
- `forward_netbox/utilities/sync_facade.py`
- `forward_netbox/tests/test_models.py`
- `forward_netbox/tests/test_sync_state.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach
1. Introduce a small helper module for `ForwardSync` facade behavior.
2. Move the remaining helper logic out of the model and keep wrappers in place.
3. Add regression tests for the helper boundary and existing wrappers.
4. Update architecture and debt notes if the model surface is now thinner.

## Validation
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback
- Restore the original `ForwardSync` helper bodies in `models.py`.
- Remove `utilities/sync_facade.py`.

## Decision Log
- Chosen: keep the model methods stable and move only the implementation details.
- Chosen: keep enqueue and snapshot resolution logic close together because they are all sync-facing facade behavior.
- Rejected: leaving the remaining helpers in `models.py` because the model class is still doing too much orchestration work.

## Validation Result
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

