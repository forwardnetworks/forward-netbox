# Sync Core Model Boundary Extraction

## Goal

Extract the core identity model adapters from `forward_netbox/utilities/sync.py` into a dedicated module while preserving the existing runner-level API and behavior.

## Constraints

- Keep the public runner methods intact for existing tests and UI flows.
- Preserve coalesce behavior and delete semantics for core identity models.
- Do not change NetBox-native branching behavior.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_core_models.py`
- `forward_netbox/tests/test_sync.py`
- `docs/03_Plans/technical-debt.md`
- `ARCHITECTURE.md`

## Approach

Move site, manufacturer, platform, device-role, and device-type apply/delete logic into `forward_netbox/utilities/sync_core_models.py` and keep `ForwardSyncRunner` methods as delegation shims.

## Validation

- `invoke lint`
- `invoke test`
- `invoke ci`

## Rollback

Restore the core identity model adapters inside `forward_netbox/utilities/sync.py` and remove `forward_netbox/utilities/sync_core_models.py`.

## Decision Log

- Chose the core identity models because they are stable, low-risk adapter surfaces and a natural next step in shrinking the overgrown sync module.
- Rejected a broader sync rewrite in the same pass because the goal is to keep each boundary move small and verifiable.
