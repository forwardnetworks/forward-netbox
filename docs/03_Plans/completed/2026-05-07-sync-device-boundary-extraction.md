# Sync Device Boundary Extraction

## Goal

Extract the device and virtual chassis adapters from `forward_netbox/utilities/sync.py` into a dedicated module while preserving the existing runner-level API and behavior.

## Constraints

- Keep the public runner methods intact for existing tests and UI flows.
- Preserve device, virtual chassis, and delete semantics.
- Do not change NetBox-native branching behavior.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_device.py`
- `forward_netbox/tests/test_sync.py`
- `docs/03_Plans/technical-debt.md`
- `ARCHITECTURE.md`

## Approach

Move device and virtual chassis apply/delete logic into `forward_netbox/utilities/sync_device.py` and keep `ForwardSyncRunner` methods as delegation shims.

## Validation

- `invoke lint`
- `invoke test`
- `invoke ci`

## Rollback

Restore the device and virtual chassis adapter methods inside `forward_netbox/utilities/sync.py` and remove `forward_netbox/utilities/sync_device.py`.

## Decision Log

- Chose the device family because it is a core adapter surface and the extracted helpers still preserve the same persistence and dependency behavior.
- Rejected a broad rewrite of the remaining model adapters in the same pass because the goal is incremental boundary repair, not churn.
