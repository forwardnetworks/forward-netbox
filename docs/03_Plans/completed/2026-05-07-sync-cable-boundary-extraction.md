# Sync Cable Boundary Extraction

## Goal

Extract the cable adapter family from `forward_netbox/utilities/sync.py` into a dedicated module while preserving the existing runner-level API and behavior.

## Constraints

- Keep the public runner methods intact for existing tests and UI flows.
- Preserve cable conflict, skip, and delete semantics.
- Do not change NetBox-native branching behavior.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_cable.py`
- `forward_netbox/tests/test_synthetic_scenarios.py`
- `docs/03_Plans/technical-debt.md`
- `ARCHITECTURE.md`

## Approach

Move cable lookup, create, and delete logic into `forward_netbox/utilities/sync_cable.py` and keep the `ForwardSyncRunner` methods as delegation shims. This starts the adapter boundary split without forcing a broader `sync.py` rewrite.

## Validation

- `invoke lint`
- `invoke test`
- `invoke ci`

## Rollback

Restore the cable adapter methods inside `forward_netbox/utilities/sync.py` and remove `forward_netbox/utilities/sync_cable.py`.

## Decision Log

- Chose a dedicated cable adapter module because the cable family already has strong behavior coverage and is a natural first extraction from the overgrown sync module.
- Rejected a broader `sync.py` split in the same pass because that would widen the blast radius beyond the verified cable behavior.
