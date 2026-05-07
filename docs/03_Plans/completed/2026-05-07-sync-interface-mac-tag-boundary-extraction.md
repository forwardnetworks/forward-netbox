# Sync Interface, MAC, and Feature-Tag Boundary Extraction

## Goal

Extract the interface, MAC address, and feature-tag adapters from `forward_netbox/utilities/sync.py` into a dedicated module while preserving the existing runner-level API and behavior.

## Constraints

- Keep the public runner methods intact for existing tests and UI flows.
- Preserve interface LAG, MAC assignment, and feature-tag semantics.
- Do not change NetBox-native branching behavior.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_interface.py`
- `forward_netbox/tests/test_sync.py`
- `docs/03_Plans/technical-debt.md`
- `ARCHITECTURE.md`

## Approach

Move interface, MAC address, and feature-tag apply/delete logic into `forward_netbox/utilities/sync_interface.py` and keep `ForwardSyncRunner` methods as delegation shims.

## Validation

- `invoke lint`
- `invoke test`
- `invoke ci`

## Rollback

Restore the interface, MAC address, and feature-tag adapter methods inside `forward_netbox/utilities/sync.py` and remove `forward_netbox/utilities/sync_interface.py`.

## Decision Log

- Chose this adapter family because it is the next coherent chunk after the cable, core, device, inventory, IPAM, and routing extractions.
- Rejected folding these methods into the earlier cable tranche because the goal is to keep each boundary move small and easy to verify.
