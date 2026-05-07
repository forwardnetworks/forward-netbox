# Sync IPAM Boundary Extraction

## Goal

Extract the IPAM adapters from `forward_netbox/utilities/sync.py` into a dedicated module while preserving the existing runner-level API and behavior.

## Constraints

- Keep the public runner methods intact for existing tests and UI flows.
- Preserve VLAN, VRF, prefix, and IP address semantics.
- Do not change NetBox-native branching behavior.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_ipam.py`
- `forward_netbox/tests/test_sync.py`
- `docs/03_Plans/technical-debt.md`
- `ARCHITECTURE.md`

## Approach

Move VLAN, VRF, prefix, and IP address apply/delete logic into `forward_netbox/utilities/sync_ipam.py` and keep `ForwardSyncRunner` methods as delegation shims.

## Validation

- `invoke lint`
- `invoke test`
- `invoke ci`

## Rollback

Restore the IPAM adapter methods inside `forward_netbox/utilities/sync.py` and remove `forward_netbox/utilities/sync_ipam.py`.

## Decision Log

- Chose IPAM because it remains one of the largest remaining adapter groups and already has focused regression coverage around host-IP reuse and prefix behavior.
- Rejected splitting IPAM into smaller pieces in the same pass because the goal is to keep each boundary extraction simple and verifiable.
