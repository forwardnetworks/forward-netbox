# Routing Helper Boundary Extraction

## Goal

Extract the remaining routing helper logic from `forward_netbox/utilities/sync.py` into `forward_netbox/utilities/sync_routing.py` while preserving the existing runner-level API and behavior.

## Constraints

- Keep the public runner methods intact for existing tests and UI flows.
- Preserve BGP peer, BGP address-family, OSPF, and peering relationship semantics.
- Do not change NetBox-native branching behavior.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_routing.py`
- `forward_netbox/tests/test_sync.py`
- `docs/03_Plans/technical-debt.md`
- `ARCHITECTURE.md`

## Approach

Move the remaining routing helper functions into `forward_netbox/utilities/sync_routing.py` and keep `ForwardSyncRunner` methods as delegation shims.

## Validation

- `invoke lint`
- `invoke test`
- `invoke ci`

## Rollback

Restore the routing helper methods inside `forward_netbox/utilities/sync.py` and remove the helper functions from `forward_netbox/utilities/sync_routing.py`.

## Decision Log

- Chose routing helpers next because the adapter entrypoints were already extracted and the remaining logic was the last significant chunk still living in `sync.py`.
- Rejected a broader branch-execution refactor in the same pass because the routing helper boundary was smaller, more deterministic, and easier to verify against the existing routing tests.
