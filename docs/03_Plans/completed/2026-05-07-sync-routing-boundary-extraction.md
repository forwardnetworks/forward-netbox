# Sync Routing Boundary Extraction

## Goal

Extract the routing and peering adapter entrypoints from `forward_netbox/utilities/sync.py` into a dedicated module while preserving the existing runner-level API and behavior.

## Constraints

- Keep the public runner methods intact for existing tests and UI flows.
- Preserve BGP, OSPF, and peering-session semantics.
- Do not change NetBox-native branching behavior.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_routing.py`
- `forward_netbox/tests/test_sync.py`
- `docs/03_Plans/technical-debt.md`
- `ARCHITECTURE.md`

## Approach

Move routing and peering apply/delete entrypoints into `forward_netbox/utilities/sync_routing.py` and keep `ForwardSyncRunner` methods as delegation shims while preserving the deeper routing helpers in `sync.py` for now.

## Validation

- `invoke lint`
- `invoke test`
- `invoke ci`

## Rollback

Restore the routing and peering adapter methods inside `forward_netbox/utilities/sync.py` and remove `forward_netbox/utilities/sync_routing.py`.

## Decision Log

- Chose the routing/peering entrypoints next because they are a coherent adapter family with existing coverage and they reduce the main sync module without disturbing the lower-level routing helpers yet.
- Rejected moving the deeper routing resolution helpers in the same pass because that would widen the change set beyond the boundary needed for this tranche.
