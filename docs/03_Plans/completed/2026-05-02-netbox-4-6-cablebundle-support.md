# Plan: NetBox 4.6 Cable Bundle Support

## Goal

Add `dcim.cablebundle` support on the `netbox-4.6-beta2` branch so bundle rows can be synced natively and inferred cable rows can optionally bind to bundles.

## Constraints

- Keep behavior compatible with pre-4.6 environments where `dcim.CableBundle` and `Cable.bundle` do not exist.
- Do not force bundle creation by default; make bundle ingestion opt-in.
- Keep existing cable import behavior unchanged when bundle fields are absent.

## Touched Surfaces

- `forward_netbox/choices.py`
- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/utilities/query_registry.py`
- `forward_netbox/queries/forward_inferred_interface_cables.nqe`
- `forward_netbox/queries/forward_cable_bundles.nqe`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`

## Approach

1. Add `dcim.cablebundle` to supported models and sync contracts.
2. Add an optional built-in map (`Forward Cable Bundles`) that derives bundle names from inferred links.
3. Extend inferred cable rows with `bundle_name`.
4. Implement `dcim.cablebundle` apply/delete adapters.
5. Update cable apply logic to attach bundle only when the NetBox runtime supports `Cable.bundle`.
6. Add/adjust tests for query seeding and bundle-aware cable behavior.

## Rollback

- Revert this branch commit to remove bundle support entirely.
- Keep optional map disabled by default to avoid operational impact before explicit enablement.

## Validation

- `pre-commit run --all-files`
- `invoke test`
- branch CI (`netbox-4.6-beta2`) after push

## Decision Log

- Bundle ingestion is optional by default to preserve existing sync behavior.
- Runtime guards for missing `CableBundle`/`Cable.bundle` keep this branch safe in mixed local test environments.
