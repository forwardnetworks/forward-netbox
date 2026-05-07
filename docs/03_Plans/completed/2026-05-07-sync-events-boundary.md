# Sync Events Boundary

## Goal
Move the `EventsClearer` utility out of `forward_netbox/utilities/sync.py` into a dedicated helper module so the main sync runner only coordinates imports and execution.

## Constraints
- Keep `EventsClearer` behavior identical.
- Preserve the existing event flush timing and clear-events signal behavior.
- Do not change row application or sync execution semantics.
- Keep the NetBox-native workflow unchanged.

## Touched Surfaces
- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_events.py`
- `forward_netbox/tests/test_sync.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach
1. Move the utility class into a dedicated helper module.
2. Keep `sync.py` as a thin importer/consumer of the helper.
3. Update the existing tests to target the new helper path.
4. Update architecture and debt notes if the boundary is thinner.

## Validation
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback
- Restore `EventsClearer` in `sync.py`.
- Remove `sync_events.py`.

## Decision Log
- Chosen: extract the already-tested utility instead of touching a more behavior-rich path.
- Chosen: keep the flush-on-threshold semantics intact because that guards event queue hygiene.
- Rejected: broader `sync.py` rewrites because the runner already has a stable, test-pinned shape.

## Validation Result
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`
