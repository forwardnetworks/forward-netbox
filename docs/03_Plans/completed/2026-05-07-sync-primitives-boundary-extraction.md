# Sync Primitives Boundary Extraction

## Goal
Move the reusable coalesce, upsert, delete-by-coalesce, optional-model, and lookup primitives out of `forward_netbox/utilities/sync.py` into a dedicated helper boundary while preserving the current runner API and row behavior.

## Constraints
- Keep the public `ForwardSyncRunner` methods intact as shims.
- Preserve lookup, conflict, and unique-coalesce behavior exactly.
- Do not change adapter semantics or row failure handling.
- Keep the refactor NetBox-native and Branching-native.

## Touched Surfaces
- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_primitives.py`
- `forward_netbox/tests/`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Validation
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Decision Log
- This is the remaining reusable helper hotspot after the adapter and row-reporting splits.
- A separate boundary for primitives makes later adapter and reporting cleanup simpler without changing the import workflow.
