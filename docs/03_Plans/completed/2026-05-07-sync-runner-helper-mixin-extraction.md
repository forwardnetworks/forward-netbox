# Sync Runner Helper Mixin Extraction

## Goal

Thin `forward_netbox/utilities/sync.py` by moving the remaining `ForwardSyncRunner` adapter/helper surface into a dedicated helper module while preserving existing row application and deletion behavior.

## Constraints

- Preserve the current `ForwardSyncRunner` public behavior.
- Keep adapter semantics unchanged.
- Do not alter model selection, coalesce, delete-by-coalesce, or routing/IPAM logic.
- Keep the existing `ForwardSyncRunner` import path valid for callers.
- Avoid introducing new dependencies or new user-facing workflow changes.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_runner_adapters.py`
- `forward_netbox/tests/test_sync.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach

Move the large adapter/helper block out of `sync.py` into `sync_runner_adapters.py` and have `ForwardSyncRunner` inherit from the helper mixin. Keep the core orchestration and identity logic in `sync.py` so the import boundary and execution contract remain stable.

## Validation

- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback

Revert the helper extraction and restore the adapter methods to `sync.py` if the runner import chain or adapter dispatch changes behavior.

## Decision Log

- Rejected: moving the runner orchestration into `models.py` | that would blur the current execution boundary.
- Rejected: changing adapter logic while extracting the wrappers | this tranche is structural only.
- Rejected: splitting the work into many tiny helper files | a single helper boundary is enough for this pass.
