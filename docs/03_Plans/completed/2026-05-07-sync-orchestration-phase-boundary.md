# Sync Orchestration Phase Boundary

## Goal
Move the start/failure/finalization phases of `run_forward_sync()` into helper functions so the orchestration module keeps the flow while the phase logic becomes easier to test and reason about.

## Constraints
- Keep `run_forward_sync()` as the public entrypoint.
- Preserve the existing status transitions, failure capture, and job-data persistence.
- Do not change branch execution behavior or row application behavior.
- Keep the NetBox-native workflow unchanged.

## Touched Surfaces
- `forward_netbox/utilities/sync_orchestration.py`
- `forward_netbox/tests/test_sync_orchestration.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Validation
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Approach
Extract the start, failure, and finalization steps from `run_forward_sync()` into helper functions so the orchestration wrapper stays readable without changing the public entrypoint.

## Decision Log
- Chosen: split the phase logic rather than the executor because the orchestration wrapper is the stable public surface.
- Chosen: keep failure capture and final state updates together so the error path stays auditable.
- Rejected: moving this into `models.py` because sync job orchestration does not belong on the model object.

## Rollback
Collapse the helper functions back into `run_forward_sync()` if the split causes lifecycle drift or makes failure handling harder to verify.
