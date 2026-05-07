# Ingestion Presentation Boundary Extraction

## Goal
Move `ForwardIngestion` snapshot, metrics, model-result, statistics, and execution-summary presentation helpers out of `forward_netbox/models.py` into a dedicated helper boundary while preserving the current serialized output.

## Constraints
- Preserve the current output shape for API/UI consumers.
- Do not change merge behavior or persisted state semantics.
- Keep customer-specific data out of examples and tests.
- Keep the refactor mechanical and reversible.

## Touched Surfaces
- `forward_netbox/models.py`
- `forward_netbox/utilities/ingestion_presentation.py`
- `forward_netbox/tests/test_models.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Validation
- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Approach
Move the snapshot and execution-summary formatting helpers into `forward_netbox/utilities/ingestion_presentation.py` so presentation stays separate from persisted state and merge logic.

## Decision Log
- Rejected moving merge or status-transition logic in the same tranche because this pass is only about presentation helpers.
- Rejected changing the summary schemas because the API/UI already depend on them.

## Rollback
Inline the presentation helpers back into `ForwardIngestion` if the output shape changes or the boundary adds unnecessary indirection.
