# Query Diagnostics Boundary Extraction

## Goal

Move the IPAM and routing diagnostic synthesis out of `forward_netbox/utilities/query_fetch.py` into a dedicated diagnostics boundary while preserving the current query fetch and model-result behavior.

## Constraints

- Preserve the current query fetch and preflight behavior.
- Keep diagnostics visible in the same model-result shape as today.
- Do not change the underlying diagnostic query semantics.
- Keep customer-specific data out of examples and tests.

## Touched Surfaces

- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/utilities/query_diagnostics.py`
- `forward_netbox/tests/test_sync.py`
- `ARCHITECTURE.md`
- `docs/03_Plans/technical-debt.md`

## Approach

Move the IPAM and routing diagnostic helpers into `forward_netbox/utilities/query_diagnostics.py` and keep `ForwardQueryFetcher` focused on context resolution, preflight, workload fetching, and model-result assembly. The fetcher delegates diagnostics so the warning synthesis is isolated from the main fetch path.

## Validation

- `invoke lint`
- `invoke test`
- `invoke docs`
- `invoke ci`

## Rollback

Restore the diagnostic helper bodies inside `forward_netbox/utilities/query_fetch.py` and remove the diagnostics module if the extraction changes behavior.

## Decision Log

- Rejected leaving diagnostics inline in the fetcher because the module is still carrying unrelated concerns after the preflight and workload split.
- Rejected changing diagnostic output structure because those warnings are already used by tests and operator-visible UI surfaces.
