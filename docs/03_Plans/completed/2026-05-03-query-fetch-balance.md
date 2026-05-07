## Goal

Reduce the long "planning" pause before a multi-branch sync starts applying changes by balancing the query preflight and workload fetch stages without changing the user-visible workflow.

## Constraints

- Keep the native NetBox/Branching sync flow intact.
- Do not add new dependencies.
- Preserve validation and row-shape checks.
- Keep the change conservative; bounded concurrency only.

## Touched Surfaces

- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/tests/test_sync.py`
- `docs/03_Plans/completed/2026-05-03-query-fetch-balance.md`

## Approach

Introduce bounded parallelism for query preflight and workload fetching so the fetch phase can use more than one in-flight NQE request while preserving deterministic result ordering and existing validation behavior.

## Validation

- `invoke lint`
- `invoke test`
- `invoke ci`
- Live smoke plan-only run on the smoke dataset completed in 224.9 seconds and planned 171 branches with a max planned shard size of 9,877.

## Rollback

Revert the query fetch concurrency helper and restore the serial loop behavior if NQE load or ordering regressions appear.

## Decision Log

- Chosen: bounded parallelism inside the fetcher.
- Rejected: changing planning semantics or skipping preflight, because that would weaken the native workflow instead of balancing it.
