# NQE Async API Switchover

## Goal

Finish the client-side async NQE switchover so the plugin always uses
non-blocking query execution on Forward 26.6 deployments. Async NQE requires
Forward 26.6 or newer.

## Constraints

- Do not retain the synchronous `/nqe` path. Async NQE is now the only fetch
  transport under the existing `ForwardClient.run_nqe_query` contract.
- Preserve existing pagination, parameter pushdown, API pacing, usage metrics,
  and row parsing semantics.
- Do not route `nqe-diffs` through async NQE until Forward exposes and documents
  an equivalent async diff endpoint.
- Keep customer data, credentials, network IDs, and live execution keys out of
  docs and tests.

## Touched Surfaces

- `forward_netbox/utilities/forward_api_impl.py`
- `forward_netbox/utilities/forward_api.py`
- `forward_netbox/tests/test_forward_api.py`
- `docs/02_Reference/` async NQE reference note, if implementation expands past
  the client boundary

## Approach

1. Add source parameters and source-form support:
   - `nqe_async_poll_interval_seconds`
   - `nqe_async_max_polls`
2. Build async query execution around the confirmed Forward API shape:
   - `POST /networks/{networkId}/nqe-executions?snapshotId=...`
   - `GET /networks/{networkId}/nqe-executions/{executionKey}`
   - `GET /networks/{networkId}/nqe-executions/{executionKey}/result`
3. Trigger async execution once per query, poll status until `COMPLETED`, then
   page result reads with `offset` and `limit`.
4. Require `network_id`, `snapshot_id`, and JSON result format before issuing
   async NQE.
5. Record separate usage counters for async trigger, status, and result calls so
   Sync Health can later expose the fetch mode and polling footprint.
6. Leave NQE diff execution synchronous until Forward documents an async diff
   contract.

## Validation

- Unit tests for request shape, result pagination, non-OK outcomes, and poll
  exhaustion.
- `invoke harness-test`
- `invoke lint`
- `invoke check`
- Local validation against a Forward 26.6 deployment using the async transport
  on a small snapshot, confirming rows match the expected query output and the
  validation org query set stays in sync with the bundled maps.

## Rollback

Restore the previous release tag or revert the async transport commit. Since
async NQE does not persist execution keys in NetBox state, rollback does not
require data cleanup.

## Decision Log

- Do not keep a sync fallback. Async NQE is the only supported transport in
  this branch.
- Do not emulate async NQE for diffs. The current Forward API source confirms
  async query execution only; using it for diff-shaped workflows would require
  separate semantics.
