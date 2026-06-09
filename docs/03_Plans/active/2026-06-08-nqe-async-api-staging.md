# NQE Async API Staging

## Goal

Stage client-side support for Forward's 26.6 async NQE query execution API so the
plugin can opt into non-blocking query execution once the endpoint is available
on the target Forward deployment.

## Constraints

- Keep the current synchronous `/nqe` path as the default until live 26.6 SaaS
  validation proves the endpoint is available and compatible.
- Do not create a second sync workflow. Async NQE is a fetch-layer transport
  option under the existing `ForwardClient.run_nqe_query` contract.
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

1. Add disabled-by-default source parameters:
   - `nqe_async_enabled`
   - `nqe_async_poll_interval_seconds`
   - `nqe_async_max_polls`
2. Build async query execution around the confirmed Forward API shape:
   - `POST /networks/{networkId}/nqe-executions?snapshotId=...`
   - `GET /networks/{networkId}/nqe-executions/{executionKey}`
   - `GET /networks/{networkId}/nqe-executions/{executionKey}/result`
3. Trigger async execution once per query, poll status until `COMPLETED`, then
   page result reads with `offset` and `limit`.
4. Only use async mode when `network_id`, `snapshot_id`, and JSON result format
   are present. Otherwise, continue to the synchronous `/nqe` endpoint.
5. Record separate usage counters for async trigger, status, and result calls so
   Sync Health can later expose the fetch mode and polling footprint.
6. Leave NQE diff execution synchronous until Forward documents an async diff
   contract.

## Validation

- Unit tests for request shape, result pagination, non-OK outcomes, poll
  exhaustion, and sync fallback.
- `invoke harness-test`
- `invoke lint`
- `invoke check`
- Future live validation after 26.6: one raw query and one saved query against a
  small snapshot with async enabled, confirming rows match the synchronous path.

## Rollback

Disable `nqe_async_enabled` on the source. Since async NQE remains a client fetch
transport and does not persist execution keys in NetBox state, rollback does not
require data cleanup.

## Decision Log

- Do not make async NQE default in this staging branch. Existing sync behavior is
  proven and the async endpoint is not yet released broadly.
- Do not emulate async NQE for diffs. The current Forward API source confirms
  async query execution only; using it for diff-shaped workflows would require
  separate semantics.
