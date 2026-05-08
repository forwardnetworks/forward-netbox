# NQE Preflight Hardening

## Goal

Make all-enabled smoke runs more resilient during NQE preflight by retrying transient Forward API transport failures, fixing optional OSPF instance row identity for default-VRF rows, and applying learned branch change density to remaining same-model shards after a branch budget retry.

## Constraints

- Keep timeout behavior aligned with Forward NQE's long-running query boundary.
- Do not hide deterministic query contract failures behind retries.
- Keep optional routing maps beta and disabled by default.
- Do not persist customer identifiers, network IDs, snapshot IDs, or screenshots.

## Touched Surfaces

- `forward_netbox/utilities/forward_api.py`
- `forward_netbox/utilities/multi_branch_executor.py`
- `forward_netbox/utilities/multi_branch_lifecycle.py`
- `forward_netbox/utilities/model_validation.py`
- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/tests/test_forward_api.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`
- `docs/01_User_Guide/configuration.md`

## Approach

Add bounded retries for transient `httpx` request and timeout errors in the Forward API client. This covers Forward-side disconnects during read-only NQE calls without changing deterministic HTTP status or row-shape failures.

Add a secondary OSPF instance coalesce identity of `device + process_id`, so default-VRF rows with `vrf = null` satisfy the same model contract used by the adapter.

After a shard exceeds the branch change budget, record the observed row-to-change density, split the failed shard, and also re-split future shards for that model using the learned density. This avoids repeatedly creating oversized branches for the same model during the same run.

## Validation

- Targeted Forward API retry tests.
- Query registry test for OSPF instance coalesce fallback.
- Multi-branch executor test for future shard re-splitting after observed branch density.
- `invoke lint`.
- `invoke ci`.

## Rollback

Remove retry settings, restore the previous OSPF coalesce set, and remove the future-shard re-splitting path. Transient Forward disconnects will fail the sync immediately, default-VRF OSPF instance rows will require manual map customization, and oversized shards will return to one-failure-at-a-time retries.

## Decision Log

- Rejected: increase the default timeout. The observed failure was a remote disconnect and a deterministic row-shape failure, not a local timeout.
- Rejected: retry row validation errors. Retrying invalid rows would only delay a deterministic failure.
- Rejected: raise branch budgets after a retry. The Branching guidance remains the constraint; row shards should get smaller instead.
