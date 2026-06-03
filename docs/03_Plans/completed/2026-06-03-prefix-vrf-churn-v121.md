# Prefix VRF Churn Patch Release (v1.2.1)

## Goal

Stop `ipam.prefix` syncs from updating otherwise unchanged prefixes only because
the NetBox VRF foreign key is re-resolved during repeat syncs.

## Constraints

- Preserve the existing Forward SaaS API/NQE reduction work: prefix shards must
  continue to use `forward_netbox_shard_keys` query parameters.
- Keep `ipam.ipaddress` coalesce behavior unchanged.
- Keep the fix local to the NetBox adapter, coalesce contract, shard-key
  derivation, tests, and release docs.
- Do not add customer identifiers, network IDs, credentials, or screenshots.

## Field Signal

A production NetBox change log showed an unchanged prefix row where the only
changed field was `vrf` moving between two NetBox object IDs. That means the
prefix import path can still match an existing prefix too broadly and then
rewrite the VRF reference, producing unnecessary branch diffs and object
changes.

## Touched Surfaces

- `forward_netbox/utilities/sync_ipam.py`
- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/utilities/sync_runner_contracts.py`
- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/utilities/apply_engine_bulk.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_models.py`
- Release docs and package version metadata

## Approach

- Tighten built-in/default `ipam.prefix` coalesce identity to `prefix + vrf`.
- Make the prefix adapter use explicit `vrf__isnull=True` for global prefixes
  and exact `prefix + vrf` for VRF-scoped prefixes.
- Keep existing `ipam.ipaddress` fallback behavior unchanged.
- Add focused regressions for:
  - existing global prefix plus incoming VRF-scoped prefix creates a distinct
    row instead of rewriting the global row
  - repeating the same VRF-scoped prefix row is a no-op
  - built-in and default prefix maps no longer seed prefix-only identity

## Validation

- Focused prefix regression set passed:
  - `ForwardBranchBudgetPlanTest.test_ipam_prefix_global_shard_key_preserves_parameterized_fetch`
  - `ForwardSyncRunnerTest.test_validate_row_shape_allows_prefix_with_null_vrf_identity`
  - `ForwardSyncRunnerTest.test_validate_row_shape_allows_prefix_with_empty_vrf_identity`
  - `ForwardSyncRunnerTest.test_validate_row_shape_rejects_prefix_missing_vrf_identity`
  - `ForwardSyncRunnerTest.test_apply_ipam_prefix_keeps_global_and_vrf_scoped_rows_distinct`
  - `ForwardSyncRunnerTest.test_apply_ipam_prefix_repeat_sync_does_not_rewrite_vrf`
  - `ForwardSyncRunnerTest.test_run_prefix_only_fresh_sync_imports_prefix_rows`
- `invoke lint` passed with `.venv/bin` on `PATH`.
- `invoke harness-check` passed with `.venv/bin` on `PATH`.
- `invoke harness-test` passed with `.venv/bin` on `PATH`.
- `invoke check` passed.
- `invoke scenario-test` passed.
- `invoke docs` passed.
- `invoke test` passed: `879` tests.
- `invoke ci` passed and built:
  - `dist/forward_netbox-1.2.1.tar.gz`
  - `dist/forward_netbox-1.2.1-py3-none-any.whl`
- Release publication remains the final step after commit, push, tag, and CI.

## Decision Log

- Chose exact prefix adapter lookups instead of another generic coalesce
  fallback because null VRF is a meaningful NetBox identity value for prefixes.
- Chose to keep the prefix shard NQE parameter contract unchanged because the
  release risk is NetBox object churn, not Forward query volume.

## Rollback

Restore the prior prefix coalesce defaults and adapter lookup behavior. That
would re-enable prefix-only matching, so rollback should only be used if exact
VRF identity blocks a needed migration path.
