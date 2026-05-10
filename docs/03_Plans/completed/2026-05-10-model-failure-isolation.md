# Model Failure Isolation

## Goal

Prevent one stale or invalid Forward NQE model result from aborting the rest of a
sync, so downstream models such as routing can still run when an unrelated
device or virtual-chassis row is bad.

## Constraints

- Keep Forward NQE as the source of truth for normalization and NetBox-ready row
  shape.
- Keep NetBox and Branching native execution paths; do not introduce an external
  recovery queue or side-channel importer.
- Do not silently accept invalid virtual-chassis membership. NetBox requires a
  VC member position, and the plugin must not invent one.
- Do not mark a dirty run as an incremental diff baseline when any shard records
  row issues.
- Surface model failures through existing validation and ingestion result
  structures.

## Touched Surfaces

- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/utilities/multi_branch_executor.py`
- `forward_netbox/utilities/multi_branch_lifecycle.py`
- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/utilities/sync_device.py`
- `forward_netbox/utilities/sync_reporting.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_models.py`
- `forward_netbox/tests/test_synthetic_scenarios.py`
- `docs/01_User_Guide/troubleshooting.md`

## Approach

1. Tighten the `dcim.virtualchassis` contract so rows must include
   `vc_position`.
2. Reject positionless virtual-chassis assignment in the adapter before NetBox
   model validation raises a later device-save failure.
3. Treat adapter data-contract failures as row issues, consistent with existing
   row-level lookup/query failures.
4. Isolate per-model preflight and planning query failures into model results
   with diagnostics instead of aborting the whole planner.
5. Skip only the failed model's workload and continue planning later models.
6. Allow a multi-branch shard with row issues to finish and continue later
   shards, while preventing that run from becoming the incremental diff
   baseline.
7. Document the stale Forward Org Repository query remediation path for
   `vc_position` failures.

## Validation

- `invoke test -- forward_netbox.tests.test_synthetic_scenarios.SyntheticSyncScenarioHarnessTest.test_bad_model_rows_are_isolated_during_preflight forward_netbox.tests.test_sync.ForwardMultiBranchPlannerPreflightTest.test_preflight_skips_invalid_model_before_full_fetch forward_netbox.tests.test_sync.ForwardMultiBranchPlannerPreflightTest.test_preflight_failure_for_one_model_still_plans_later_models forward_netbox.tests.test_sync.ForwardMultiBranchPlannerPreflightTest.test_preflight_error_explains_disabled_optional_module_map forward_netbox.tests.test_sync.ForwardMultiBranchExecutorAdaptiveSplitTest.test_branch_row_issues_do_not_stop_later_shards_or_mark_baseline forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_virtual_chassis_rejects_membership_without_position forward_netbox.tests.test_models.ForwardNQEMapTest.test_virtual_chassis_map_rejects_query_missing_position`
- The NetBox test runner expanded the focused command to the full suite: 286
  tests passed.
- `invoke lint`
- `invoke check`
- `invoke docs`
- `invoke ci`

## Rollback

Revert the query-fetch isolation, shard-continuation behavior, and VC contract
changes together. After rollback, stale or invalid model rows can again abort a
full multi-branch sync, and `dcim.virtualchassis` will no longer be preflight
blocked for missing `vc_position`.

## Decision Log

- Rejected: inventing a `vc_position` in Python. NetBox position semantics are
  model data and must come from NQE.
- Rejected: aborting all later models after one model preflight failure. That
  creates unnecessary blast radius and can suppress unrelated routing/cabling
  imports.
- Rejected: marking a run with row issues as baseline-ready. That would make
  subsequent diffs trust an incomplete import.
