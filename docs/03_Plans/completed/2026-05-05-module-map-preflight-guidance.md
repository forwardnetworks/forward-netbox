# Module Map Preflight Guidance

## Goal

Make the `dcim.module` preflight failure actionable when the sync model is
selected but the optional `Forward Modules` NQE map is not enabled.

## Constraints

- Keep `dcim.module` beta and disabled by default.
- Do not auto-enable optional maps during sync execution.
- Preserve fail-fast preflight behavior when a selected model has no enabled
  query map.

## Touched Surfaces

- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/utilities/query_registry.py`
- `forward_netbox/tests/test_sync.py`
- `docs/01_User_Guide/troubleshooting.md`

## Approach

Add a model-aware missing-map message in the query-fetch boundary. Optional
built-in maps now report the exact map to enable and the alternate model toggle
to disable. Apply the same message in both preflight and skipped-preflight
workload fetch paths.

## Validation

- `python manage.py test forward_netbox.tests.test_sync.ForwardMultiBranchPlannerPreflightTest.test_preflight_error_explains_disabled_optional_module_map --keepdb --noinput`
- `python manage.py test forward_netbox.tests.test_sync.ForwardMultiBranchPlannerPreflightTest forward_netbox.tests.test_query_registry.QueryRegistryTest --keepdb --noinput`
- `invoke lint`
- `invoke harness-check`

## Rollback

Revert the query-fetch message helper, optional-map registry helper, regression
test, docs note, and this plan.

## Decision Log

- Chosen: keep the hard preflight failure and improve the message because a
  selected model without an enabled map cannot produce deterministic sync input.
- Rejected: auto-enable `Forward Modules` because the module path remains beta
  and must stay operator-selected.
