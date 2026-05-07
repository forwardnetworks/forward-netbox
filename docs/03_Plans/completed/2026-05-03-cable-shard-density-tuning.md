# Cable Shard Density Tuning

## Goal

Reduce oversized `dcim.cable` branch shards during long sync runs by applying deterministic density tuning before the first overflow retry.

## Constraints

- Keep the branch-budget mechanism generic for all models.
- Avoid changing sync semantics or model apply behavior.
- Preserve adaptive density learning from observed shard outcomes.

## Touched Surfaces

- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/utilities/multi_branch.py`
- `forward_netbox/tests/test_sync.py`

## Approach

1. Add model-default density seeds and model-specific safety-factor overrides to the generic branch-budget module.
2. Set `dcim.cable` as the initial tuned profile.
3. Route auto-split retry sizing through the same budget helper to keep planning and retry behavior aligned.
4. Add tests for cable default and override budget calculations.

## Decision Log

- Chose framework-level tuning to preserve consistency across initial planning and overflow splits.
- Rejected cable-only retry math because it diverges from the budget engine and drifts over time.

## Validation

- `invoke lint`
- `docker compose --project-name forward-netbox --project-directory "/home/captainpacket/src/forward-netbox/development" exec -T netbox bash -lc "cd /opt/netbox/netbox && python manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest.test_effective_row_budget_uses_cable_default_density_and_safety forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest.test_effective_row_budget_uses_cable_safety_override_with_observed_density forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest.test_effective_row_budget_scales_by_density"`
- `docker compose --project-name forward-netbox --project-directory "/home/captainpacket/src/forward-netbox/development" exec -T netbox bash -lc "cd /opt/netbox/netbox && python manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardMultiBranchExecutorAdaptiveSplitTest"`
- `invoke test`
- `invoke ci`

## Rollback

- Remove model default/safety entries for `dcim.cable`.
- Restore overflow split row-budget math to prior behavior.
- Revert associated tests.
