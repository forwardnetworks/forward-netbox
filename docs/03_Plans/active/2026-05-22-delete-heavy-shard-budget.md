# Delete-Heavy Shard Budget

## Goal

Prevent delete-heavy Branching shards from exceeding NetBox branch-change guidance when one planned delete row expands into many native NetBox object changes.

## Constraints

- Keep normal device upsert planning unchanged.
- Stay NetBox/Branching-native; do not delete through side channels.
- Treat the configured branch budget as a branch-change guideline, not only a row-count cap.
- Do not commit customer identifiers, network IDs, snapshot IDs, or screenshots.

## Touched Surfaces

- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/utilities/multi_branch_lifecycle.py`
- `forward_netbox/tests/test_sync.py`

## Approach

1. Keep normal row budgets unchanged for upsert-heavy workloads.
2. Add delete-sensitive row-budget estimation for models where delete rows are known to expand into many NetBox changes.
3. Apply the same budget rule during initial planning and adaptive retry re-splitting.
4. Cover the behavior with focused branch-budget tests.
5. Use a conservative `dcim.device` delete expansion factor because live tag-prune evidence showed 12x still allowed a shard to exceed 10k native branch changes.

## Validation

- `poetry run ruff check forward_netbox/utilities/branch_budget.py forward_netbox/utilities/multi_branch_lifecycle.py forward_netbox/tests/test_sync.py`
- Focused NetBox test for `ForwardBranchBudgetPlanTest`
- `poetry run invoke harness-check`

## Rollback

Remove the delete-sensitive workload budget helper and return `build_branch_plan_with_density` plus retry splitting to model-only row budgets.

## Decision Log

- Rejected display-only correction because the reported shard still exceeded branch-change guidance after UI clamping.
- Rejected hard-capping all `dcim.device` shards because it would slow normal device upsert imports unnecessarily.
- Chose workload-sensitive budgeting so delete-heavy cleanup runs get smaller shards while normal upsert runs keep current behavior.
- Raised the default `dcim.device` delete expansion factor to 20x after live evidence showed a 12x assumption still produced an 11,957-change shard.
