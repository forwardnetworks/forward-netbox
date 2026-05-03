# Sync Budget Feedback

## Goal

Expose branch-budget estimates on the sync detail page so operators can see the effective per-model row budget before a sync runs.

## Constraints

- Use the existing sync detail parameters card rather than inventing a new workflow.
- Keep the budget calculation generic and consistent with branch planning.
- Do not change sync execution semantics.

## Touched Surfaces

- `forward_netbox/models.py`
- `forward_netbox/tests/test_models.py`

## Approach

1. Include model change density and effective row-budget hints in `ForwardSync.get_display_parameters()`.
2. Make sure the cable default density seed is visible through the computed hints.
3. Add model tests that assert the display payload includes the tuned cable budget.

## Decision Log

- Chose to surface the data in the existing parameters card because it is already rendered on sync detail and survives branch lifecycle changes.
- Rejected a new standalone UI panel because it would duplicate the existing parameters source of truth.

## Validation

- `invoke lint`
- `docker compose --project-name forward-netbox --project-directory "/home/captainpacket/src/forward-netbox/development" exec -T netbox bash -lc "cd /opt/netbox/netbox && python manage.py test --keepdb --noinput forward_netbox.tests.test_models.ForwardSyncModelTest.test_display_parameters_include_branch_budget_hints forward_netbox.tests.test_models.ForwardSyncModelTest.test_display_parameters_include_model_change_density_when_present"`
- `invoke test`
- `invoke ci`

## Rollback

- Remove the extra keys from `ForwardSync.get_display_parameters()`.
- Remove the model tests that cover the displayed budget hints.
