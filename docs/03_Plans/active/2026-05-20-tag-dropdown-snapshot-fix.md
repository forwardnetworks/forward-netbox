## Goal
Fix Forward source tag dropdown population when Forward rejects `latestProcessed` as a literal NQE snapshot id.

## Constraints
- Keep source tag lookup in the existing NetBox plugin API endpoint.
- Preserve existing dropdown API contract (`count/results/detail`).

## Touched Surfaces
- `forward_netbox/api/views.py`

## Approach
1. Resolve latest processed snapshot id via Forward API before running the tag query.
2. Use concrete snapshot id for `run_nqe_query`.
3. Return a clear detail message when no processed snapshot can be resolved.

## Rollback
Revert endpoint snapshot-resolution block to prior literal selector behavior.

## Decision Log
- Use explicit snapshot id for compatibility with Forward orgs that reject selector tokens on NQE endpoint.

## Validation
- `python manage.py test --keepdb forward_netbox.tests.test_api_views`
- `invoke ci`
