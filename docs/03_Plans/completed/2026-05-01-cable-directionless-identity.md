## Goal

Make `dcim.cable` diff identity and multi-branch sharding direction-insensitive so the same physical cable is treated consistently even when Forward reports endpoints in reverse order.

## Constraints

- Preserve strict endpoint resolution in the NetBox cable adapter.
- Keep existing `dcim.cable` coalesce fields compatible with seeded query maps.
- Avoid version or release metadata changes; this is staged for a future feature release.

## Touched Surfaces

- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/tests/test_sync.py`

## Approach

1. Add a shared canonical cable endpoint identity helper that sorts the two endpoint tuples.
2. Use that helper for `dcim.cable` diff identity comparisons so reversed endpoints do not schedule a delete for the same cable.
3. Use that helper for `dcim.cable` branch shard keys so reversed cable rows land in the same branch planning bucket.
4. Add focused tests for reversed endpoint diff behavior and shard-key stability.

## Rollback

Revert this plan and the associated helper/use sites to restore directional cable identity behavior.

## Decision Log

- Chosen: keep the seeded coalesce fields as-is and apply cable-specific canonical identity only where row identity is operationally significant.
- Rejected: changing public coalesce fields to synthetic fields; shipped query maps do not emit a synthetic cable key today.
- Rejected: loosening cable adapter matching; exact endpoint matching remains the correct NetBox safety boundary.

## Validation

- `pre-commit run --all-files`
- `python manage.py test --keepdb --noinput forward_netbox.tests.test_sync` inside the NetBox development container

