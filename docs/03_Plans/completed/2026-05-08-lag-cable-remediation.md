# LAG Cable Remediation

## Goal

Prevent interface shards from failing when a previously cabled interface is later modeled as a native NetBox LAG, and prevent inferred cable rows from targeting aggregate interfaces.

## Constraints

- Keep the fix NetBox-native: use normal cable deletion and interface update behavior inside the active Branching branch.
- Do not add a side-channel cleanup workflow or require operator cleanup before sync.
- Keep shipped NQE changes paired with tests.
- Do not persist customer identifiers, network IDs, snapshot IDs, or screenshots.

## Touched Surfaces

- `forward_netbox/queries/forward_inferred_interface_cables.nqe`
- `forward_netbox/utilities/sync_interface.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`

## Approach

Filter inferred cable rows so neither local nor remote endpoint is an aggregate interface. This prevents new cable imports from targeting interfaces that NetBox requires to remain uncabled when represented as `type="lag"`.

For upgrade/remediation safety, have the interface adapter detect an existing cable before converting an existing interface to `type="lag"`. Delete that cable in the active branch before applying the LAG update, then continue with the normal interface upsert and LAG membership behavior.

## Validation

- `docker compose --project-name forward-netbox --project-directory development exec -T netbox bash -lc 'cd /opt/netbox/netbox && python manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_dcim_interface_sets_lag_membership_after_parent forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_dcim_interface_creates_lag_placeholder_across_shards forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_dcim_interface_removes_existing_cable_before_lag_conversion forward_netbox.tests.test_query_registry.QueryRegistryTest.test_inferred_interface_cable_query_uses_resolved_interface_links'`
- `invoke lint`

## Rollback

Revert the query filter and adapter remediation. Operators would then need to manually remove any existing cable attached to an interface before a sync can convert it to a native LAG.

## Decision Log

- Rejected: leave cable cleanup to operators. The failure happens mid-shard and blocks otherwise valid sync work.
- Rejected: keep aggregate links as cables. NetBox explicitly disallows cables on LAG interfaces.
