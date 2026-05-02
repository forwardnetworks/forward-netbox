# Cable Conflict Policy Consistency

## Goal

Avoid hard sync failures when a cable row targets interfaces already connected to a different cable, while implementing that behavior through an explicit model conflict policy path.

## Constraints

- Preserve existing cable create/update behavior for exact endpoint matches.
- Do not silently delete or rewrite existing non-matching cables.
- Keep branch-native workflow and ingestion issue model unchanged.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/tests/test_sync.py`

## Approach

1. Add `dcim.cable` to `MODEL_CONFLICT_POLICIES` with `skip_warn_aggregate`.
2. Replace cable-specific warning counters with aggregated warning helpers keyed by model/reason.
3. Keep exact cable-match updates and reverse-endpoint reuse unchanged.
4. Update cable conflict tests to assert skip behavior and aggregated warning summary output.

## Decision Log

- Chose explicit policy routing so cable behavior is controlled like other model conflict modes (`strict`, `reuse_on_unique_conflict`).
- Rejected model-specific one-off logic because it does not scale to future conflict-handling consistency work.

## Validation

- `invoke lint`
- `docker compose --project-name forward-netbox --project-directory /home/captainpacket/src/forward-netbox/development exec -T netbox bash -lc "cd /opt/netbox/netbox && python manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_dcim_cable_skips_conflicting_existing_cable forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_apply_dcim_cable_aggregates_conflict_warnings"`
- `invoke check`
- `invoke scenario-test`
- `invoke test`
- `invoke harness-check`
- `invoke harness-test`
- `invoke ci`

## Rollback

- Remove `dcim.cable` from `MODEL_CONFLICT_POLICIES` and restore strict failure for occupied cable endpoints.
- Revert cable conflict test updates for skip/aggregation behavior.
