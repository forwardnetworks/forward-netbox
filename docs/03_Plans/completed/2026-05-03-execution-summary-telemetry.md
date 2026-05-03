# Execution Summary Telemetry

## Goal

Expose a concise run-level summary on sync and ingestion detail that shows shard count, retry count, and model timing from the latest run.

## Constraints

- Reuse existing sync/ingestion detail pages.
- Keep the summary derived from existing collected data.
- Avoid changing any sync execution semantics.

## Touched Surfaces

- `forward_netbox/models.py`
- `forward_netbox/templates/forward_netbox/forwardsync.html`
- `forward_netbox/templates/forward_netbox/forwardingestion.html`
- `forward_netbox/tests/test_models.py`

## Approach

1. Add a reusable execution-summary helper on `ForwardIngestion`.
2. Add a sync-level wrapper that reuses the latest ingestion summary.
3. Render the summary on sync and ingestion detail pages.
4. Add tests for retry counting, runtime aggregation, and sync-level reuse.

## Decision Log

- Chose to keep the summary generic and data-driven so it works across all models and run types.
- Rejected a standalone performance dashboard because it would be a second reporting surface with the same source data.

## Validation

- `invoke lint`
- `docker compose --project-name forward-netbox --project-directory "/home/captainpacket/src/forward-netbox/development" exec -T netbox bash -lc "cd /opt/netbox/netbox && python manage.py test --keepdb --noinput forward_netbox.tests.test_models.ForwardSyncModelTest.test_execution_summary_includes_latest_ingestion_telemetry forward_netbox.tests.test_models.ForwardSyncModelTest.test_display_parameters_include_branch_budget_hints forward_netbox.tests.test_models.ForwardSyncModelTest.test_display_parameters_include_model_change_density_when_present"`
- `invoke test`
- `invoke ci`

## Rollback

- Remove the execution-summary helpers and the two template includes.
- Remove the model tests covering summary output.
