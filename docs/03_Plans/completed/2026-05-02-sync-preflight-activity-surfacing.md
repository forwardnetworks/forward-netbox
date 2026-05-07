# Sync Preflight Activity Surfacing

## Goal

Surface long pre-ingestion preflight work in UI/logs so operators can tell healthy planning/validation from a stuck sync.

## Constraints

- Keep the NetBox-native sync workflow unchanged.
- Do not expose customer identifiers in test fixtures or docs.
- Maintain compatibility with existing branch-run resume state.

## Touched Surfaces

- `forward_netbox/models.py`
- `forward_netbox/utilities/multi_branch.py`
- `forward_netbox/choices.py`
- `forward_netbox/templates/forward_netbox/forwardsync.html`
- `forward_netbox/management/commands/forward_seed_ui_harness.py`
- `forward_netbox/tests/test_models.py`
- `scripts/playwright_forward_ui.mjs`

## Approach

1. Write phase metadata (`phase`, `phase_message`, `phase_started`) into branch-run state during sync execution.
2. Emit explicit phase log messages at preflight/planning/validation boundaries.
3. Show `Current activity` on sync detail with elapsed time formatting.
4. Add source `syncing` status and set it when sync starts.
5. Seed deterministic phase data for UI harness and assert it in Playwright.

## Decision Log

- Chose branch-run state for phase data to avoid adding new database models.
- Chose elapsed-time rendering on Sync page to improve operator confidence during silent pre-ingestion windows.
- Chose Playwright assertion on seeded text to make the UI guard deterministic.

## Validation

- `invoke harness-check`
- `invoke harness-test`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox bash -lc "cd /opt/netbox/netbox && python manage.py test --keepdb --noinput forward_netbox.tests.test_models.ForwardSyncModelTest.test_display_parameters_include_branch_phase_details forward_netbox.tests.test_models.ForwardSyncModelTest.test_get_sync_activity_prefers_phase_message forward_netbox.tests.test_models.ForwardSyncModelTest.test_get_sync_activity_appends_elapsed_phase_time forward_netbox.tests.test_models.ForwardSyncModelTest.test_sync_sets_source_status_to_syncing_during_run"`
- `invoke playwright-test`

## Rollback

- Remove runtime phase metadata writes from `ForwardMultiBranchExecutor`.
- Remove elapsed formatting and `Current activity` UI row from sync detail.
- Remove `ForwardSourceStatusChoices.SYNCING` and restore prior source status transitions.
- Remove seeded preflight-phase fixture data and corresponding Playwright assertions.
