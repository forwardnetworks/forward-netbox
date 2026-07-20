# 2026-05-23 Adaptive Density Learning Hardening

## Goal

Harden adaptive model-density learning so branch-budget tuning remains stable
under noisy runs and exposes operator confidence signals.

## Constraints

- Keep NQE as the source of truth for row normalization.
- Keep NetBox-native mutation behavior unchanged.
- Preserve branch budget hard limits and existing fallback guards.
- Avoid schema migrations for this tranche.

## Touched Surfaces

- `forward_netbox/utilities/density_learning.py`
- `forward_netbox/utilities/sync_state.py`
- `forward_netbox/utilities/multi_branch_lifecycle.py`
- `forward_netbox/utilities/multi_branch_executor.py`
- `forward_netbox/utilities/execution_telemetry.py`
- `forward_netbox/utilities/execution_ledger_metrics.py`
- `forward_netbox/utilities/health_summary_blocks.py`
- `forward_netbox/utilities/health.py`
- `forward_netbox/templates/forward_netbox/forwardsync_health.html`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_sync_state.py`
- `forward_netbox/tests/test_health.py`
- `forward_netbox/tests/test_models.py`
- `docs/03_Plans/active/2026-05-23-long-term-architecture-roadmap.md`

## Approach

1. Add a dedicated density-learning helper for normalization, guarded updates,
   and confidence scoring.
2. Persist both learned density values and per-model profile metadata.
3. Apply guarded learning in the execution lifecycle where observed shard change
   density is recorded.
4. Surface learned-vs-default confidence in sync summaries, health, and support
   telemetry outputs.
5. Validate with targeted tests plus harness/check gates.

## Implementation

- Added a density-learning utility with:
  - profile normalization
  - outlier rejection for warmed-up models
  - confidence score/bucket from sample count, variance, and recency
- Added persisted profile state in sync parameters:
  - `_model_change_density_profile`
- Updated execution lifecycle to:
  - update learned density via guarded learning
  - persist profile metadata
  - log explicit anomalies when observations are rejected
- Added operator visibility:
  - display/workload/execution summaries include profile summary
  - health summary includes a new `Density Learning` section/card
  - support execution metrics include density-profile summary

## Validation

- `docker compose --project-name forward-netbox --project-directory development exec -T netbox /opt/netbox/venv/bin/python /opt/netbox/netbox/manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardBranchBudgetPlanTest forward_netbox.tests.test_sync_state.ForwardSyncStateHelperTest forward_netbox.tests.test_health.ForwardSyncHealthTest forward_netbox.tests.test_models.ForwardSyncModelTest`
- `poetry run invoke harness-check`
- `poetry run invoke check`

## Rollback

- Remove the density profile parameter and helper usage.
- Restore direct EWMA density updates in `record_model_density`.
- Remove health/telemetry density-confidence surfaces.

## Decision Log

- Kept this tranche migration-free by storing confidence metadata in sync
  parameters and deriving summaries at read time.
- Retained legacy learned-density map for backward compatibility with existing
  branch-budget consumers.
