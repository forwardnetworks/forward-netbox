# Delete-Heavy Support Bundle Hardening

## Goal

Make delete-heavy syncs easier to diagnose by carrying delete-wave evidence
into the exported support bundle, so operators can explain delete counts,
dependency skips, and stage progression from one artifact.

## Constraints

- Preserve the current NetBox-native and Branching-native mutation flow.
- Keep delete-wave reporting read-only and derived from existing run/ingestion
  state.
- Do not add retry or workaround logic.
- Keep customer-sensitive values out of committed fixtures and docs.

## Touched Surfaces

- `forward_netbox/utilities/execution_ledger_serialization.py`
- `forward_netbox/tests/test_log_export.py`
- `docs/03_Plans/active/2026-05-24-long-term-architecture-next-tranche.md`

## Approach

1. Extend ingestion support-bundle serialization with delete-wave summary data
   derived from the latest execution run and latest ingestion.
2. Keep the existing health-page delete-wave card as the source of truth for the
   shape of the summary.
3. Add regression coverage to prove the exported bundle carries the same
   delete-wave evidence operators already see in health.

## Validation

- `invoke harness-check`
- Focused unit tests for the support-bundle JSON export
- `invoke ci` before release if the tranche is merged into a release branch

## Rollback

Remove the `delete_wave` support-bundle field and revert the matching test if
the summary proves noisy or redundant.

## Decision Log

- Keep this as read-only evidence. The goal is better diagnostics, not new
  delete behavior.
