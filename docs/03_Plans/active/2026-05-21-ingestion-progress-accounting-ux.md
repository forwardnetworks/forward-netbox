# 2026-05-21 Ingestion Progress Accounting UX

## Goal

Reduce operator confusion during large branching runs by making ingestion progress display stable and explicitly shard-scoped.

## Constraints

- Keep behavior NetBox-native and Branching-native; no alternate sync path changes.
- Keep patch narrow to presentation and state plumbing for the existing progress UI.
- Preserve existing statistics payload keys consumed by templates and tests.

## Touched Surfaces

- `forward_netbox/utilities/ingestion_presentation.py`
- `forward_netbox/views.py`
- `forward_netbox/templates/forward_netbox/partials/ingestion_progress.html`

## Approach

1. Clamp per-model utilization display to `min(current, total)` so visual progress never exceeds `100%` when intermediate counters temporarily overshoot due to delete-heavy cascades.
2. Pass execution state (`get_execution_display_state`) into ingestion log/progress/detail views.
3. Render explicit shard row progress (`current_row_count/current_row_total`) in the progress card when available.
4. Run full local CI gate.

## Rollback

- Revert the three touched files above to remove the clamp and shard-row progress line if unexpected UI regressions occur.

## Decision Log

- We intentionally corrected display/accounting only; we did not alter underlying sync/delete execution semantics.
- We chose explicit shard row progress text because it is the clearest operator signal during long-running shard execution.

## Validation

1. Run full local regression gate via `poetry run invoke ci`.
2. Verify no harness policy failures.
3. Verify progress UI renders with and without active execution state.
