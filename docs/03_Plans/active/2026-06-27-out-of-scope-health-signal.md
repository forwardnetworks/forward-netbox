# Out-of-scope orphan health signal

**Date:** 2026-06-27

## Goal
The health summary surfaces the **backfilled** device count (in-scope, kept) but
not the **out-of-scope orphan** count (matches no included tag, removable). That
removable bucket only existed on the Scope Reconciliation page behind a live
query, so operators repeatedly asked "why isn't out-of-scope stuff removed?"
Surface it at a glance, symmetric to the backfilled signal.

## Constraints
- Cheap on render: DB tag-count, no live Forward query (mirror collection-gap).
- Read-only signal; deletion stays behind the existing Prune orphans action.
- No change to scope/prune semantics.

## Touched Surfaces
- `forward_netbox/utilities/scope_reconciliation.py` — add `forward-out-of-scope`
  tag constants; factor the add/remove into `_apply_maintained_device_tag`;
  `tag_backfilled_devices` now also maintains the out-of-scope tag and returns
  `total_out_of_scope` (backfilled keys unchanged).
- `forward_netbox/utilities/health.py` — `_out_of_scope_summary` (cheap tag count
  + trend from persisted `total_out_of_scope`); generalized `_job_data_count_trend`;
  wired into `get_health` as `out_of_scope`.
- `forward_netbox/templates/forward_netbox/forwardsync_health.html` — "Out of
  Scope" card mirroring the Collection Gap card.
- Tests: `test_scope_module_ui.py` (out-of-scope tagging), `test_health.py`
  (out-of-scope summary).

## Approach
The reconciliation already computes the out-of-scope set (`_out_of_scope`). The
existing backfilled tag job now maintains a second maintained tag for orphans in
the same live call, and records `total_out_of_scope` in `job.data` so the health
trend is free. Health reads the tag count (current) + job.data history (trend),
exactly like backfilled.

## Validation
Unit: out-of-scope tagging add set + count; health summary info/warn. Full suite
(902). Lint/harness/sensitive. Health page renders the new card.

## Rollback
Additive: revert the four surfaces + tests. The `forward-out-of-scope` tag is
self-healing (cleared when a device re-enters scope) and harmless if left.

## Decision Log
- Mirror the backfilled tag+signal pattern rather than a new model — cheapest,
  on-pattern, gives operators a `?tag=forward-out-of-scope` device filter too.
- Maintain both tags in one job/live call (no extra Forward round-trip).
- Signal stays `warn` (not escalating) — orphans accumulate until pruned, which is
  expected, not degrading like a growing collection gap.
