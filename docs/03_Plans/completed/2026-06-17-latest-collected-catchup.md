# latestCollected Snapshot Catch-Up

## Goal

Give the `latestCollected` snapshot selector the same end-of-run catch-up
behavior `latestProcessed` already has: if a newer collected snapshot appears
while a sync runs, automatically queue a follow-up sync instead of waiting for
the next scheduled interval.

## Constraints

- Catch-up resolution runs once at the end of a run (and after merge), not in a
  loop, so the extra `latestCollected` probe calls (snapshot scan + NQE probes)
  are bounded to one resolution per run.
- Must reuse the sync's own device-tag scope so the catch-up target matches what
  the sync would actually fetch.
- `latestCollected` raising "no collected snapshot" must be treated as a failed
  lookup (no catch-up), never an error.
- Backward compatible: existing decision reason strings and the
  `latest_processed_snapshot_id` key are preserved so callers/tests are
  unaffected.

## Touched Surfaces

- `forward_netbox/utilities/snapshot_freshness.py` — selector-aware resolution
  (`_resolve_latest_snapshot_id`, `DYNAMIC_SNAPSHOT_SELECTORS`).
- `forward_netbox/utilities/sync_facade.py` — promote `_device_tag_scope` to a
  public `device_tag_scope` helper for reuse.
- `forward_netbox/utilities/sync_orchestration.py`,
  `forward_netbox/utilities/ingestion_merge.py` — selector-aware catch-up log.
- `forward_netbox/tests/test_sync_orchestration.py` — latestCollected catch-up
  tests.
- `docs/01_User_Guide/configuration.md` — note catch-up now covers
  latestCollected.

## Approach

Generalize `latest_processed_catchup_decision` to fire for both dynamic
selectors. Resolve the target snapshot via `_resolve_latest_snapshot_id`:
`get_latest_processed_snapshot_id` for latestProcessed, or
`get_latest_collected_snapshot_id` scoped to the source device tags for
latestCollected. Compare to the current snapshot and queue an adhoc catch-up
when it advanced, reusing the existing active-job guard. Carry the selector in
the decision so the catch-up log names the right selector.

## Validation

- `invoke test --test-label forward_netbox.tests.test_sync_orchestration`
  (new latestCollected catch-up tests: advances vs no-collected probe failure).
- `invoke test --test-label forward_netbox.tests.test_sync_facade`.
- Regression: `test_sync`, `test_ingestion_merge`.
- `invoke lint`, `invoke harness-check`.

## Rollback

Revert the listed modules. Fixed-snapshot and latestProcessed catch-up behavior
are unchanged, so reverting only removes latestCollected catch-up.

## Decision Log

- Kept the original decision reason strings and `latest_processed_snapshot_id`
  key for backward compatibility rather than renaming, to avoid churn in callers
  and tests.
- Catch-up stays end-of-run (existing call sites) — not a polling loop — so the
  added latestCollected probe cost is one resolution per run.
