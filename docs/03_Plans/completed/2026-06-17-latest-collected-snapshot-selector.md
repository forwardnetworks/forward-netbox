# latestCollected Snapshot Selector (v1.5.1)

## Goal

Let a sync skip Forward snapshots whose in-scope devices are all backfilled
(collection canceled) and resolve instead to the most recent snapshot that
actually collected an in-scope device, so a fully-backfilled `latestProcessed`
snapshot no longer silently syncs zero rows.

## Constraints

- All built-in queries only ingest devices with `snapshotInfo.result ==
  completed`; backfilled devices are intentionally excluded.
- The selector applies to the whole sync — one resolved snapshot for every
  model, not per-model snapshot sourcing.
- Because the resolved snapshot can change between runs, `latestCollected` always
  runs a full fetch, never a Forward `nqe-diff`.
- No customer data, credentials, or network IDs in repo, tests, or docs; live
  validation is local only via the untracked CustomerOrg source.

## Touched Surfaces

- `forward_netbox/utilities/forward_api_impl.py` — `LATEST_COLLECTED_SNAPSHOT`,
  `get_latest_collected_snapshot_id`, shared `build_device_tag_scope_where`.
- `forward_netbox/utilities/forward_api.py` — re-exports.
- `forward_netbox/utilities/sync_facade.py` — resolver wiring.
- `forward_netbox/utilities/query_fetch_execution.py` — snapshot-info lookup and
  all-backfilled warning.
- `forward_netbox/utilities/sync_execution.py` — resolved snapshot metadata.
- `forward_netbox/forms.py`, `forward_netbox/api/views.py` — selectable option.
- Tests across `test_sync_facade`, `test_forward_api`, `test_sync`.
- `docs/01_User_Guide/configuration.md`, `docs/02_Reference/architecture-flow.md`.

## Approach

Add a `latestCollected` selector that resolves at sync time: scan the most recent
processed snapshots newest-first (bounded), probe each for an in-scope device
with `result == completed`, and return the first match; raise a clear error if
none. Scope the probe to the source device-tag filter. Record the resolved
snapshot's own metadata. When a tag-scoped run finds zero collected but matching
backfilled devices, warn and point at `latestCollected` instead of logging the
plain "0 matched" line.

## Validation

- `invoke test` across `test_sync_facade`, `test_forward_api`, `test_sync`.
- `invoke lint`, `invoke harness-check`, `invoke docs`.
- Live: confirmed against the CustomerOrg source that `latestCollected` skips the
  backfilled snapshot and resolves a collected one.

## Rollback

Revert the listed modules; existing `latestProcessed` and fixed-snapshot
selectors are unchanged, so reverting is non-destructive.

## Decision Log

- "Any in-scope device collected" threshold chosen over "all collected" — more
  forgiving and syncs whatever is real.
- Rejected per-model snapshot sourcing — the selector must apply to the whole
  sync to avoid orphaned child objects referencing devices from another snapshot.
