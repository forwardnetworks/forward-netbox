# Surface Forward Collection-Failure Reasons in Scope Reconciliation (2026-06-21)

## Goal

Turn the opaque "Tagged but backfilled: N" count into a self-service diagnostic
so an operator never has to run a manual Forward API probe to learn *why*
in-scope devices were not freshly collected. Add a per-reason breakdown
(AUTHENTICATION_FAILED / CONNECTION_TIMEOUT / INCOMPLETE_SETUP /
STATE_COLLECTION_FAILED), per-device staleness age, and an opt-in auto-refresh
of the `forward-backfilled` tag on each sync.

## Constraints

- No customer identifiers, network IDs, snapshot IDs, or raw live rows in the
  diff, tests, or docs.
- Backward compatible: older reconciliation payloads (rows without `reason` /
  `backfillTime`) must still work — reason falls back to `unknown`, staleness to
  `None`.
- No new model field / migration (the breakdown is computed live from the
  existing scope-reconciliation NQE; the cheap DB-count health signal is
  unchanged).
- Auto-tag refresh is strictly opt-in and must never affect the sync result.

## Bundled changes

1. **Per-reason breakdown + staleness** — `compute_scope_reconciliation` NQE
   select now also fetches `toString(device.snapshotInfo.result)`,
   `collectionTime`, `backfillTime`. New helpers `_collection_failure_reason`
   and `_stale_days` parse them. Report gains `backfilled_reason_breakdown`
   (reason -> count) and `present_backfilled_detail_sample`
   (name / reason / stale_days). The CLI audit command emits these for free.
2. **Scope Reconciliation panel** — new "Backfill Reason Breakdown" card and an
   enriched "Tagged-but-Backfilled Sample" table (device / reason / stale age).
3. **Auto-refresh backfilled tag** — `_maybe_enqueue_backfilled_tag_refresh`
   hooks into `sync_forwardsync`, gated on the `auto_tag_backfilled` parameter
   (mirrors the existing `auto_refresh_device_analysis` pattern), so the tag and
   the Collection Gap health signal stop drifting from reality between manual
   refreshes.
4. **Health message** — the Collection Gap signal now points operators at the
   Scope Reconciliation per-reason breakdown.
5. **ACI delete safety valve** — ACI inventory models are no longer auto-pruned.
   A failed APIC empties its fabric query, so a snapshot diff would emit a DELETE
   for every BD/L3Out/Pod/Node that "disappeared". The delete wave now holds back
   ACI deletes (warning the operator) unless `aci_allow_deletes` is set. ACI maps
   are new in 1.7.0, so no established auto-delete behavior is changed.
6. **Collection-gap growth/trend** — the health signal reads the last two
   `tag_forward_backfilled_devices` job snapshots (`total_backfilled` in
   `job.data`, no new storage) and escalates to `danger` with an "Up N / Down N
   since the previous reconciliation" note when the gap moves (the 18 -> 72 jump
   was invisible to a point-in-time count).
7. **Per-device collection result** — `ForwardDeviceAnalysis` gains a
   `collection_result` field (migration 0026) populated from the device-analysis
   NQE; the device panel now shows the specific failure token next to an
   unreachable device instead of a bare "No".

## Touched Surfaces

- `forward_netbox/utilities/scope_reconciliation.py` — NQE select, reason/stale
  helpers, new report keys.
- `forward_netbox/utilities/health.py` — collection-gap message wording.
- `forward_netbox/jobs.py` — `_maybe_enqueue_backfilled_tag_refresh` + hook.
- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`
  — breakdown + staleness cards.
- `forward_netbox/tests/test_device_scope_reconciliation_audit_command.py` —
  breakdown/staleness + back-compat tests.
- `forward_netbox/tests/test_jobs.py` — auto-tag enqueue gating test.
- `forward_netbox/utilities/sync_execution.py` — ACI delete safety valve.
- `forward_netbox/utilities/device_analysis.py` — store `collection_result`.
- `forward_netbox/models.py` + `forward_netbox/migrations/0026_*.py` —
  `ForwardDeviceAnalysis.collection_result` field.
- `forward_netbox/queries/forward_device_analysis.nqe` — emit the result token.
- `forward_netbox/templates/forward_netbox/inc/device_analysis_panel.html`,
  `forwardsync_health.html` — render the token and the danger badge.
- `forward_netbox/tests/test_sync_aci.py`, `test_health.py`,
  `test_scope_module_ui.py` — guard, trend, and per-device result tests.

## Approach

1. Extend the existing scope-reconciliation NQE select (proven live: the result
   string renders as `DeviceSnapshotResult.collectionFailed(DeviceCollectionError.X)`).
2. Parse the `DeviceCollectionError.X` token with a regex; derive whole-day
   staleness from `backfillTime`/`collectionTime` vs `timezone.now()`.
3. Aggregate over the backfilled set; expose breakdown + per-device sample.
4. Render in the panel; auto-include in the CLI JSON payload.
5. Add the opt-in auto-tag hook beside the device-analysis refresh hook.

## Validation

- `manage.py test forward_netbox.tests.test_device_scope_reconciliation_audit_command`
  — breakdown, staleness, and missing-reason back-compat all pass.
- `manage.py test forward_netbox.tests.test_jobs` — auto-tag enqueues only when
  the parameter is set.
- Adjacent suites green: `test_health`, `test_scope_module_ui`,
  `test_device_scope_tagging` (51 tests).
- Live CustomerOrg validation (local only): the exact NQE select returned the expected
  per-reason breakdown and parsed staleness for the in-scope backfilled devices.
- `pre-commit` (reorder/black/flake8) clean.

## Rollback

Revert this commit. The NQE select extension and report keys are additive; the
panel falls back to "No backfilled devices" when the new keys are absent, and the
auto-tag hook is a no-op unless `auto_tag_backfilled` is set.

## Decision Log

- **Reason fetched in the same NQE (no new API)** — `device.snapshotInfo.result`
  already carries the `DeviceCollectionError` token via `toString`, so no
  separate collection-status API call is needed.
- **No model/migration for the reason** — the breakdown is rendered live on the
  panel; persisting per-device reasons (for the cheap health-count path) is
  deferred to a follow-up.
- **Auto-tag is opt-in** — auto-running a live reconciliation query after every
  sync is a cost the operator should choose, matching `auto_refresh_device_analysis`.
- **ACI deletes opt-in (not fabric-aware)** — ACI delete rows carry only
  tenant/name, no fabric/APIC identity, so per-fabric filtering would mean
  retrofitting every delete function. A blanket opt-in (`aci_allow_deletes`,
  default off) eliminates the data-loss path with a single, well-tested gate and
  breaks nothing because ACI maps are new in 1.7.0. A future refinement could
  suppress only when a parent APIC is actually unhealthy.
- **Trend from job history (no new table)** — the tag job already records
  `total_backfilled`, so the last two completed runs give the growth signal for
  free; persisting a dedicated time series was unnecessary.
- **Follow-ups now included** — the (a) trend, (b) per-device result, and
  (c) ACI delete guard items originally deferred from the first cluster are all
  implemented here.
