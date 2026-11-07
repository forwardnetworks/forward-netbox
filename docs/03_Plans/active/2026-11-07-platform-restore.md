# Operator Restore of Platforms a Sync Run Blanked

## Goal

An operator can see, and then restore, the platform that a pre-3.0.14 empty-scope
sync run cleared on devices. `2026-11-07-empty-scope-and-shared-catalogue-deletes.md`
stops new ones; this repairs the devices it already blanked and that no later run
revisits (devices outside the sync's scope). Reachable from the UI (Scope
Reconciliation), because customers have no CLI
(`2026-09-10-no-operator-cli-troubleshooting.md`).

## Constraints

- Preview first: the preview is read-only, makes no Forward call, and stores
  counts only. The restore recomputes the plan and restores no more than the
  preview the operator saw.
- A device is restorable only if NetBox's change log proves it: the latest change
  to its platform is an update made by one of this sync's own ingestions that took
  a platform away, the platform is still blank, nothing touched the platform
  since, and the removed platform can be found again (by pk, or by slug after a
  delete and recreate; exactly one match). Anything else is held with a reason.
  The decision logic is `platform_restore.plan_platform_restore`
  (`test_platform_restore`); this plan covers the operator surface.
- The restore is refused without a completed preview, when a run finalized after
  the preview, and while a sync run is active. Change-logged under the invoking
  user.
- Every UI figure, its split and when it was computed are in the support bundle
  (standing rule).
- No customer names, hostnames, addresses or IDs anywhere.

## Touched Surfaces

- `forward_netbox/utilities/platform_restore.py`: `latest_preview_job`,
  `reviewed_preview`, `run_platform_restore`, `REFUSED_PREVIEW_STALE` (the planning
  and restore core already existed).
- `forward_netbox/utilities/sync_facade.py`: two `BUTTON_JOB_SPECS` kinds
  (`preview_platform_restore`, `restore_platforms` with `dcim.change_device`); the
  restore joins the kinds blocked by an active sync run.
- `forward_netbox/jobs.py`: `_preview_platform_restore_work`,
  `_restore_platforms_work`, `PreviewPlatformRestoreJob`, `RestorePlatformsJob`.
- `forward_netbox/api/views.py`: `preview-platform-restore`, `restore-platforms`
  (REST parity, enforced by `test_button_jobs`).
- `forward_netbox/views.py`: `_platform_restore_payload`, `PLATFORM_RESTORE_FIGURES`,
  the card context, the bundle section `scope_reconciliation.platform_restore`
  (with `ui_figures` under the card's labels), two POST views (the restore view
  refuses on the page when there is no current preview), the checklist argument.
- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`:
  the "Platforms a Sync Run Blanked" card.
- `forward_netbox/utilities/cleanup_checklist.py`: item `platform_blanked_by_sync`,
  shown only when the stored preview says restorable > 0 (also in the bundle's
  `diagnostics.cleanup_checklist` via `bundle_diagnostics.py`).
- `forward_netbox/utilities/bundle_label_index.py`: card -> bundle paths.
- `forward_netbox/tests/test_platform_restore_wiring.py` (new),
  `test_cleanup_checklist.py`, `test_button_jobs.py` (work-function registry).
- `docs/01_User_Guide/operations.md`: "Platforms a sync run blanked".

## Approach

1. The preview job stores `preview_platform_restore(sync)` (blanked, restorable,
   held by reason, `generated_at`) as its data.
2. The card reads the stored job only; a page render never recomputes. It shows
   the figures under their labels, or one quiet line when nothing was blanked, and
   offers the restore only while the preview is current and restorable > 0.
3. The restore view checks for a reviewed preview before queuing; the job checks
   again (`reviewed_preview`), passes the preview's restorable count to
   `restore_platforms`, and runs under `_resolve_request_user`.
4. The "What to do" item appears only when the stored preview says restorable > 0;
   its steps are Preview, then Restore on this page.

## Validation

- `invoke test-isolated --test-label forward_netbox.tests.test_platform_restore_wiring`
- `invoke test-isolated --test-label forward_netbox.tests.test_platform_restore`
- `invoke test-isolated --test-label forward_netbox.tests.test_cleanup_checklist`
- `invoke test-isolated --test-label forward_netbox.tests.test_button_jobs`
- `invoke test-isolated --test-label forward_netbox.tests.test_bundle_label_index`
- `invoke harness-check`, `pre-commit run --files <changed files>`
- Live, before release (standing rule): on the customer-shaped stack, blank the
  platform on devices through an empty-scope run on a 3.0.13 build, upgrade, then
  preview and restore through the UI and confirm the restored count equals the
  previewed one and the Device change log shows the invoking user.

Not yet run (written without access to the shared test stack): every test in
`test_platform_restore_wiring` and the new cases in `test_cleanup_checklist`.

## Rollback

Revert the commit. No migration; only job data is stored. A restore already run is
in NetBox's change log (each edit under the invoking user) and can be reverted
device by device from there.

## Decision Log

- Operator-triggered, not automatic: restoring a field on devices outside the
  sync's scope is an edit the sync would not make on its own, and the proof (the
  change log) only reaches as far as NetBox retains it.
- Restore view refuses on the page rather than queuing a job that would fail: a
  message beside the buttons beats a failed job the operator has to open.
- Permission for the restore is `dcim.change_device` (it edits devices), and for
  the preview the sync-run permission, matching the stale-row preview.
- The card stays visible with only a Preview button when there is no preview,
  because the "What to do" item can appear only after a preview has run.
