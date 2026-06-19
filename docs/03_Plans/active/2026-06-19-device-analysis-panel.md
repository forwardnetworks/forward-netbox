# Device Analysis Panel (reachability / blast radius / CVE)

## Goal

Surface GA Forward per-device operational signals into NetBox as a read-only
device-detail panel: a reachability proxy, a connectivity-degree "blast radius",
CVE exposure, and up-interface count.

## Constraints

- GA NQE only. Forward Predict (paid, not GA) is excluded. Full path-based
  reachability/blast radius come from Forward's path/Predict APIs, which are not
  in the device NQE schema; this surfaces NQE-derivable proxies.
- Read-only overlay — not a Branching-managed sync model. No live Forward call on
  page render.
- No customer data in repo/tests.

## Touched Surfaces

- `forward_netbox/queries/forward_device_analysis.nqe` — device-parallel NQE
  (lint-clean) returning name, reachable, blast_radius, cve_count, up_interfaces,
  detail.
- `forward_netbox/models.py` + `migrations/0024_forwarddeviceanalysis.py` —
  `ForwardDeviceAnalysis` (sync, device_name, metrics, unique per sync+device).
- `forward_netbox/utilities/device_analysis.py` — run the NQE and upsert rows
  (only for devices present in NetBox; prune stale).
- `forward_netbox/jobs.py` — `refresh_forward_device_analysis` background job.
- `forward_netbox/views.py` — `ForwardSyncRefreshDeviceAnalysisView` (POST
  enqueues) + sync-detail Refresh Device Analysis button URL.
- `forward_netbox/template_content.py` + `templates/.../inc/device_analysis_panel.html`
  — device-detail panel rendering the stored analysis.
- Tests in `test_scope_module_ui`.

## Approach

`blast_radius` is the count of physical links on the device (connectivity degree
— a proxy for how many links/neighbors a device failure touches). `reachable` is
whether the device was freshly collected this snapshot. `cve_count` is confirmed-
vulnerable CVE findings. The NQE is validated with the NQE linter (exit 0) but
field bindings can vary by Forward release, so they are re-validatable. A Refresh
Device Analysis action runs the NQE as a background job and upserts
`ForwardDeviceAnalysis` rows; the device panel renders the latest stored row with
no live call. Rows for devices not in NetBox are skipped; stale rows are removed.

## Validation

- `forward_netbox.tests.test_scope_module_ui.ScopeModuleUiTest.test_refresh_device_analysis_job`
  (job upserts, NetBox-only filter, metric mapping).
- `manage.py check` (plugin + template extension load).
- NQE linter clean on the query file.
- Full suite; local CI mirror.

## Rollback

Drop the model + migration, the utility/job/view/panel, the NQE file, and the
sync-detail button. No effect on the Branching sync path.

## Decision Log

- Read-only overlay model over custom fields: the analysis is multi-field and
  refreshed out-of-band; a dedicated model + panel is cleaner than several custom
  fields and keeps it off the Branching sync path.
- Connectivity-degree as the blast-radius proxy: true path-based blast radius
  needs Forward path/Predict APIs not exposed in device NQE; degree is the honest
  GA-NQE approximation, clearly labeled in the query and panel.
