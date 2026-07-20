# Feature: optional netbox-dlm integration + provision table preflight (2.5.0)

## Goal

Stop syncs from crashing when an installed plugin's migrations were never
applied (field report: `relation "public.netbox_dlm_contract" does not exist`
mid-provision), and add an optional integration for the netbox-dlm Device
Lifecycle Management plugin backed by Forward's end-of-life analysis.

## Constraints

- netbox-dlm is NOT installed in CI or the dev runtime; every surface must
  degrade gracefully when the plugin is absent (existing optional-plugin
  pattern) and the new maps ship disabled.
- New model strings require a ForwardNQEMap choices migration (0032) — the
  only schema change; no data migrations.
- NQE stays the source of truth: field-length clamping and identity shaping
  live in the bundled queries, not the plugin.
- Never commit validation-organization network identifiers or tokens.

## Touched Surfaces

- `forward_netbox/utilities/branching.py` (`missing_branch_table_report`),
  `single_branch_executor.py` (preflight), `health.py` (Database tables check)
- `forward_netbox/utilities/sync_dlm.py` (new adapter),
  `sync_runner_adapters.py` + `sync.py` (dispatch wiring)
- `forward_netbox/choices.py` (+ migration 0032), `query_registry.py`,
  `plugin_integrations/registry.py`, `apply_engine_decision.py`,
  `branch_budget.py`, `sync_contracts.py`, `sync_primitives.py`,
  `query_fetch_execution.py`, `sync_reporting.py`
- New queries: `forward_dlm_software_versions.nqe`,
  `forward_dlm_hardware_notices.nqe`, `forward_dlm_device_software.nqe`
- Tests: `test_provision_preflight.py`, `test_dlm_integration.py`

## Approach

1. Preflight: Django registers ContentTypes for every installed app even when
   its migrations never ran, so netbox_branching's provision runs
   `CREATE TABLE branch.T (LIKE public.T)` for a missing table and the sync
   dies with an opaque ProgrammingError. `missing_branch_table_report()` diffs
   `get_tables_to_replicate()` against `connection.introspection.table_names()`
   (the exact list provision replicates), maps missing tables to app labels,
   and the executor raises an actionable SyncError before the fetch. A
   fail-severity **Database tables** Health check surfaces the same report.
2. netbox-dlm integration (three opt-in maps, following the
   netbox_routing/ACI optional-plugin pattern end to end): SoftwareVersion
   from `device.platform.osSupport` per (platform, osVersion); HardwareNotice
   from chassis `device.platform.components[].support` per device type;
   DeviceSoftware one row per device (the adapter ensures the referenced
   SoftwareVersion exists without overwriting announced dates). Registry
   integration (`DLM_INTEGRATION`, adapter contract on
   `forward_netbox.utilities.sync_dlm`), decision/budget/contract/identity
   tables, and runner dispatch are wired for all three model strings.
3. Fixed a latent import-order bug the new models exposed:
   `_fallback_bucket_key_family` was defined after the module-level shard
   contract build in `branch_budget.py` and NameError'd for any supported
   model without a structured shard contract.

## Validation

Live against the validation network: 35 software-version rows (ISO dates,
announcement URLs), 48 hardware-notice rows, 4960 device-software rows; all
three bundled queries lint clean and run via raw NQE. Unit tests cover the
preflight (report, executor SyncError, Health check pass/fail/soft-fail) and
the DLM wiring (choices/registry/maps disabled-by-default/adapter contract/
runner dispatch/query structure). Full suite + lint + harness green.

## Rollback

Revert the branch and roll back migration 0032 (choices-only alter). No data
migrations; the new maps are seeded disabled and can be deleted.

## Decision Log

- Ship the DLM maps disabled: the plugin is alpha (0.1.0), ships no
  migrations, and the integration is a beta surface — operators opt in per map.
- DeviceSoftware ensures a bare SoftwareVersion when the vendor has no EOL
  announcement (create-only, never overwriting dates from the versions map),
  so every device links even when Forward lacks announcement data.
- Forward exposes no end-of-sale date, so HardwareNotice.end_of_sale stays
  null; end_of_support/lastSupportDate, end_of_security_patches/
  lastVulnerabilityDate, end_of_sw_releases/lastMaintenanceDate.
- The preflight lives in the executor (not provision monkeypatching) and the
  Health check never raises — diagnostics failures return None rather than
  breaking the page.
