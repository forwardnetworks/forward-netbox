# Forward NetBox Documentation

`forward_netbox` connects NetBox directly to Forward, executes NQE against a selected Forward snapshot, and stages the resulting changes in a NetBox branch for review and merge by default. Large trusted baselines can optionally use fast bootstrap direct writes before returning to the Branching workflow.

Forward 26.6 is the baseline for async NQE.

## Release Compatibility

| Plugin Release | NetBox Version | Status |
| --- | --- | --- |
| `v1.5.8` | `4.5.9` and `4.6.2` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Current release; `dcim.module` sync now **adopts** the device interfaces Forward already syncs instead of recreating them (fixes `dcim_interface_unique_device_name` IntegrityError when modules are enabled), and `ipam.fhrpgroup` no longer churns (delete+recreate the same HSRP groups every sync) — the snapshot diff no longer deletes a group it is simultaneously upserting. Preview Dependencies now runs as a background job (cached result on the preview page), fixing a 504 timeout on large fabrics. |
| `v1.5.7` | `4.5.9` and `4.6.2` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.5.8`; **Prune orphans** and **Create missing module bays** now run as background jobs (watch the Jobs tab) instead of synchronously, fixing a 504 gateway timeout on large fabrics. Module Readiness `Ready` reflects missing bays only (out-of-scope-device rows no longer hold it `No`), and the bulk `ipam.ipaddress` path tolerates duplicate global IPs. |
| `v1.5.6` | `4.5.9` and `4.6.2` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.5.7`; fixes an `ipam.ipaddress` sync failure (`Ambiguous coalesce lookup`) when a reused /30 link range leaves duplicate global (VRF-less) IPs for the same host — the adapter now resolves to one deterministically (preferring the copy already on the synced interface) and warns, instead of failing the row. |
| `v1.5.5` | `4.5.9` and `4.6.2` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.5.6`; surfaces the orphan-prune and module-bay readiness workflows in the sync detail UI (no CLI or CSV): a **Scope Reconciliation** page with a **Prune orphans** button, and a **Module Readiness** page with a **Create missing module bays** button that creates the bays directly in NetBox. |
| `v1.5.4` | `4.5.9` and `4.6.2` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.5.5`; adds `--prune-orphans`/`--apply` to `forward_device_scope_reconciliation_audit` to delete stale out-of-scope (orphan) NetBox devices left by an earlier broader sync that `device_tag_prune_out_of_scope` cannot reach (orphans are absent from the scoped Forward result). Dry-run by default; tagged-but-backfilled devices are preserved. |
| `v1.5.3` | `4.5.9` and `4.6.2` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.5.4`; classifies APIC controllers onto the `APIC` platform (distinct from ACI switches) so controller and switch software versions model separately; splits IP address import into independent `Forward IPv4 IP Addresses` and `Forward IPv6 IP Addresses` maps (a migration removes the combined map) so address families toggle independently; promotes `dcim.interface` and `ipam.ipaddress` into the default bulk-ORM safe set and removes bulk-apply update churn across every bulk model so steady-state syncs issue no redundant writes; preserves operator platform-manufacturer overrides on bulk update; and adds the opt-in `Apply Device Scope Tags` source option plus the `forward_device_scope_reconciliation_audit` and `forward_apic_cimc_readiness_audit` commands. |
| `v1.5.2` | `4.5.9` and `4.6.2` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.5.3`; collapses the flood of `dcim.modulebay` branch-merge failures (a NetBox Branching/MPTT limitation when a new device's module bays are auto-instantiated in a branch) into a single actionable `ModuleBayMergeUnsupported` ingestion issue that points at the `forward_module_readiness` import workflow. Device and interface sync are unaffected. |
| `v1.5.1` | `4.5.9` and `4.6.2` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.5.2`; adds the `latestCollected` snapshot selector that skips backfilled (collection-canceled) snapshots and resolves to the most recent snapshot with a freshly-collected in-scope device, warns when a `latestProcessed` run finds every in-scope device backfilled instead of silently applying zero changes, records the resolved snapshot's own metadata for `latestCollected` runs, and adds an Architecture Flow reference doc. |
| `v1.5.0.1` | `4.5.9` and `4.6.2` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.5.1`; fixes platform NQE query using `normalizePlatformName` to avoid evaluation failures on unsupported vendor/OS combinations, adds `--overwrite` flag to the validation-org repair command, and hardens the NQE org-publish commit loop to retry after 409 INVALID_CHANGE_PATH. |
| `v1.5.0` | `4.5.9` and `4.6.2` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.5.0.1`; hardens ingest throughput via adaptive async-NQE poll backoff, ndjson streaming, webhook/event-rule signal suppression during the apply loop, targeted validation (skips DB-hitting uniqueness checks on existing objects in both simple and tree-model bulk paths), and async advanced-reachability trigger (FWD-53559). Full test suite green on NetBox 4.5.9 (1092/0/0) and 4.6.2 (1092/0/26 routing-plugin version-gated). |
| `v1.4.3` | `4.5.9` and `4.6.2` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.5.0`; hardens query-path provenance by requiring source-backed query-id repair at preflight, enforces async NQE source parsing for 26.6 execution paths, proves CIMC/APIC custom-command updates in source and keeps the 1.4 production-hardening line intact. |
| `v1.4.2` | `4.5.9` and `4.6.2` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.4.3`; adds CIMC platform separation, visible query-drift repair and dependency preview on the sync detail page, and keeps the module-bay merge hardening plus parent-interface description preservation from the prior patch line. |
| `v1.4.1.1` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.4.2`; prevents optional `dcim.module` sync from emitting merge-breaking `dcim.modulebay` side-effect creates when module bays are missing and prevents LAG member rows from clearing existing parent interface descriptions. |
| `v1.4.1` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.4.2`; keeps the hard parent-device sync contract, adds query-ID drift remediation plus support-bundle diagnostics, and carries the 1.4 production-hardening tranche forward as the release line. |
| `v1.4.0` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.4.1`; enforces a hard parent-device sync contract so child models cannot run without `dcim.device`, which prevents stale sync configs from skipping the device shard and breaking dependent imports. |
| `v1.3.5.5` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.3.6`; adds compressed support-bundle ZIP downloads with optional password protection, and folds live source health, live query-drift, and live data-file diagnostics into the troubleshooting bundle so operator support can work from one artifact. |
| `v1.3.5.4` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.3.5.5`; repackaged the `1.3.5.3` query-contract hardening on a fresh patch tag and kept strict shipped-query parameter-contract validation, legacy tag alias stripping, and summary-only support-bundle previews. |
| `v1.3.5.3` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.3.5.4`; keeps the `1.3.5.2` claimed-step and payload compaction behavior, adds strict shipped-query parameter-contract validation, strips legacy tag aliases from runtime NQE payloads, and keeps support-bundle previews summary-only. |
| `v1.3.5.1` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.3.5.2`; removes raw `model_results` from the sync telemetry summary and prevents unparameterized query IDs from receiving source-level tag parameters, which keeps the sync detail view responsive and preserves the saved-query-ID path compatibility. |
| `v1.3.5` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.3.5.2`; keeps the 1.3.x saved-query-ID path parameter-compatible, tightens ACI platform detection with command-inventory signals, and preserves the lower-noise execution accounting used by the 1.3.x sync path |
| `v1.3.4` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.3.5`; makes non-retryable Branching merge failures visible in job logs, leaves failed merge branches in a terminal `Failed` state instead of stale `Merging`, and carries disabled async NQE client staging for future Forward 26.6 support |
| `v1.3.3` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.3.5.2`; refreshes bundled NQE syntax for saved query-ID execution, keeps all shipped maps parameter-compatible with `forward_netbox_shard_keys`, and updates the saved validation-folder query IDs used by the 1.3.x sync path |
| `v1.3.2` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.3.3`; adds optional `netbox-cisco-aci` integration maps and adapter support, keeps ACI maps disabled by default, preserves parameterized NQE execution, and validates repeat-sync idempotence for the proven ACI write path |
| `v1.3.1` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.3.2`; preserves the `v1.3.0` parameterized NQE path, removes the legacy sync column-filter shard path, and fixes repeat prefix sync accounting so unchanged `ipam.prefix` rows report as unchanged instead of update churn |
| `v1.3.0` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.3.1`; eliminates default Forward NQE column-filter shard fetches in favor of query-side `forward_netbox_shard_keys` parameters, keeps local shard safety filtering, and preserves branch boundaries while reducing Forward SaaS API/NQE pressure |
| `v1.2.3` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.3.0`; further reduced Forward SaaS API/NQE pressure by coalescing compatible sibling shard EQUALS_ANY filters, added local change-explainability summaries, and kept staged branch boundaries unchanged |
| `v1.2.1` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.2.2`; fixes prefix VRF churn by making `ipam.prefix` identity exact for global and VRF-scoped rows while preserving parameterized prefix shard NQE execution |
| `v1.2.0` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.2.1`; adds optional NetBox-native HSRP/VRRP FHRP import, bounded access/native interface VLAN assignment, upgrade-safe FHRP VIP conflict handling, and NetBox 4.6 job-test compatibility hardening while preserving the 1.1 API/NQE limits |
| `v1.1.1` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.2.0`; adds optional NetBox-native HSRP/VRRP FHRP import, bounded access/native interface VLAN assignment, upgrade-safe FHRP VIP conflict handling, and NetBox 4.6 job-test compatibility hardening while preserving the 1.1 API/NQE limits |
| `v1.1.0` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.1.1`; reduces Forward SaaS API/NQE pressure with source-level API pacing, parameterized prefix shard queries, single-pass interface NQE, and release-validation smoke evidence |
| `v1.0.0` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.1.0`; first 1.x release line with API/NQE stability groundwork but without 1.1 API pacing and scale-optimized query improvements |
| `v0.9.4.6` | `4.5.9` and `4.6.1` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v1.1.0`; tightens delete-heavy device cleanup shard planning after live evidence showed device deletes still exceeded native Branching change-budget guidance |
| `v0.9.4.5` | `4.5.9` and `4.6.0` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v0.9.4.6`; plans delete-heavy device cleanup shards more conservatively so tag-scope prune runs stay closer to native Branching change-budget guidance |
| `v0.9.4.4` | `4.5.9` and `4.6.0` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v0.9.4.5`; clarifies large branching progress by clamping progress-bar display and surfacing current shard row progress in the ingestion UI |
| `v0.9.4.3` | `4.5.9` and `4.6.0` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v0.9.4.4`; hardens delete behavior by converting protected-reference delete failures into dependency skips so tag-scope prune/device cleanup runs continue safely |
| `v0.9.4.1.1` | `4.5.9` and `4.6.0` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v0.9.4.3`; keeps the shared-branch architecture, execution ledger, support logging, and scale hardening while preserving the read-only advisory surfaces from `v0.9.0` |
| `v0.9.0` | `4.5.9` and `4.6.0` validated; shared branch for `4.5.x` and `4.6.x` with capability-gated 4.6 features | Superseded by `v0.9.4.1.1`; adds read-only analysis, workload preview, advisory summaries, and native log export for troubleshooting while keeping lifecycle enrichment and predict deferred |
| `v0.8.6.3` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.9.4.1.1`; hardens beta routing scope resolution, invalid ASN filtering, conservative virtual chassis skips, and fast-bootstrap baseline readiness when only optional model issues remain |
| `v0.8.6.2` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.8.6.3`; hardens issue and job-log rendering so unexpected nested payload objects stay JSON-safe in the UI and API |
| `v0.8.6.1` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.8.6.2`; clarifies the native NQE map bulk edit workflow so repository-path mode and runtime query-ID resolution are explicit in the UI |
| `v0.8.6` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.8.6.1`; refreshes org-repository query publishing with flattened built-ins, filters invalid IPv4 prefix artifacts, adds parent-prefix diagnostics, and hardens virtual chassis/device and routing issue handling |
| `v0.8.5` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.8.6`; makes the beta routing and module maps broadly available by default while keeping virtual chassis conservative, hardens repository query lookup responses, and clears stale row progress when a sync fails or advances phases |
| `v0.8.4` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.8.5`; stops importing Forward HA peers as NetBox virtual chassis by default, hardens repository query lookup responses, and clears stale row progress when a sync fails or advances phases |
| `v0.8.3` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.8.4`; isolates per-model query failures, blocks positionless virtual-chassis assignments before NetBox save, and lets later shards such as routing continue while withholding dirty diff baselines |
| `v0.8.2` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.8.3`; adds portable repository query-path execution with native NetBox selectors, publish-and-bind bulk edit, bidirectional restore, and fixes IP address rows whose Forward interface cannot be resolved |
| `v0.8.1.1` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.8.2`; fixes virtual chassis NQE output so NetBox receives a member position with virtual chassis assignments |
| `v0.8.1` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.8.1.1`; fixes fast-bootstrap native change tracking/statistics and adds timeout guidance plus transient Forward API HTTP retries |
| `v0.8.0` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.8.1`; adds an opt-in fast bootstrap backend for trusted large baselines while keeping Branching as default, and skips NetBox-invalid LAG cable endpoints |
| `v0.7.1` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.8.0`; keeps the NetBox-native multi-branch workflow, adds shard heartbeat visibility, and hardens large-shard retries and cable ingestion handling |
| `v0.7.0` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.7.1`; extracts the 0.7 sync boundaries and adds shard heartbeat visibility |
| `v0.6.5` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.7.0`; adds audited validation force-allow overrides and routing evidence enrichment; optional routing/peering import remains beta; native `dcim.module` import is beta |
| `v0.6.4` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.6.5`; optional routing/peering import is beta; native `dcim.module` import is beta |
| `v0.6.3` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.6.4`; native `dcim.module` import is beta |
| `v0.6.2` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.6.3`; native `dcim.module` import is beta |
| `v0.6.1` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.6.2`; native `dcim.module` import is beta |
| `v0.6.0` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.6.1`; native `dcim.module` import is beta |
| `v0.5.9.1` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.6.0` |
| `v0.5.9` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.5.9.1` |
| `v0.5.8` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.5.9` |
| `v0.5.7` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.5.8` |
| `v0.5.2.1` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.5.3` |
| `v0.4.0` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.5.2.1` |
| `v0.3.1` | `4.5.8` validated; `4.5.x` only | Superseded by `v0.4.0` |
| `v0.3.0.1` | `4.5.8` validated; `4.5.x` only | Superseded by `v0.3.1` |
| `v0.3.0` | `4.5.8` validated; `4.5.x` only | Superseded by `v0.3.0.1` |

## Version History

| Release | Summary |
| --- | --- |
| `v1.4.3` | Hardens query provenance with source-backed query-id repair during preflight, adds async execution path readiness for 26.6 with resilient NDJSON/JSONL result handling, and proves CIMC/APIC platform/type updates from command-source evidence while preserving prior 1.4 protections. |
| `v1.4.2` | Adds CIMC platform separation, visible query-drift repair and dependency preview on the sync detail page, and keeps module-bay merge hardening plus parent-interface description preservation. |
| `v1.4.1.1` | Prevents optional module sync from creating `dcim.modulebay` side-effect changes during merge, skips missing module bays with a readiness/import warning, and keeps LAG member rows from clearing existing parent interface descriptions. |
| `v1.4.1` | Publishes the 1.4 patch release line with query-ID drift remediation and support-bundle diagnostics on top of the parent-device contract and release surfaces. |
| `v1.3.6` | Enforces a hard parent-device sync contract so child models cannot run without `dcim.device`, preventing stale sync configs from skipping the device shard and breaking dependent imports. |
| `v1.3.5.5` | Adds compressed support-bundle ZIP downloads with optional password protection and folds live source/query-drift/data-file diagnostics into the troubleshooting bundle. |
| `v1.3.5.4` | Republishes the `v1.3.5.3` query-contract hardening as the prior patch release. |
| `v1.3.5.3` | Preserves the claimed-step and payload-compaction protections from `v1.3.5.2`, adds strict shipped-query parameter-contract validation, strips legacy tag aliases from runtime NQE payloads, and keeps support-bundle previews summary-only. |
| `v1.3.5.2` | Prevents shard execution drift by tying resume/overlap workers to the claimed execution step and compacts sync execution payloads (`plan_items`, workload previews, and advisory branch summaries) so large runs remain user-visible but lightweight in UI/log exports. |
| `v1.3.5.1` | Removes the raw `model_results` payload from sync telemetry summaries and stops unparameterized query IDs from inheriting source-level tag parameters, which keeps sync-detail rendering responsive while preserving the saved-query-ID path. |
| `v1.3.5` | Tightens the saved-query-ID path for 1.3.x, keeps shipped maps parameter-compatible with `forward_netbox_shard_keys`, and uses command-inventory signals to avoid misclassifying ACI platforms during repeat syncs. |
| `v1.3.4` | Makes non-retryable Branching merge errors operator-visible by persisting the failure reason before the job terminates, marks branches still stuck in `Merging` as `Failed`, preserves timeout/transient retry behavior, and adds disabled-by-default async NQE client support for the future Forward 26.6 execution API. |
| `v1.3.3` | Refreshes shipped NQE syntax so saved query IDs accept the standard `forward_netbox_shard_keys` parameter payload, republishes the saved validation-folder query set, and validates the affected saved query IDs against a live Forward SaaS dataset. |
| `v1.3.2` | Adds optional `netbox-cisco-aci` plugin support with disabled-by-default ACI fabric, pod, node, tenant, VRF, and filter maps; keeps deeper ACI policy maps conservative until source identity is proven; and hardens duplicate ACI node observations so repeat syncs remain no-op when source data is unchanged. |
| `v1.3.1` | Fixes repeat prefix sync accounting by treating unchanged NetBox `ipam.prefix` rows as unchanged, avoids dependency VRF metadata rewrites from prefix/IP/FHRP imports, removes the legacy sync column-filter shard path, and preserves the `v1.3.0` parameterized NQE path. |
| `v1.3.0` | Eliminates default Forward NQE column-filter shard fetches in favor of query-side `forward_netbox_shard_keys` parameters, keeps local shard safety filtering, and preserves branch boundaries while reducing Forward SaaS API/NQE pressure. |
| `v1.2.3` | Coalesces compatible sibling shard column-filter fetches into bounded EQUALS_ANY requests, caches prefetched sibling rows locally to avoid repeated Forward calls, and surfaces local Branching change-explainability summaries in support bundles and the ingestion UI. |
| `v1.2.2` | Adds operator-visible Forward API usage budgets/rate evidence in Sync Health and support bundles, extends repeat-sync no-op hardening across key adapters, and persists successful staged deletes into ingestion statistics so active delete shards are visible before merge accounting catches up. |
| `v1.2.1` | Fixes repeat prefix sync churn where otherwise unchanged `ipam.prefix` rows could be updated only because the VRF foreign key was re-resolved; built-in prefix maps now use exact `prefix + vrf` identity while prefix shard fetches still use parameterized NQE. |
| `v1.2.0` | Adds optional NetBox-native HSRP/VRRP FHRP import from Forward native FHRP state, bounded access/native interface VLAN assignment from existing site-scoped VLANs, keeps FHRP upgrade behavior safe for existing 1.1 IPAM data, and hardens current NetBox job-test compatibility. |
| `v1.1.1` | Adds optional NetBox-native HSRP/VRRP FHRP import from Forward native FHRP state, bounded access/native interface VLAN assignment from existing site-scoped VLANs, keeps FHRP upgrade behavior safe for existing 1.1 IPAM data, and hardens current NetBox job-test compatibility. |
| `v1.1.0` | Adds Forward SaaS API request pacing, parameterized prefix shard execution, single-pass interface NQE, configured max-shard persistence in smoke evidence, and release-validation gates for API/NQE scale validation. |
| `v1.0.0` | Introduced the first `v1.0.0` release line and initial API/NQE release validation flow before the 1.1 runtime and request-pacing enhancements. |
| `v0.9.4.6` | Tightens delete-heavy `dcim.device` cleanup planning to about 500 planned delete rows per 10k branch-change budget after live shard evidence showed the earlier estimate was still too high. |
| `v0.9.4.5` | Plans delete-heavy `dcim.device` cleanup shards with a conservative row budget so tag-scope prune runs do not pack thousands of cascading device deletes into one Branching shard. |
| `v0.9.4.4` | Clarifies large branching progress by clamping utilization display to 100% during intermediate accounting overshoots and showing current shard row progress in the ingestion UI. |
| `v0.9.4.3` | Converts NetBox protected-reference delete failures into dependency skips so large tag-scope prune/device cleanup runs continue without shard-failing delete rows. |
| `v0.9.4.1.1` | Keeps the shared 4.5/4.6 branch line, execution ledger, support logging, and scale hardening while preserving the read-only advisory surfaces from `v0.9.0`. |
| `v0.9.0` | Adds read-only analysis, workload preview, advisory summaries, and native log export for troubleshooting while keeping lifecycle enrichment and predict deferred. |
| `v0.8.6.3` | Hardens beta routing scope resolution, filters invalid BGP ASN rows in shipped NQE, skips positionless virtual-chassis rows conservatively, and lets fast bootstrap retain its diff baseline when only optional model issues remain. |
| `v0.8.6.2` | Hardens issue and job-log rendering for JSON safety when unexpected nested payload objects leak into failure data. |
| `v0.8.6.1` | Clarifies NQE map bulk edit labels and help text so operators use repository query paths and understand query IDs are resolved automatically during sync and diff execution. |
| `v0.8.6` | Publishes flattened built-in NQE to Forward org repositories, filters IPv4 host/any/loopback prefix artifacts, reports IP rows missing imported parent prefixes on full baselines, and prevents stale virtual-chassis/device/routing row failures from blocking unrelated models. |
| `v0.8.5` | Makes the beta routing and module maps broadly available by default, preserves the conservative bundled virtual chassis map, handles Forward repository query lookups that return a `queries` list, and makes failed sync activity show the terminal failure instead of stale row heartbeat text. |
| `v0.8.3` | Isolates stale or invalid per-model query output, rejects virtual-chassis rows missing `vc_position`, records row issues without aborting later multi-branch shards, and prevents dirty runs from becoming diff baselines. |
| `v0.8.2` | Adds portable repository `query_path` execution with Forward-backed selectors, true native bulk edit for publishing bundled NQE into the Forward Org Repository, binding selected maps to repository paths, restoring bundled raw NQE, and clearer skipping for IP address rows whose interface cannot be resolved. |
| `v0.8.1.1` | Emits `vc_position` in the built-in virtual chassis NQE map for vPC and MLAG memberships so NetBox does not reject virtual chassis device assignments without a member position. |
| `v0.8.1` | Runs fast bootstrap inside native NetBox change tracking, shows branchless ingestion changes from `ObjectChange` rows, updates fast-bootstrap counters from real object changes, warns about undersized worker timeouts, and retries transient Forward API HTTP timeouts/gateway responses. |
| `v0.8.0` | Adds an opt-in fast bootstrap direct-write backend for trusted large baselines, keeps NQE validation and row adapters shared with Branching, and skips LAG cable endpoints that NetBox cannot cable directly. |
| `v0.7.1` | Keeps the NetBox-native multi-branch workflow stable while hardening cable ingestion, retry handling, and shard re-planning for large runs. |
| `v0.7.0` | Splits the remaining sync orchestration, reporting, and validation helpers into dedicated boundaries, adds shard heartbeat visibility, and preserves the NetBox-native branch workflow. |
| `v0.6.5` | Adds audited validation force-allow overrides and routing evidence enrichment while reducing skipped routing rows through conservative NQE-side identity inference. |
| `v0.6.4` | Adds beta optional routing and peering imports for `netbox-routing` and `netbox-peering-manager`, including BGP peers, BGP address families, OSPF objects, peering sessions, routing diagnostics, and query-ID-aware built-in map handling. |
| `v0.6.3` | Models Forward aggregate interfaces as native NetBox LAGs, attaches member interfaces through `Interface.lag`, and keeps the MTU value sourced from Forward's normalized interface field. |
| `v0.6.2` | Canonicalizes duplicate global-table IP address rows by host IP before import and records row-scoped apply/delete failures as ingestion issues without aborting the rest of the shard. |
| `v0.6.1` | Filters interface IP rows that NetBox cannot assign, such as subnet network IDs and IPv4 broadcasts, while reporting aggregate diagnostics for filtered addresses. |
| `v0.6.0` | Adds beta native `dcim.module` import for chassis modules and similar bay-aware hardware, improves inventory item normalization, and avoids duplicate generic inventory rows when beta module sync is enabled. |
| `v0.5.9.1` | Keeps job logs visible during execution by persisting plugin log entries into the native NetBox job log tab while preserving the full plugin ingestion log view. |
| `v0.5.9` | Balances query preflight and workload fetch with bounded parallelism, reducing long planning pauses on large datasets. |
| `v0.5.8` | Defers event flushing until commit so large prefix ingestions do not trip transaction state changes mid-run. |
| `v0.5.5` | Applies a consistent model conflict policy for cable sync rows: skip occupied-interface conflicts, aggregate warning spam, and keep non-conflict updates/creates unchanged. |
| `v0.5.4` | Persists ingestion change counters so list/detail values stay consistent after branch merge cleanup, matching merge summaries. |
| `v0.5.3` | Surfaces preflight activity and elapsed phase timing on sync detail, emits early phase logs before ingestion rows, and sets source status to `Syncing` while runs are active. |
| `v0.5.2.1` | Fixes plugin admin version display and ships inferred cable query parser compatibility update (no `let` declarations) while preserving synthetic endpoint filtering. |
| `v0.4.0` | Corrects built-in IPv4/IPv6 prefix NQE filters to exclude host routes (`/32` and `/128`) from prefix import and validates the behavior against a live smoke dataset. |
| `v0.3.1` | Adds optional data-file-aware device type alias maps, a Device Type Library alias data-file builder, and documentation for the snapshot requirement while keeping the default no-data-file maps available. |
| `v0.3.0.1` | Fixes the validation-run list UI by removing unsupported edit actions from read-only validation records, and adds Playwright coverage for the validation-run list route. |
| `v0.3.0` | Adds the NetBox 4.5.8-validated harness architecture with first-class validation runs, drift policies, query-fetch boundaries, model-result reporting, and Playwright-covered UI workflow validation. |
| `v0.2.4` | Hardens native multi-branch resilience with adaptive shard splitting and retry on branch-budget overflow, plus model-density tracking to keep large initial syncs within NetBox branching guidance. |
| `v0.2.3` | Adds native sync preflight validation before full multi-branch planning so invalid model/query rows fail earlier in the UI/API run path. |
| `v0.2.2` | NQE-only correction release: filters zero-length prefixes, broadens interface coverage for IP assignment targets, and enforces inventory `part_id` length limits. |
| `v0.2.1` | Makes NetBox-native multi-branch execution the only UI/API sync path and exposes the branch budget in the sync form. |
| `v0.2.0` | Adds NetBox-native multi-branch baseline syncs for large datasets, uses NetBox outbound proxy routing for Forward API calls, and keeps branch event queues bounded during large imports. |
| `v0.1.6.0` | Adds explicit diff baselines, Forward `nqe-diffs` execution for eligible `query_id` maps, and updated large-dataset guidance for baseline versus incremental syncs. |
| `v0.1.5.1` | Patch release that validates and hardens null-VRF coalesce behavior and inventory-item serial bounds against the live dataset. |
| `v0.1.5` | Fixes null VRF coalesce handling, imports loopback interfaces for IP attachment, and hardens inventory-item identity fallbacks. |
| `v0.1.4.2` | CI/package patch release that applies repository formatting/import-order fixes and publishes a clean artifact line. |
| `v0.1.4.1` | Patch release that bounds built-in `dcim.virtualchassis` names and domains to NetBox limits. |
| `v0.1.4` | Hardened built-in NQE mappings and docs for large dataset syncs. |
| `v0.1.3` | Enforced deterministic model identity contracts across sync ingestion. |
| `v0.1.2` | Improved ingestion safety, diagnostics, and compatibility with existing NetBox data. |
| `v0.1.1` | Added NQE pagination, shared helper composition, and release hygiene updates. |
| `v0.1.0` | Initial unsupported release of the Forward-to-NetBox sync plugin. |

## Support Disclaimer

This repository is provided for use at your own risk. It is an unsupported release and is not an officially supported Forward product.

## What This Plugin Provides

- Direct Forward API connectivity with username and password authentication
- Dynamic network selection from the authenticated Forward tenant
- First-class `Sources`, `NQE Maps`, `Syncs`, and `Ingestions`
- Branch-backed review and merge flow through `netbox_branching`
- Optional fast bootstrap execution backend for trusted large baseline loads
- Built-in shipped NQE maps that can be used as-is or copied into custom map definitions
- Disabled alternate NQE maps for NetBox Device Type Library alias matching through a Forward JSON data file
- Disabled alternate NQE map for data-file-driven device feature tag rules
- Support for repository `query_path`, direct `query_id`, and raw `query` execution modes
- Snapshot selection per sync, including `latestProcessed`
- Snapshot details and Forward metrics recorded on each ingestion

## Start Here

- [Project Knowledge](00_Project_Knowledge/README.md)
- [Installation](01_User_Guide/README.md)
- [Configuration](01_User_Guide/configuration.md)
- [Usage and Validation](01_User_Guide/usage.md)
- [Troubleshooting](01_User_Guide/troubleshooting.md)
- [Architecture Flow](02_Reference/architecture-flow.md)
- [Built-In NQE Reference](02_Reference/built-in-nqe-maps.md)
- [Device Type Alias Data File](02_Reference/device-type-alias-data-file.md)
- [Model Mapping Matrix](02_Reference/model-mapping-matrix.md)
- [Active Implementation Plans](03_Plans/active/README.md)
- [Technical Debt Tracker](03_Plans/technical-debt.md)
- [Shipped NQE Query Files](https://github.com/forwardnetworks/forward-netbox/tree/main/forward_netbox/queries)

## Screenshot Set

Current UI screenshots are stored in `docs/images/` and are captured from the current plugin UI:

- `forward-sources.jpg`
- `forward-source.jpg`
- `forward-nqe-maps.jpg`
- `forward-nqe-map.jpg`
- `forward-sync-detail.jpg`
- `forward-ingestions.jpg`
- `forward-ingestion-detail.jpg`

These screenshots reflect the current snapshot-aware and native multi-branch workflow: source network on the source, snapshot selection and branch budget on the sync, and snapshot details on the ingestion. The NQE map list screenshot is filtered to built-in maps so published customer query IDs are not shown in documentation assets.

## Current Built-In Coverage

The shipped built-in NQE maps currently cover:

- Sites
- Manufacturers
- Device roles
- Platforms
- Device types
- Devices
- Virtual chassis
- Device feature tags
- Interfaces
- Cables from exact Forward inferred interface matches
- MAC addresses
- Inventory items
- Optional beta modules
- Optional beta BGP peers, BGP address families, OSPF objects, and peering sessions through external NetBox plugins
- VLANs
- VRFs
- IPv4 and IPv6 prefixes
- IP addresses
