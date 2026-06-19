# Changelog

Generated from the README compatibility table by `scripts/gen_changelog.py`. Do not edit by hand.

## v1.6.2 — 2026-06-19

completes the 1.6.1 line (1.6.1 was yanked — its PyPI build predated these): device tag scope now covers VLANs/VRFs and prefixes derive from connected interface subnets; the FHRP group churn (delete+recreate every sync) is fixed by identity-bucket sharding; device analysis is a first-class model with a fleet list view, REST API, and an Open in Forward deep-link.

## v1.6.1 — 2026-06-19

matures the 1.6.0 features and tooling — Device Analysis is now a NetBox model with a fleet-wide list view, REST API, and per-device-FK panel scoping (with up-interface blast-radius and opt-in post-sync refresh); adds a schedulable collection-gap alert command, run-history drill-down links, and hardened release tooling (one-command release script, generated CHANGELOG, conventional-commit hook).

## v1.6.0 — 2026-06-19

ships the blue-sky tranche — release automation (`invoke release`), an Operations Guide, a collection-gap health signal, a sync run-history panel, a read-only device analysis panel (GA reachability / connectivity-degree blast radius / CVE exposure), and a bidirectional per-model drift report.

## v1.5.10 — 2026-06-18

promotes `ipam.prefix` into the default bulk-ORM safe set (the last model still on the adapter path) — it runs the per-object tree apply so NetBox prefix hierarchy `_depth` stays correct, with null-VRF (global) prefix identity and canonical-CIDR matching parity-tested against the adapter.

## v1.5.9 — 2026-06-18

adds a maintained `forward-backfilled` NetBox tag so operators can see which in-scope devices were backfilled (not freshly collected) in the latest snapshot — a Tag backfilled devices button on the Scope Reconciliation page plus a link to the filtered device list (`?tag=forward-backfilled`); the tag self-heals as devices collect again.

## v1.5.8 — 2026-06-18

`dcim.module` sync now **adopts** the device interfaces Forward already syncs instead of recreating them (fixes `dcim_interface_unique_device_name` IntegrityError when modules are enabled), and `ipam.fhrpgroup` no longer churns (delete+recreate the same HSRP groups every sync) — the snapshot diff no longer deletes a group it is simultaneously upserting. Preview Dependencies now runs as a background job (cached result on the preview page), fixing a 504 timeout on large fabrics.

## v1.5.7 — 2026-06-18

**Prune orphans** and **Create missing module bays** now run as background jobs (watch the Jobs tab) instead of synchronously, fixing a 504 gateway timeout on large fabrics. Module Readiness `Ready` reflects missing bays only (out-of-scope-device rows no longer hold it `No`), and the bulk `ipam.ipaddress` path tolerates duplicate global IPs.

## v1.5.6 — 2026-06-18

fixes an `ipam.ipaddress` sync failure (`Ambiguous coalesce lookup`) when a reused /30 link range leaves duplicate global (VRF-less) IPs for the same host — the adapter now resolves to one deterministically (preferring the copy already on the synced interface) and warns, instead of failing the row.

## v1.5.5 — 2026-06-18

surfaces the orphan-prune and module-bay readiness workflows in the sync detail UI (no CLI or CSV): a **Scope Reconciliation** page with a **Prune orphans** button, and a **Module Readiness** page with a **Create missing module bays** button that creates the bays directly in NetBox.

## v1.5.4 — 2026-06-18

adds `--prune-orphans`/`--apply` to `forward_device_scope_reconciliation_audit` to delete stale out-of-scope (orphan) NetBox devices left by an earlier broader sync that `device_tag_prune_out_of_scope` cannot reach (orphans are absent from the scoped Forward result). Dry-run by default; tagged-but-backfilled devices are preserved.

## v1.5.3 — 2026-06-18

classifies APIC controllers onto the `APIC` platform (distinct from ACI switches) so controller and switch software versions model separately; splits IP address import into independent `Forward IPv4 IP Addresses` and `Forward IPv6 IP Addresses` maps (a migration removes the combined map) so address families toggle independently; promotes `dcim.interface` and `ipam.ipaddress` into the default bulk-ORM safe set and removes bulk-apply update churn across every bulk model so steady-state syncs issue no redundant writes; preserves operator platform-manufacturer overrides on bulk update; and adds the opt-in `Apply Device Scope Tags` source option plus the `forward_device_scope_reconciliation_audit` and `forward_apic_cimc_readiness_audit` commands.

## v1.5.2 — 2026-06-17

collapses the flood of `dcim.modulebay` branch-merge failures (a NetBox Branching/MPTT limitation when a new device's module bays are auto-instantiated in a branch) into a single actionable `ModuleBayMergeUnsupported` ingestion issue that points at the `forward_module_readiness` import workflow. Device and interface sync are unaffected.

## v1.5.1 — 2026-06-17

adds the `latestCollected` snapshot selector that skips backfilled (collection-canceled) snapshots and resolves to the most recent snapshot with a freshly-collected in-scope device, warns when a `latestProcessed` run finds every in-scope device backfilled instead of silently applying zero changes, records the resolved snapshot's own metadata for `latestCollected` runs, and adds an Architecture Flow reference doc.

## v1.5.0.1 — 2026-06-17

fixes platform NQE query using `normalizePlatformName` to avoid evaluation failures on unsupported vendor/OS combinations, adds `--overwrite` flag to the validation-org repair command, and hardens the NQE org-publish commit loop to retry after 409 INVALID_CHANGE_PATH.

## v1.5.0 — 2026-06-17

hardens ingest throughput via adaptive async-NQE poll backoff, ndjson streaming, webhook/event-rule signal suppression during the apply loop, targeted validation (skips DB-hitting uniqueness checks on existing objects in both simple and tree-model bulk paths), and async advanced-reachability trigger (FWD-53559). Full test suite green on NetBox 4.5.9 (1092/0/0) and 4.6.2 (1092/0/26 routing-plugin version-gated).

## v1.4.3 — 2026-06-13

hardens query-path provenance by requiring source-backed query-id repair at preflight, enforces async NQE source parsing for 26.6 execution paths, proves CIMC/APIC custom-command updates in source and keeps the 1.4 production-hardening line intact.

## v1.4.2 — 2026-06-12

adds CIMC platform separation, visible query-drift repair and dependency preview on the sync detail page, and keeps the module-bay merge hardening plus parent-interface description preservation from the prior patch line.

## v1.4.1.1 — 2026-06-11

prevents optional `dcim.module` sync from emitting merge-breaking `dcim.modulebay` side-effect creates when module bays are missing and prevents LAG member rows from clearing existing parent interface descriptions.

## v1.4.1 — 2026-06-11

keeps the hard parent-device sync contract, adds query-ID drift remediation plus support-bundle diagnostics, and carries the 1.4 production-hardening tranche forward as the release line.

## v1.4.0 — 2026-06-11

enforces a hard parent-device sync contract so child models cannot run without `dcim.device`, which prevents stale sync configs from skipping the device shard and breaking dependent imports.

## v1.3.5.5 — 2026-06-10

adds compressed support-bundle ZIP downloads with optional password protection, and folds live source health, live query-drift, and live data-file diagnostics into the troubleshooting bundle so operator support can work from one artifact.

## v1.3.5.4 — 2026-06-10

repackaged the `1.3.5.3` query-contract hardening on a fresh patch tag and kept strict shipped-query parameter-contract validation, legacy tag alias stripping, and summary-only support-bundle previews.

## v1.3.5.3 — 2026-06-10

keeps the `1.3.5.2` claimed-step and payload compaction behavior, adds strict shipped-query parameter-contract validation, strips legacy tag aliases from runtime NQE payloads, and keeps support-bundle previews summary-only.

## v1.3.5.1 — 2026-06-09

removes raw `model_results` from the sync telemetry summary and prevents unparameterized query IDs from receiving source-level tag parameters, which keeps the sync detail view responsive and preserves the saved-query-ID path compatibility.

## v1.3.5 — 2026-06-09

keeps the 1.3.x saved-query-ID path parameter-compatible, tightens ACI platform detection with command-inventory signals, and preserves the lower-noise execution accounting used by the 1.3.x sync path

## v1.3.4 — 2026-06-08

makes non-retryable Branching merge failures visible in job logs, leaves failed merge branches in a terminal `Failed` state instead of stale `Merging`, and carries disabled async NQE client staging for future Forward 26.6 support

## v1.3.3 — 2026-06-08

refreshes bundled NQE syntax for saved query-ID execution, keeps all shipped maps parameter-compatible with `forward_netbox_shard_keys`, and updates the saved validation-folder query IDs used by the 1.3.x sync path

## v1.3.2 — 2026-06-06

adds optional `netbox-cisco-aci` integration maps and adapter support, keeps ACI maps disabled by default, preserves parameterized NQE execution, and validates repeat-sync idempotence for the proven ACI write path

## v1.3.1 — 2026-06-05

preserves the `v1.3.0` parameterized NQE path, removes the legacy sync column-filter shard path, and fixes repeat prefix sync accounting so unchanged `ipam.prefix` rows report as unchanged instead of update churn

## v1.3.0 — 2026-06-05

eliminates default Forward NQE column-filter shard fetches in favor of query-side `forward_netbox_shard_keys` parameters, keeps local shard safety filtering, and preserves branch boundaries while reducing Forward SaaS API/NQE pressure

## v1.2.3 — 2026-06-04

further reduced Forward SaaS API/NQE pressure by coalescing compatible sibling shard EQUALS_ANY filters, added local change-explainability summaries, and kept staged branch boundaries unchanged

## v1.2.1 — 2026-06-03

fixes prefix VRF churn by making `ipam.prefix` identity exact for global and VRF-scoped rows while preserving parameterized prefix shard NQE execution

## v1.2.0 — 2026-06-03

adds optional NetBox-native HSRP/VRRP FHRP import, bounded access/native interface VLAN assignment from existing site-scoped VLANs, upgrade-safe FHRP VIP conflict handling, and NetBox 4.6 job-test compatibility hardening while preserving the 1.1 API/NQE limits

## v1.1.1 — 2026-06-03

adds optional NetBox-native HSRP/FHRP import, upgrade-safe FHRP VIP conflict handling, and NetBox 4.6 job-test compatibility hardening while preserving the 1.1 API/NQE limits

## v1.1.0 — 2026-06-02

reduces Forward SaaS API/NQE pressure with source-level API pacing, parameterized prefix shard queries, single-pass interface NQE, and release-validation smoke evidence

## v1.0.0 — 2026-05-27

first 1.x release line with API/NQE stability groundwork but without 1.1 API pacing and scale-optimized query improvements

## v0.9.4.6 — 2026-05-22

tightens delete-heavy device cleanup shard planning after live evidence showed device deletes still exceeded native Branching change-budget guidance

## v0.9.4.5 — 2026-05-22

plans delete-heavy device cleanup shards more conservatively so tag-scope prune runs stay closer to native Branching change-budget guidance

## v0.9.4.4 — 2026-05-21

clarifies large branching progress by clamping progress-bar display and surfacing current shard row progress in the ingestion UI

## v0.9.4.3 — 2026-05-20

hardens delete behavior by converting protected-reference delete failures into dependency skips so tag-scope prune/device cleanup runs continue safely

## v0.9.4.1.1

keeps the shared-branch architecture, execution ledger, support logging, and scale hardening while preserving the read-only advisory surfaces from `v0.9.0`

## v0.9.0 — 2026-05-15

adds read-only analysis, workload preview, advisory summaries, and native log export for troubleshooting while keeping lifecycle enrichment and predict deferred

## v0.8.6.3 — 2026-05-14

hardens beta routing scope resolution, invalid ASN filtering, conservative virtual chassis skips, and fast-bootstrap baseline readiness when only optional model issues remain

## v0.8.6.2 — 2026-05-13

hardens issue and job-log rendering so unexpected nested payload objects stay JSON-safe in the UI and API

## v0.8.6.1 — 2026-05-12

clarifies the native NQE map bulk edit workflow so repository-path mode and runtime query-ID resolution are explicit in the UI

## v0.8.6 — 2026-05-12

refreshes org-repository query publishing with flattened built-ins, filters invalid IPv4 prefix artifacts, adds parent-prefix diagnostics, and hardens virtual chassis/device and routing issue handling

## v0.8.5 — 2026-05-11

makes the beta routing and module maps broadly available by default while keeping virtual chassis conservative, hardens repository query lookup responses, and clears stale row progress when a sync fails or advances phases

## v0.8.4 — 2026-05-11

stops importing Forward HA peers as NetBox virtual chassis by default, hardens repository query lookup responses, and clears stale row progress when a sync fails or advances phases

## v0.8.3 — 2026-05-10

isolates per-model query failures, blocks positionless virtual-chassis assignments before NetBox save, and lets later shards such as routing continue while withholding dirty diff baselines

## v0.8.2 — 2026-05-10

adds portable repository query-path execution with native NetBox selectors, publish-and-bind bulk edit, bidirectional restore, and fixes IP address rows whose Forward interface cannot be resolved

## v0.8.1.1 — 2026-05-09

fixes virtual chassis NQE output so NetBox receives a member position with virtual chassis assignments

## v0.8.1 — 2026-05-09

fixes fast-bootstrap native change tracking/statistics and adds timeout guidance plus transient Forward API HTTP retries

## v0.8.0 — 2026-05-09

adds an opt-in fast bootstrap backend for trusted large baselines while keeping Branching as default, and skips NetBox-invalid LAG cable endpoints

## v0.7.1 — 2026-05-08

keeps the NetBox-native multi-branch workflow, adds shard heartbeat visibility, and hardens large-shard retries and cable ingestion handling

## v0.7.0 — 2026-05-07

extracts the 0.7 sync boundaries and adds shard heartbeat visibility

## v0.6.5 — 2026-05-06

adds audited validation force-allow overrides and routing evidence enrichment; optional routing/peering import remains beta; native `dcim.module` import is beta

## v0.6.4 — 2026-05-06

optional routing/peering import is beta; native `dcim.module` import is beta

## v0.6.3 — 2026-05-06

native `dcim.module` import is beta

## v0.6.2 — 2026-05-06

native `dcim.module` import is beta

## v0.6.1 — 2026-05-05

native `dcim.module` import is beta

## v0.6.0 — 2026-05-04

native `dcim.module` import is beta

## v0.5.9.1 — 2026-05-04

Superseded by `v0.6.0`

## v0.5.9 — 2026-05-03

Superseded by `v0.5.9.1`

## v0.5.8 — 2026-05-03

Superseded by `v0.5.9`

## v0.5.7 — 2026-05-03

Superseded by `v0.5.8`

## v0.5.2.1 — 2026-05-02

Superseded by `v0.5.3`

## v0.4.0 — 2026-04-29

Superseded by `v0.5.2.1`

## v0.3.1 — 2026-04-28

Superseded by `v0.4.0`

## v0.3.0.1 — 2026-04-28

Superseded by `v0.3.1`

## v0.3.0 — 2026-04-27

Superseded by `v0.3.0.1`
