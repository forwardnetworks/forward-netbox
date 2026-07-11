# Changelog

Generated from the README compatibility table by `scripts/gen_changelog.py`. Do not edit by hand.

## v2.5.3

**Editions** — `forward-netbox` is now one package with two install profiles: core (`pip install forward-netbox`, NetBox-builtin models only, no optional-plugin dependencies) and integrations (`pip install forward-netbox[integrations]`, or per-plugin `[dlm]`/`[routing]`/`[aci]`/`[peering]`) which install the opt-in netbox-dlm / netbox-routing / netbox-cisco-aci / netbox-peering-manager maps (still disabled until the plugin is installed and enabled). Fix: enabling the **netbox-routing** models no longer crashes the sync with `TypeError: '<' not supported between instances of 'NoneType' and 'int'` — the BGP/OSPF dependency-lookup cache sorted scope keys whose global-table VRF pk is `None` against a VRF peer on the same router/device; the sort is now None-safe. Fix: **Drift Report clarity** — the report replays a cached dependency-preview, so a stale or empty-baseline preview could read as real drift (field report: 18/19 models showing 100% pending). Now (1) an **empty-baseline hint** when every model shows all Forward rows pending with zero removals (the "preview ran before data was ingested/merged" signature — it is everything Forward has, not real mismatches), (2) the **staleness banner** also fires when the preview is over a day old (not only when a newer sync ran since), and (3) a **Preview Dependencies** button on the report to recompute on the spot. No query or data change.

## v2.5.2

Feature: optional **netbox-dlm CVE + Vulnerability** feed — two new opt-in NQE maps import Forward's security analysis into the netbox-dlm plugin: the **CVE catalog** (`network.cveDatabase.cves`, worst-case per-vendor severity mapped to the plugin's severity choices) and **per-device vulnerabilities** (`device.cveFindings`, one row per device↔CVE). Disabled by default; requires the netbox-dlm plugin (0.2.0+ ships migrations — run `migrate`; 0.1.0 needs `makemigrations netbox_dlm` first). The Vulnerability map is large (~16 rows/device) — enable it scoped or on a fresh branch first. Fix: **SNMP endpoint platform unification** — Avocent/Cyclades/AlterPath (enterprise OIDs `10418` + `2925` plus product-name signatures) now resolve to a single `Avocent` platform instead of fragmenting across `Avocent`/`AlterPath`/`SNMP`; a multiline `sysDescr` is whitespace-collapsed so it can't leak a junk platform name, and a missing `sysDescr` falls back to `Unknown` rather than a fake `SNMP` vendor. Query-only endpoint change; **Publish Bundled Queries** after upgrading.

## v2.5.1

Fix: rows with a blank `device_type` were rejected with `model: This field cannot be blank` — a device with no resolved model (`device.platform.model` null) and, more commonly, an SNMP endpoint reporting an empty `sysDescr`. The bundled queries now guard both (null-safe/empty-safe fallbacks to `Unknown` / `SNMP Endpoint`) instead of dropping the row (live-verified: 0 blank device types across 5645 rows). Query-only change; **Publish Bundled Queries** after upgrading.

## v2.5.0

Feature: optional **netbox-dlm** (Device Lifecycle Management) integration — three new opt-in NQE maps sync Forward's end-of-life analysis into the netbox-dlm plugin: OS software versions with vendor EOL dates per (platform, version), hardware end-of-life notices per device type (Cisco/Palo Alto/Fortinet part support), and each device's running software version. Disabled by default; requires the netbox-dlm plugin (run `makemigrations netbox_dlm && migrate` after installing it — it ships no migrations). Fix: syncs no longer crash mid-provision when an installed plugin's migrations were never applied (`relation ... does not exist`) — a preflight now fails in seconds with the app name and remedy, and a new **Database tables** Health check surfaces the gap before you sync.

## v2.4.5

Fix: sync no longer crashes on netbox-branching **1.1.1** (`SquashMergeStrategy has no attribute '_split_bidirectional_cycles'` — 1.1.1 removed that internal helper; the bidirectional-cycle split is now built into the plugin and the dependency is bounded to `<1.2`). Also fixes SNMP-endpoint rows failing validation: the bundled endpoint query branches now clamp sysDescr-derived `device_type` to NetBox's 100-char limit (`substring`) and guard empty slugs — the fix lives in the NQE queries (the source of truth), so **Publish Bundled Queries** again after upgrading (fixes the `Ensure this value has at most 100 characters` rejects and the `At least one coalesce lookup must be provided` error).

## v2.4.4

Fix: SNMP-endpoint import now works on **tag-scoped** syncs — the device-tag include scope silently excluded every endpoint, both query-side and in the plugin's local scope filter (whose scoped-device set was built from modeled devices only, so endpoint rows were always dropped; with prune enabled they would even be deleted). Endpoint import now ignores the include scope (exclude tags still apply) and endpoint names join the scoped set (validated live: 355 Avocent endpoints import under a tag-scoped sync). Also fixes the merge-phase `Tag with this Name already exists` issues: a same-named/same-slug tag already on main is now treated as merged instead of failing the branch's tag create.

## v2.4.3

Fix: the pinned-query opt-in Health warning (2.4.1) over-claimed failure — it reads "nothing new syncs" and fires on any pinned map with the feature on, even after the query is fixed, because the Health page can't read a pinned query's contents. Reworded to a "Pinned — can't verify locally" heads-up that points at **Export Live Query Drift** to confirm (`source_matches_bundled`), instead of asserting failure. No behavior change.

## v2.4.2

Fix: endpoint import (`sync_endpoints`) and device-tag sync (`sync_device_tags`) now work with the alias-aware and rules-aware query variants (`forward_devices_with_netbox_aliases`, `forward_device_feature_tags_with_rules`), not just the base queries — operators running the variants saw the toggles silently do nothing (validated live: 355 Avocent endpoints import; `Mgmt_*` tags sync). Adds a **Publish Bundled Queries** button on the sync Health page (beside Refresh Query IDs) and two Health warnings: when an opt-in feature is enabled but no enabled map provides it, and when a base query and its opt-in variant are both enabled (they double-apply rows for the same model and churn — enable one). The alias-aware device query now emits the clean role name (e.g. `ROUTER`) to match the base query — expect a one-time role update on alias-mapped devices.

## v2.4.1

Fix: opt-in features (SNMP endpoint import, device-tag sync) silently did nothing on sources that run org-managed **pinned** Forward query IDs predating the feature — the sync Health page now raises an actionable warning instead of a silent badge. Remediation: publish the bundled queries to your Forward org folder (Overwrite on), then use Refresh Query IDs, then re-sync.

## v2.4.0

Fix: the "Import SNMP Endpoints as Devices" toggle now renders on the source form (the field shipped in 2.3.2 but was not in any fieldset, so it never showed), letting operators enable endpoint import from the GUI.

## v2.3.2

Feature: optional import of Forward SNMP endpoints (e.g. Avocent console servers) as NetBox devices — off by default (`sync_endpoints`), enabled per source and scoped by the same device tags.

## v2.3.1



## v2.3.0

GA/enterprise hardening: encrypted Forward credential at rest, PyPI Trusted Publishing + SBOM, Prometheus metrics + stuck-job alert, populated-DB upgrade test, dead-code removal (multi_branch/density-learning), reliability fixes (jittered/Retry-After backoff, SaaS rate clamp, PK-anchored device prune), and supported-product framing. Drop-in from 2.2.5 — stored credentials auto-encrypt on save; rotating SECRET_KEY requires re-entering them.

## v2.2.5

Feature: operator-selectable **Sync Device Tags** — pick which Forward device tags (e.g. `Mgmt_*`) become NetBox device tags (replaces the hardcoded feature-tag set); Fix dependency-preview AttributeError + vsys job pile-up guard (hung pending); test/require NetBox 4.6.4

## v2.2.4

Hotfix: device-analysis NQE (bare foreach) errored refresh + CVE list; surface job errors into job.data

## v2.2.3

Field-feedback fixes: delete-count labeling, vsys/vdom auto-link, skip empty VRFs, per-device CVE list, churn pinpoint, query-ID status clarify

## v2.2.2

Fix 504 gateway timeouts on large syncs: stop recomputing change-explainability on every poll during a long merge + back off poll to 15s

## v2.2.1

Add read-only forward_apply_identity_audit diagnostic to pinpoint 1-created/1-deleted idempotency churn

## v2.2.0

Fix devices mis-assigned the ACI platform; link Palo vsys / Fortinet vdom firewalls to their physical chassis

## v2.1.5

Fix Prune orphans erroring on empty sites that still hold a VLAN/VM/prefix (delete only truly-empty sites)

## v2.1.4

Tag delete-eligible global IPAM (prefixes/VLANs/VRFs) for manual review

## v2.1.3

Prune empty orphan sites (zero devices + zero racks) alongside out-of-scope devices

## v2.1.2

Feature + docs: (1) new out-of-scope orphan health signal — the sync health summary now shows how many NetBox devices match none of the included Forward tags (removable via Scope Reconciliation -> Prune orphans), mirroring the backfilled signal, via a self-healing `forward-out-of-scope` device tag and a `?tag=forward-out-of-scope` filter; (2) docs: the "no covering prefix" diagnostic now names /32 and /128 host addresses (loopbacks, anycast, some VIPs), and the Operations Guide documents backfilled (in-scope, kept) vs out-of-scope (removable) devices. Drop-in from `2.1.1`.

## v2.1.1

Bugfix + diagnostics: (1) the IPv4/IPv6 IP queries global dedup now pins the chosen interface to the chosen device (mirroring the VRF and MAC dedup blocks), so a deduped global address can no longer be attributed to an interface on a different device — the source of spurious "target interface was not imported" skips; (2) new read-only `forward_primary_ip_audit` command buckets Mgmt_ primary-IP resolution per device (resolvable / device-not-in-netbox / interface-not-matched / interface-present-no-IP) to pinpoint why a device does not get a primary IP. Drop-in from `2.1.0`.

## v2.1.0

Feature: `forward_scope_ipam_audit` management command — a read-only audit listing network-global IPAM (prefixes, VLANs, VRFs) that NetBox holds but the sync's latest Forward fetch no longer reports, as manual-review candidates. Device-tag scope prune is device-derived and never removes global IPAM; this surfaces stale global objects without deleting anything (identity matching reuses the apply engine so verdicts match the sync). Drop-in from `2.0.8`.

## v2.0.8

Bugfix: progress bars now reach 100% on a completed sync. For relationship and two-phase models (cable+termination, device+primary_ip, module+moduletype, fhrp group+assignment) the per-model bar settled below 100% because the merge `total` counts ChangeDiff rows while `current` counts applied objects; a finished job now renders every model at 100%. Cosmetic only — no apply/merge/data change. Drop-in from `2.0.7`

## v2.0.7

Bugfixes + diagnostics: (1) a MAC whose target interface was not imported is now a benign aggregated skip like the IP path (with the canonical-name fallback), not a red `ForwardSearchError` failure; (2) the two benign IP diagnostics (filtered-unassignable, no-parent-prefix) collapse to one summary line each instead of a 20-row wall; (3) when a `require_diff` sync is blocked by a failed diff fetch, the block now names that cause and the `Allow full fallback` remedy. Drop-in from `2.0.6`

## v2.0.6

Bugfix: stop the pernicious FHRP-group sync churn. When a virtual IP is shared by two HSRP/VRRP groups (different group_id), the second group was created then immediately deleted every sync (VIP-conflict), so a fixed set of FHRP groups was added and removed on every run. The second group now persists with its interface assignment (the VIP stays attached to the first group; NetBox allows a VIP on only one group), and deleting a shared-VIP group no longer removes the other group's VIP. Drop-in from `2.0.5`

## v2.0.5

Branding + polish: the plugin is now presented as **Forward Field Integration** (NetBox plugin name, sidebar menu, docs/site titles). Adds a theme-aware Forward Networks logo + `#ff3506` accent bar at the top of the Source/Sync/Ingestion pages. Display-only: package `forward_netbox`, the `forward` URL prefix, NQE query names, and all APIs are unchanged. Drop-in from `2.0.4`

## v2.0.4

Patch: collapse the module-sync readiness warning wall into ONE summary. When module sync is enabled before a device's module bays exist in NetBox, every module row is skipped; 2.0.3 capped the per-row lines at 3, this replaces them entirely with a single actionable line per sync (total skipped + a few examples + the `forward_module_readiness` remedy). Other skip reasons are unchanged. No engine/schema/org changes; drop-in from `2.0.3`

## v2.0.3

Patch: (1) module-sync readiness warnings no longer flood the log — the per-row `module bay does not exist; run forward_module_readiness` skip is capped to a few examples plus a suppressed-count summary (was up to 20 near-identical lines per sync); (2) fixes the release `CI` gate (`CHANGELOG matches README`) that had been red since v1.7.2 — the generator no longer depends on git tag-date timing; (3) removes dead executor code (`ForwardFastBootstrapExecutor.run`) and refreshes stale internal docs. No engine/schema changes; drop-in from `2.0.2`

## v2.0.2

Patch: apply_device_scope_tags now works with multiple include tags in `any` match mode — each device is tagged with exactly the include tag(s) it carries (resolved per-device at fetch time), instead of skipping. Also silences the spurious `Skipping untagged VLAN 1` warning (VID 1 is NetBox's implicit access default and is intentionally not imported). No engine/schema changes; drop-in from `2.0.1`, no org republish

## v2.0.1

Patch: fixes two 2.0.0 regressions an operator hits immediately — a false `netbox_branching is not installed; syncs will fail` startup warning (the dependency check used the wrong distribution name), and a 500 on the Sync list page (`KeyError: 'available'` from a removed execution-ledger summary). No engine or data changes; drop-in upgrade from `2.0.0`

## v2.0.0

Breaking 2.0 — single-branch is the only execution path. Removed the per-shard branching/fast-bootstrap/resumable executor, 10k-change budget sharding, and the execution-ledger run-history; dropped the backend/max-changes/scheduler-overlap selectors

## v1.7.2

Collection-gap diagnostics: per-reason backfill breakdown + staleness, growth/trend escalation, per-device collection result, ACI delete safety valve, opt-in auto-tag

## v1.7.1

ACI BD/L3Out graduation + FHRP churn fix (replaces yanked 1.7.0 and 1.6.2)

## v1.7.0

ACI bridge domain and L3Out NQE maps; query publish hardening

## v1.6.2

completes the 1.6.1 line (1.6.1 was yanked — its PyPI build predated these): device tag scope now covers VLANs/VRFs and prefixes derive from connected interface subnets; the FHRP group churn (delete+recreate every sync) is fixed by identity-bucket sharding; device analysis is a first-class model with a fleet list view, REST API, and an Open in Forward deep-link.

## v1.6.1

matures the 1.6.0 features and tooling — Device Analysis is now a NetBox model with a fleet-wide list view, REST API, and per-device-FK panel scoping (with up-interface blast-radius and opt-in post-sync refresh); adds a schedulable collection-gap alert command, run-history drill-down links, and hardened release tooling (one-command release script, generated CHANGELOG, conventional-commit hook).

## v1.6.0

ships the blue-sky tranche — release automation (`invoke release`), an Operations Guide, a collection-gap health signal, a sync run-history panel, a read-only device analysis panel (GA reachability / connectivity-degree blast radius / CVE exposure), and a bidirectional per-model drift report.

## v1.5.10

promotes `ipam.prefix` into the default bulk-ORM safe set (the last model still on the adapter path) — it runs the per-object tree apply so NetBox prefix hierarchy `_depth` stays correct, with null-VRF (global) prefix identity and canonical-CIDR matching parity-tested against the adapter.

## v1.5.9

adds a maintained `forward-backfilled` NetBox tag so operators can see which in-scope devices were backfilled (not freshly collected) in the latest snapshot — a Tag backfilled devices button on the Scope Reconciliation page plus a link to the filtered device list (`?tag=forward-backfilled`); the tag self-heals as devices collect again.

## v1.5.8

`dcim.module` sync now **adopts** the device interfaces Forward already syncs instead of recreating them (fixes `dcim_interface_unique_device_name` IntegrityError when modules are enabled), and `ipam.fhrpgroup` no longer churns (delete+recreate the same HSRP groups every sync) — the snapshot diff no longer deletes a group it is simultaneously upserting. Preview Dependencies now runs as a background job (cached result on the preview page), fixing a 504 timeout on large fabrics.

## v1.5.7

**Prune orphans** and **Create missing module bays** now run as background jobs (watch the Jobs tab) instead of synchronously, fixing a 504 gateway timeout on large fabrics. Module Readiness `Ready` reflects missing bays only (out-of-scope-device rows no longer hold it `No`), and the bulk `ipam.ipaddress` path tolerates duplicate global IPs.

## v1.5.6

fixes an `ipam.ipaddress` sync failure (`Ambiguous coalesce lookup`) when a reused /30 link range leaves duplicate global (VRF-less) IPs for the same host — the adapter now resolves to one deterministically (preferring the copy already on the synced interface) and warns, instead of failing the row.

## v1.5.5

surfaces the orphan-prune and module-bay readiness workflows in the sync detail UI (no CLI or CSV): a **Scope Reconciliation** page with a **Prune orphans** button, and a **Module Readiness** page with a **Create missing module bays** button that creates the bays directly in NetBox.

## v1.5.4

adds `--prune-orphans`/`--apply` to `forward_device_scope_reconciliation_audit` to delete stale out-of-scope (orphan) NetBox devices left by an earlier broader sync that `device_tag_prune_out_of_scope` cannot reach (orphans are absent from the scoped Forward result). Dry-run by default; tagged-but-backfilled devices are preserved.

## v1.5.3

classifies APIC controllers onto the `APIC` platform (distinct from ACI switches) so controller and switch software versions model separately; splits IP address import into independent `Forward IPv4 IP Addresses` and `Forward IPv6 IP Addresses` maps (a migration removes the combined map) so address families toggle independently; promotes `dcim.interface` and `ipam.ipaddress` into the default bulk-ORM safe set and removes bulk-apply update churn across every bulk model so steady-state syncs issue no redundant writes; preserves operator platform-manufacturer overrides on bulk update; and adds the opt-in `Apply Device Scope Tags` source option plus the `forward_device_scope_reconciliation_audit` and `forward_apic_cimc_readiness_audit` commands.

## v1.5.2

collapses the flood of `dcim.modulebay` branch-merge failures (a NetBox Branching/MPTT limitation when a new device's module bays are auto-instantiated in a branch) into a single actionable `ModuleBayMergeUnsupported` ingestion issue that points at the `forward_module_readiness` import workflow. Device and interface sync are unaffected.

## v1.5.1

adds the `latestCollected` snapshot selector that skips backfilled (collection-canceled) snapshots and resolves to the most recent snapshot with a freshly-collected in-scope device, warns when a `latestProcessed` run finds every in-scope device backfilled instead of silently applying zero changes, records the resolved snapshot's own metadata for `latestCollected` runs, and adds an Architecture Flow reference doc.

## v1.5.0.1

fixes platform NQE query using `normalizePlatformName` to avoid evaluation failures on unsupported vendor/OS combinations, adds `--overwrite` flag to the validation-org repair command, and hardens the NQE org-publish commit loop to retry after 409 INVALID_CHANGE_PATH.

## v1.5.0

hardens ingest throughput via adaptive async-NQE poll backoff, ndjson streaming, webhook/event-rule signal suppression during the apply loop, targeted validation (skips DB-hitting uniqueness checks on existing objects in both simple and tree-model bulk paths), and async advanced-reachability trigger (FWD-53559). Full test suite green on NetBox 4.5.9 (1092/0/0) and 4.6.2 (1092/0/26 routing-plugin version-gated).

## v1.4.3

hardens query-path provenance by requiring source-backed query-id repair at preflight, enforces async NQE source parsing for 26.6 execution paths, proves CIMC/APIC custom-command updates in source and keeps the 1.4 production-hardening line intact.

## v1.4.2

adds CIMC platform separation, visible query-drift repair and dependency preview on the sync detail page, and keeps the module-bay merge hardening plus parent-interface description preservation from the prior patch line.

## v1.4.1.1

prevents optional `dcim.module` sync from emitting merge-breaking `dcim.modulebay` side-effect creates when module bays are missing and prevents LAG member rows from clearing existing parent interface descriptions.

## v1.4.1

keeps the hard parent-device sync contract, adds query-ID drift remediation plus support-bundle diagnostics, and carries the 1.4 production-hardening tranche forward as the release line.

## v1.4.0

enforces a hard parent-device sync contract so child models cannot run without `dcim.device`, which prevents stale sync configs from skipping the device shard and breaking dependent imports.

## v1.3.5.5

adds compressed support-bundle ZIP downloads with optional password protection, and folds live source health, live query-drift, and live data-file diagnostics into the troubleshooting bundle so operator support can work from one artifact.

## v1.3.5.4

repackaged the `1.3.5.3` query-contract hardening on a fresh patch tag and kept strict shipped-query parameter-contract validation, legacy tag alias stripping, and summary-only support-bundle previews.

## v1.3.5.3

keeps the `1.3.5.2` claimed-step and payload compaction behavior, adds strict shipped-query parameter-contract validation, strips legacy tag aliases from runtime NQE payloads, and keeps support-bundle previews summary-only.

## v1.3.5.1

removes raw `model_results` from the sync telemetry summary and prevents unparameterized query IDs from receiving source-level tag parameters, which keeps the sync detail view responsive and preserves the saved-query-ID path compatibility.

## v1.3.5

keeps the 1.3.x saved-query-ID path parameter-compatible, tightens ACI platform detection with command-inventory signals, and preserves the lower-noise execution accounting used by the 1.3.x sync path

## v1.3.4

makes non-retryable Branching merge failures visible in job logs, leaves failed merge branches in a terminal `Failed` state instead of stale `Merging`, and carries disabled async NQE client staging for future Forward 26.6 support

## v1.3.3

refreshes bundled NQE syntax for saved query-ID execution, keeps all shipped maps parameter-compatible with `forward_netbox_shard_keys`, and updates the saved validation-folder query IDs used by the 1.3.x sync path

## v1.3.2

adds optional `netbox-cisco-aci` integration maps and adapter support, keeps ACI maps disabled by default, preserves parameterized NQE execution, and validates repeat-sync idempotence for the proven ACI write path

## v1.3.1

preserves the `v1.3.0` parameterized NQE path, removes the legacy sync column-filter shard path, and fixes repeat prefix sync accounting so unchanged `ipam.prefix` rows report as unchanged instead of update churn

## v1.3.0

eliminates default Forward NQE column-filter shard fetches in favor of query-side `forward_netbox_shard_keys` parameters, keeps local shard safety filtering, and preserves branch boundaries while reducing Forward SaaS API/NQE pressure

## v1.2.3

further reduced Forward SaaS API/NQE pressure by coalescing compatible sibling shard EQUALS_ANY filters, added local change-explainability summaries, and kept staged branch boundaries unchanged

## v1.2.1

fixes prefix VRF churn by making `ipam.prefix` identity exact for global and VRF-scoped rows while preserving parameterized prefix shard NQE execution

## v1.2.0

adds optional NetBox-native HSRP/VRRP FHRP import, bounded access/native interface VLAN assignment from existing site-scoped VLANs, upgrade-safe FHRP VIP conflict handling, and NetBox 4.6 job-test compatibility hardening while preserving the 1.1 API/NQE limits

## v1.1.1

adds optional NetBox-native HSRP/FHRP import, upgrade-safe FHRP VIP conflict handling, and NetBox 4.6 job-test compatibility hardening while preserving the 1.1 API/NQE limits

## v1.1.0

reduces Forward SaaS API/NQE pressure with source-level API pacing, parameterized prefix shard queries, single-pass interface NQE, and release-validation smoke evidence

## v1.0.0

first 1.x release line with API/NQE stability groundwork but without 1.1 API pacing and scale-optimized query improvements

## v0.9.4.6

tightens delete-heavy device cleanup shard planning after live evidence showed device deletes still exceeded native Branching change-budget guidance

## v0.9.4.5

plans delete-heavy device cleanup shards more conservatively so tag-scope prune runs stay closer to native Branching change-budget guidance

## v0.9.4.4

clarifies large branching progress by clamping progress-bar display and surfacing current shard row progress in the ingestion UI

## v0.9.4.3

hardens delete behavior by converting protected-reference delete failures into dependency skips so tag-scope prune/device cleanup runs continue safely

## v0.9.4.1.1

keeps the shared-branch architecture, execution ledger, support logging, and scale hardening while preserving the read-only advisory surfaces from `v0.9.0`

## v0.9.0

adds read-only analysis, workload preview, advisory summaries, and native log export for troubleshooting while keeping lifecycle enrichment and predict deferred

## v0.8.6.3

hardens beta routing scope resolution, invalid ASN filtering, conservative virtual chassis skips, and fast-bootstrap baseline readiness when only optional model issues remain

## v0.8.6.2

hardens issue and job-log rendering so unexpected nested payload objects stay JSON-safe in the UI and API

## v0.8.6.1

clarifies the native NQE map bulk edit workflow so repository-path mode and runtime query-ID resolution are explicit in the UI

## v0.8.6

refreshes org-repository query publishing with flattened built-ins, filters invalid IPv4 prefix artifacts, adds parent-prefix diagnostics, and hardens virtual chassis/device and routing issue handling

## v0.8.5

makes the beta routing and module maps broadly available by default while keeping virtual chassis conservative, hardens repository query lookup responses, and clears stale row progress when a sync fails or advances phases

## v0.8.4

stops importing Forward HA peers as NetBox virtual chassis by default, hardens repository query lookup responses, and clears stale row progress when a sync fails or advances phases

## v0.8.3

isolates per-model query failures, blocks positionless virtual-chassis assignments before NetBox save, and lets later shards such as routing continue while withholding dirty diff baselines

## v0.8.2

adds portable repository query-path execution with native NetBox selectors, publish-and-bind bulk edit, bidirectional restore, and fixes IP address rows whose Forward interface cannot be resolved

## v0.8.1.1

fixes virtual chassis NQE output so NetBox receives a member position with virtual chassis assignments

## v0.8.1

fixes fast-bootstrap native change tracking/statistics and adds timeout guidance plus transient Forward API HTTP retries

## v0.8.0

adds an opt-in fast bootstrap backend for trusted large baselines while keeping Branching as default, and skips NetBox-invalid LAG cable endpoints

## v0.7.1

keeps the NetBox-native multi-branch workflow, adds shard heartbeat visibility, and hardens large-shard retries and cable ingestion handling

## v0.7.0

extracts the 0.7 sync boundaries and adds shard heartbeat visibility

## v0.6.5

adds audited validation force-allow overrides and routing evidence enrichment; optional routing/peering import remains beta; native `dcim.module` import is beta

## v0.6.4

optional routing/peering import is beta; native `dcim.module` import is beta

## v0.6.3

native `dcim.module` import is beta

## v0.6.2

native `dcim.module` import is beta

## v0.6.1

native `dcim.module` import is beta

## v0.6.0

native `dcim.module` import is beta

## v0.5.9.1

Superseded by `v0.6.0`

## v0.5.9

Superseded by `v0.5.9.1`

## v0.5.8

Superseded by `v0.5.9`

## v0.5.7

Superseded by `v0.5.8`

## v0.5.2.1

Superseded by `v0.5.3`

## v0.4.0

Superseded by `v0.5.2.1`

## v0.3.1

Superseded by `v0.4.0`

## v0.3.0.1

Superseded by `v0.3.1`

## v0.3.0

Superseded by `v0.3.0.1`
