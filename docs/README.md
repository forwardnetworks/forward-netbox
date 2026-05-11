# Forward NetBox Documentation

`forward_netbox` connects NetBox directly to Forward Networks, executes NQE against a selected Forward snapshot, and stages the resulting changes in a NetBox branch for review and merge by default. Large trusted baselines can optionally use fast bootstrap direct writes before returning to the Branching workflow.

## Release Compatibility

| Plugin Release | NetBox Version | Status |
| --- | --- | --- |
| `v0.8.4` | `4.5.9` validated; `4.5.x` only | Current release; stops importing Forward HA peers as NetBox virtual chassis by default, hardens repository query lookup responses, and clears stale row progress when a sync fails or advances phases |
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
| `v0.8.4` | Keeps the bundled virtual chassis map conservative by emitting no rows for Forward HA peers, preserves custom virtual-chassis maps for true NetBox membership, handles Forward repository query lookups that return a `queries` list, and makes failed sync activity show the terminal failure instead of stale row heartbeat text. |
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

This repository is provided for use at your own risk. It is an unsupported release and is not an officially supported Forward Networks product.

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
