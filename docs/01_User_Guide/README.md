# Installation

Install the plugin package, enable the required plugins, run the migrations, and confirm the built-in NQE maps were seeded.

## Requirements

- NetBox 4.5.9 validated; NetBox 4.5.x only
- `netboxlabs-netbox-branching`

## Package Installation

Install the wheel from GitHub Releases into the same Python environment as NetBox:

```bash
pip install /path/to/forward_netbox-0.6.3-py3-none-any.whl
```

Alternatively, install directly from the GitHub source archive:

```bash
pip install /path/to/forward_netbox-0.6.3.tar.gz
```

If you mirror the package into a private Python index, pin the same release version:

```bash
pip install --pre forward-netbox==0.6.3
```

## Release Compatibility

| Plugin Release | NetBox Version | Status |
| --- | --- | --- |
| `v0.6.3` | `4.5.9` validated; `4.5.x` only | Current release; native `dcim.module` import is beta |
| `v0.6.2` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.6.3`; native `dcim.module` import is beta |
| `v0.6.1` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.6.2`; native `dcim.module` import is beta |
| `v0.6.0` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.6.1`; native `dcim.module` import is beta |
| `v0.5.9.1` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.6.0` |
| `v0.5.9` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.5.9.1` |
| `v0.5.8` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.5.9` |
| `v0.5.7` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.5.8` |
| `v0.5.1` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.5.3` |
| `v0.4.0` | `4.5.9` validated; `4.5.x` only | Current unsupported release |
| `v0.3.1` | `4.5.8` validated; `4.5.x` only | Superseded by `v0.4.0` |
| `v0.3.0.1` | `4.5.8` validated; `4.5.x` only | Superseded by `v0.3.1` |
| `v0.3.0` | `4.5.8` validated; `4.5.x` only | Superseded by `v0.3.0.1` |

## Version History

| Release | Summary |
| --- | --- |
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
| `v0.5.1` | Adds default merged-branch cleanup to deprovision temporary branch schemas after successful merges, plus inferred interface cable import, device feature tag rules, and data-file-aware alias query workflows. |
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
| `v0.1.4.1` | Patch release that bounds built-in `dcim.virtualchassis` names and domains to NetBox field limits. |
| `v0.1.4` | Hardened built-in NQE mappings and docs for large dataset syncs. |
| `v0.1.3` | Enforced deterministic model identity contracts across sync ingestion. |
| `v0.1.2` | Improved ingestion safety, diagnostics, and compatibility with existing NetBox objects. |
| `v0.1.1` | Added NQE pagination, shared helper composition, and release/doc cleanup. |
| `v0.1.0` | Initial unsupported release of the plugin. |

## Support Disclaimer

This plugin is provided at your own risk. It is an unsupported release with no support commitment or compatibility guarantee beyond NetBox 4.5.x.

## Enable The Plugins

Add both plugins to the NetBox configuration:

```python
PLUGINS = [
    "netbox_branching",
    "forward_netbox",
]
```

## Database Migration

Apply the NetBox database migrations:

```bash
python manage.py migrate
```

## Post-Install Verification

After migration:

1. Open NetBox.
2. Confirm the `Forward Networks` plugin menu is present.
3. Open `NQE Maps` and verify the built-in maps were seeded automatically.
4. Confirm you can open the `Sources`, `Syncs`, and `Ingestions` views without errors.
5. Open `Syncs > Add` and confirm the `Snapshot` selector includes `latestProcessed`.
