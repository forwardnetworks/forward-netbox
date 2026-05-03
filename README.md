# Forward NetBox Plugin

`forward_netbox` is a NetBox plugin that syncs Forward Networks inventory into NetBox through direct Forward API connectivity and NQE while preserving the branch-backed sync, diff, and merge workflow.

## Release Compatibility

| Plugin Release | NetBox Version | Status |
| --- | --- | --- |
| `v0.5.8` | `4.5.9` validated; `4.5.x` only | Current release |
| `v0.5.7` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.5.8` |
| `v0.5.2.1` | `4.5.9` validated; `4.5.x` only | Superseded by `v0.5.3` |
| `v0.4.0` | `4.5.9` validated; `4.5.x` only | Current unsupported release |
| `v0.3.1` | `4.5.8` validated; `4.5.x` only | Superseded by `v0.4.0` |
| `v0.3.0.1` | `4.5.8` validated; `4.5.x` only | Superseded by `v0.3.1` |
| `v0.3.0` | `4.5.8` validated; `4.5.x` only | Superseded by `v0.3.0.1` |

## Version History

| Release | Summary |
| --- | --- |
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
| `v0.1.4.1` | Patch release that bounds built-in `dcim.virtualchassis` names/domains to NetBox field limits. |
| `v0.1.4` | Hardened built-in NQE mappings and docs for large real-world datasets. |
| `v0.1.3` | Enforced deterministic model identity contracts during sync ingestion. |
| `v0.1.2` | Improved ingestion safety, diagnostics, and compatibility with custom NetBox data. |
| `v0.1.1` | Added pagination, shared built-in NQE helpers, and release/doc cleanup. |
| `v0.1.0` | Initial unsupported NetBox plugin release with built-in Forward sync workflow and seeded NQE maps. |

## Support Disclaimer

This repository is provided for use at your own risk. It is an unsupported release and is not an officially supported Forward Networks product. There is no warranty, support commitment, or compatibility guarantee beyond the version table above.

## Features

- Branch-backed sync, diff, and merge flow through `netbox_branching`
- Forward `Sources`, `NQE Maps`, `Syncs`, and `Ingestions`
- Built-in shipped NQE maps seeded automatically after migration
- Support for either published Forward `query_id` references or raw NQE `query` text
- Explicit identity contracts per map (`coalesce_fields`) with strict sync-time ambiguity detection
- Repository-authored built-in queries can share local helper modules and still execute as flattened raw NQE when bundled
- Automatic paging across multi-page Forward NQE result sets during sync execution
- Optional disabled NQE maps for NetBox Device Type Library alias matching through a Forward JSON data file
- Optional disabled NQE map for data-file-driven device feature tag rules
- Snapshot-aware execution with `latestProcessed` or an explicit Forward snapshot per sync
- Ingestion records that preserve the selected snapshot mode, resolved snapshot ID, and Forward snapshot metrics
- Built-in coverage for:
  - `dcim.site`
  - `dcim.manufacturer`
  - `dcim.devicerole`
  - `dcim.platform`
  - `dcim.devicetype`
  - `dcim.device`
  - `dcim.virtualchassis`
  - device feature tags
  - `dcim.interface`
  - `dcim.cable` from exact Forward inferred interface matches
  - `dcim.macaddress`
  - `dcim.inventoryitem`
  - `ipam.vlan`
  - `ipam.vrf`
  - `ipam.prefix` for IPv4 and IPv6
  - `ipam.ipaddress`

## Quickstart

1. Install the plugin into the same Python environment as NetBox:

Install the wheel or source archive from GitHub Releases:

```bash
pip install /path/to/forward_netbox-0.5.8-py3-none-any.whl
```

2. Enable both plugins in the NetBox configuration:

```python
PLUGINS = [
    "netbox_branching",
    "forward_netbox",
]
```

3. Apply migrations:

```bash
python manage.py migrate
```

4. Open NetBox and create a `Forward Source`.
5. Select a Forward network for that source.
6. Create a `Forward Sync`, choose the snapshot selector, and enable the NetBox models you want to sync.
7. Run an adhoc ingestion, review the staged branch diff, review the recorded snapshot details and metrics, and merge when the changes look correct.

For large datasets, prefer committed Forward Org Repository queries referenced by `query_id`, leave `Snapshot` at `latestProcessed`, and treat the first clean merge as the baseline. Later `latestProcessed` runs can then use Forward `nqe-diffs` instead of replaying every model as a full snapshot sync.

The shipped query set includes both default maps and optional alias-aware maps. If your NetBox device types are pre-loaded from the NetBox Device Type Library, upload a Forward JSON data file named `netbox_device_type_aliases.json` with NQE name `netbox_device_type_aliases`, attach it to the Forward network, and run or reprocess a Forward snapshot before enabling the disabled alias-aware device maps or using committed query IDs for those variants. The NetBox plugin runs public `/api/nqe` against the selected snapshot, so latest uploaded data files do not affect plugin sync results until the selected snapshot exposes the data file value. The generated file carries both device type aliases and manufacturer override rows for the alias-aware maps. Without that data file in the selected snapshot, leave the default non-data-file maps enabled.

## Test It Yourself

Use this quick validation flow after installation:

1. Create a `Forward Source` using `https://fwd.app` or your custom Forward URL.
2. Enter a Forward username and password, then confirm the `Network` field populates from the live Forward tenant.
3. Open `NQE Maps` and verify the built-in maps are present.
4. Create a `Forward Sync` tied to the source, leaving `Snapshot` at `latestProcessed` for the first run.
5. Run the sync from the sync detail page.
6. Review the generated `Forward Ingestion`, `Issues`, snapshot details, snapshot metrics, and change diff.
7. Merge the branch and confirm the synced objects appear in NetBox.

## Local Validation

The repository now includes local validation tasks:

- `invoke forward_netbox.lint`
- `invoke forward_netbox.check`
- `invoke forward_netbox.test`
- `invoke forward_netbox.docs`
- `invoke forward_netbox.package`
- `invoke forward_netbox.ci`

For a live Forward smoke run outside CI, set these environment variables and run the smoke task locally:

```bash
export FORWARD_SMOKE_USERNAME='your-forward-username'
export FORWARD_SMOKE_PASSWORD='your-forward-password'
export FORWARD_SMOKE_NETWORK_ID='your-network-id'
invoke forward_netbox.smoke-sync
```

Optional smoke-sync variables:

- `FORWARD_SMOKE_URL` defaults to `https://fwd.app`
- `FORWARD_SMOKE_SNAPSHOT_ID` defaults to `latestProcessed`
- `FORWARD_SMOKE_MODELS` accepts a comma-separated subset such as `dcim.site,dcim.device,dcim.interface`
- `invoke forward_netbox.smoke-sync --validate-only` runs live snapshot/query validation without executing an ingestion
- `invoke forward_netbox.smoke-sync --plan-only` prints the native NetBox Branching shard plan without creating branches
- `invoke forward_netbox.smoke-sync --max-changes-per-branch 10000` stages and merges large baselines in multiple native branches
- `invoke forward_netbox.smoke-sync --no-auto-merge --max-changes-per-branch 10000` stages one shard and pauses for review

Normal UI/API sync jobs always use native multi-branch execution, with a default branch budget of `10000` changes. `Auto merge` controls whether shards advance automatically or pause for review after each shard.

## Documentation

- [Documentation Home](docs/README.md)
- [Installation](docs/01_User_Guide/README.md)
- [Configuration](docs/01_User_Guide/configuration.md)
- [Usage and Validation](docs/01_User_Guide/usage.md)
- [Troubleshooting](docs/01_User_Guide/troubleshooting.md)
- [Built-In NQE Reference](docs/02_Reference/built-in-nqe-maps.md)
- [Device Type Alias Data File](docs/02_Reference/device-type-alias-data-file.md)
- [Model Mapping Matrix](docs/02_Reference/model-mapping-matrix.md)
- [Shipped NQE Query Files](https://github.com/forwardnetworks/forward-netbox/tree/main/forward_netbox/queries)
- [License](LICENSE)
