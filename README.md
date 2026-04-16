# Forward NetBox Plugin

`forward_netbox` is a NetBox plugin that syncs Forward Networks inventory into NetBox through direct Forward API connectivity and NQE while preserving the branch-backed sync, diff, and merge workflow.

## Release Compatibility

| Plugin Release | NetBox Version | Status |
| --- | --- | --- |
| `v0.1.4.2` | `4.5.x` only | Current unsupported release |

## Version History

| Release | Summary |
| --- | --- |
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
  - `dcim.interface`
  - `dcim.macaddress`
  - `dcim.inventoryitem`
  - `ipam.vlan`
  - `ipam.vrf`
  - `ipam.prefix` for IPv4 and IPv6
  - `ipam.ipaddress`

## Quickstart

1. Install the plugin into the same Python environment as NetBox:

```bash
pip install forward-netbox==0.1.4.2
```

If you need an offline or pinned artifact workflow, install the wheel or source archive from GitHub Releases instead:

```bash
pip install /path/to/forward_netbox-0.1.4.2-py3-none-any.whl
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
- `invoke forward_netbox.smoke-sync --merge` will merge the staged branch after a clean run

## Documentation

- [Documentation Home](docs/README.md)
- [Installation](docs/01_User_Guide/README.md)
- [Configuration](docs/01_User_Guide/configuration.md)
- [Usage and Validation](docs/01_User_Guide/usage.md)
- [Troubleshooting](docs/01_User_Guide/troubleshooting.md)
- [Built-In NQE Reference](docs/02_Reference/built-in-nqe-maps.md)
- [Model Mapping Matrix](docs/02_Reference/model-mapping-matrix.md)
- [Shipped NQE Query Files](https://github.com/forwardnetworks/forward-netbox/tree/main/forward_netbox/queries)
- [License](LICENSE)
