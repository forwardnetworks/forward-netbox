# Installation

Install the plugin package, enable the required plugins, run the migrations, and confirm the built-in NQE maps were seeded.

## Requirements

- NetBox 4.5.x
- `netboxlabs-netbox-branching`

## Package Installation

Download the wheel or source archive from the GitHub release for this repository and install it into the same Python environment as NetBox:

```bash
pip install /path/to/forward_netbox-0.1.0-py3-none-any.whl
```

Alternatively, install directly from the source archive:

```bash
pip install /path/to/forward_netbox-0.1.0.tar.gz
```

## Release Compatibility

| Plugin Release | NetBox Version | Status |
| --- | --- | --- |
| `v0.1.0` | `4.5.x` only | Initial release, unsupported |

## Support Disclaimer

This plugin is provided at your own risk. It is an unsupported initial release with no support commitment or compatibility guarantee beyond NetBox 4.5.x.

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
