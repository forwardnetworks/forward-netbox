# Forward NetBox Integration

This repository packages the Forward Networks data source for NetBox as a
standalone plugin. It connects NetBox to Forward's network assurance platform,
ingesting inventory from Forward Enterprise (REST and NQE APIs), staging the
changes in [netbox-branching](https://docs.netboxlabs.com/netbox-extensions/branching/)
branches, and giving operators tooling to review, diff, and merge the updates.

Version **4.3.0** is the initial public release of the modernized plugin. It
supersedes the legacy Forward ↔ NetBox integration that previously shipped with
Forward appliances.

## Features

- Multiple Forward sources (local API connectivity or remote, pre-loaded feeds)
- Snapshot discovery with filtering by Forward network ID and snapshot state
- NQE-backed ingestion covering manufacturers, roles, device types, locations,
  devices, and interfaces
- Branching-aware imports with optional auto-merge after review
- Built-in scheduling, logging, and RQ job orchestration
- REST API, GraphQL schema, and tables/forms consistent with NetBox UX

## Compatibility

These combinations are validated and supported:

| NetBox version  | Plugin version |
|-----------------|----------------|
| 4.4.0 and later | 4.3.0+         |
| 4.3.0 – 4.3.7   | 4.2.2          |
| 4.3.0 – 4.3.6   | 4.0.0 – 4.2.1  |
| 4.2.4 – 4.2.9   | 3.2.2 – 3.2.4  |
| 4.2.0 – 4.2.3   | 3.2.0          |
| 4.1.5 – 4.1.11  | 3.1.1 – 3.1.3  |
| 4.1.0 – 4.1.4   | 3.1.0          |
| 4.0.1           | 3.0.1 – 3.0.3  |
| 4.0.0           | 3.0.0          |
| 3.7.0 – 3.7.8   | 2.0.0 – 2.0.6  |
| 3.4.0 – 3.6.9   | 1.0.0 – 1.0.11 |

## Installation

_This plugin is distributed from source (it is not published on PyPI)._ 

1. Install the plugin into the same virtual environment as NetBox:

   ```bash
   git clone https://github.com/forwardnetworks/forward-netbox.git
   cd forward-netbox
   pip install -e .
   ```

2. Add the plugin (and its netbox-branching dependency) to `PLUGINS` in
   `netbox/configuration.py`:

   ```python
   from netbox.core.settings_funcs import is_truthy
   import os

   PLUGINS = [
       "forward_netbox",
       "netbox_branching",
   ]

   PLUGINS_CONFIG = {
       "forward_netbox": {
           "base_url": os.getenv("FORWARD_NETBOX_BASE_URL"),
           "auth": os.getenv("FORWARD_NETBOX_API_TOKEN"),
           "network_id": os.getenv("FORWARD_NETBOX_NETWORK_ID"),
           "verify": is_truthy(os.getenv("FORWARD_NETBOX_SSL_VERIFY", "true")),
           "timeout": os.getenv("FORWARD_NETBOX_TIMEOUT"),
           "snapshot_id": os.getenv("FORWARD_NETBOX_SNAPSHOT_ID"),
       },
   }
   ```

3. Configure [netbox-branching](https://docs.netboxlabs.com/netbox-extensions/branching/)
   by wrapping `DATABASES` in a `DynamicSchemaDict` and enabling the
   `BranchAwareRouter` (see
   [installation docs](docs/01_User_Guide/installation.md#24-configure-database-router-to-support-branching)
   for the exact snippet).

4. Run the usual post-install commands:

   ```bash
   python3 manage.py migrate
   python3 manage.py collectstatic --no-input
   ```

5. Restart NetBox services (Django, rqworker, rqworker.high, etc.).

Populate the environment variables referenced above (for example in
`/opt/netbox/.env`):

```dotenv
FORWARD_NETBOX_BASE_URL=https://fwd.example.com
FORWARD_NETBOX_API_TOKEN=pk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
FORWARD_NETBOX_NETWORK_ID=12345
FORWARD_NETBOX_SSL_VERIFY=true
FORWARD_NETBOX_TIMEOUT=60
FORWARD_NETBOX_SNAPSHOT_ID=$last
```

## Getting Started

1. Navigate to **Plugins → Forward Networks → Sources** and create a Forward
   source. Provide the base URL, API token, optional network ID, and timeout /
   certificate verification settings. Remote sources can reference snapshots
   synced from another NetBox instance.
2. Use the **Sync Snapshots** action to discover the Forward snapshots for that
   source. Only snapshots in a `loaded`/`done` state are staged.
3. Create a **Forward Sync** referencing the snapshot to ingest, choose the
   site scope (or leave empty to import all sites), and review the toggles for
   each DCIM dataset. You can override default NQE query IDs directly in the
   form.
4. Run the sync immediately or schedule it. Each execution provisions a
   netbox-branching branch, applies the data, and leaves the branch ready for
   review/merge. Enable “auto merge” to queue a merge job after a successful
   import.

## Forward NQE Queries

Default Forward NQE identifiers live in
[`forward_netbox/data/nqe_map.json`](forward_netbox/data/nqe_map.json) and cover
the following datasets:

| Model key          | Description                       |
|--------------------|-----------------------------------|
| `dcim.manufacturer`| Vendor catalog                    |
| `dcim.devicerole`  | Device role taxonomy              |
| `dcim.devicetype`  | Hardware models                   |
| `dcim.location`    | Site hierarchy / locations        |
| `dcim.device`      | Device inventory                  |
| `dcim.interface`   | Physical and logical interfaces   |

Override these defaults in two ways:

- **Forward NQE Queries** (Plugins → Forward Networks → NQE Queries) lets you
  persist query IDs per NetBox model so that every sync inherits them.
- Individual **Forward Syncs** allow per-run overrides via the form fields
  generated from the default map.

The plugin executes each NQE query in order, batches results, and maps Forward
fields onto NetBox objects. Manufacturers, roles, device types, and locations
are created before devices and interfaces to satisfy foreign key relationships.

## API and Automation Surface

- REST endpoints under `/api/plugins/forward/` cover sources, snapshots,
  syncs/ingestions, issues, and NQE query mappings.
- A GraphQL schema (`plugins/forward/graphql/`) exposes the same models for
  automation-friendly queries and filtering.
- Template content, navigation items, and table views mirror NetBox patterns,
  making it easy to add the plugin to dashboards or embed partials elsewhere.

Full documentation (administrator and user guides) is published at
[forwardnetworks.com/docs/forward-netbox](https://forwardnetworks.com/docs/forward-netbox).

## Development

### Requirements

- Python 3.10+
- NetBox 4.4+ with `netboxlabs-netbox-branching` 0.7.0
- `poetry`, `invoke`, and `pre-commit` for local workflows
- Docker (optional) for the provided development environment

### Local setup

```bash
git clone https://github.com/forwardnetworks/forward-netbox.git
cd forward-netbox
python -m venv venv && source venv/bin/activate
pip install poetry
poetry install --with dev
pre-commit install
```

Spin up the demo NetBox stack (Postgres, Redis, rqworker, NetBox) with:

```bash
invoke build
invoke start
invoke createsuperuser
```

Visit http://localhost:8000 to log in and test the plugin.

### Testing

Run the unit tests inside the NetBox container (or against your configured
`DJANGO_SETTINGS_MODULE`):

```bash
invoke test
```

You can also call `pytest` directly once NetBox is on `PYTHONPATH`:

```bash
pytest
```

The current suite focuses on form validation and NQE map handling—extend it as
new functionality is added.

## Current Status / Roadmap

- ✅ Forward source discovery and snapshot synchronization
- ✅ Branch-based ingestion for manufacturers, roles, device types, locations,
  devices, and interfaces
- ✅ REST and GraphQL APIs plus job tracking/logging
- 🚧 Expand IPAM coverage (prefixes, managed IPs, VLANs) and topology exports
- 🚧 Harden integration tests against live Forward Enterprise environments

## Maintainer

Craig Johnson (Principal Solutions Architect, Forward Networks) — GitHub:
[@captainpacket](https://github.com/captainpacket),
[craigjohnson@forwardnetworks.com](mailto:craigjohnson@forwardnetworks.com).

## License

Released under the [MIT License](LICENSE). Forward Networks retains copyright for
the plugin source and associated branding assets.
