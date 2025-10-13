---
description: v1 Release Notes
---

# v1 Release Notes

## v1.0.0 (2025-10-13)

### Highlights

- First public release of the Forward ↔ NetBox integration rebuilt on the Forward Enterprise REST / NQE API.
- Multiple source support with per-source Forward network identifiers and snapshot discovery.
- Branch-aware ingestion pipeline for manufacturers, roles, device types, locations, devices, and interfaces.
- Configurable NQE map defaults plus per-sync overrides from the NetBox UI.
- Full REST and GraphQL coverage for Forward sources, snapshots, syncs, and ingestion issues.

### Upgrade Notes

- NetBox 4.4.0 or later is required along with the `netboxlabs-netbox-branching` plugin.
- Populate `PLUGINS_CONFIG["forward_netbox"]` with Forward API endpoint, token, and optional network ID. All interactions occur via the API—no Forward SDK packages are required.
