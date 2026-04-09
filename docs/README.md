# Forward NetBox Documentation

`forward_netbox` connects NetBox directly to Forward Networks, executes NQE against a selected Forward snapshot, and stages the resulting changes in a NetBox branch for review and merge.

## Release Compatibility

| Plugin Release | NetBox Version | Status |
| --- | --- | --- |
| `v0.1.3` | `4.5.x` only | Current unsupported release |

## Support Disclaimer

This repository is provided for use at your own risk. It is an unsupported release and is not an officially supported Forward Networks product.

## What This Plugin Provides

- Direct Forward API connectivity with username and password authentication
- Dynamic network selection from the authenticated Forward tenant
- First-class `Sources`, `NQE Maps`, `Syncs`, and `Ingestions`
- Branch-backed review and merge flow through `netbox_branching`
- Built-in shipped NQE maps that can be used as-is or copied into custom map definitions
- Support for both `query_id` and raw `query` execution modes
- Snapshot selection per sync, including `latestProcessed`
- Snapshot details and Forward metrics recorded on each ingestion

## Start Here

- [Installation](01_User_Guide/README.md)
- [Configuration](01_User_Guide/configuration.md)
- [Usage and Validation](01_User_Guide/usage.md)
- [Troubleshooting](01_User_Guide/troubleshooting.md)
- [Built-In NQE Reference](02_Reference/built-in-nqe-maps.md)
- [Model Mapping Matrix](02_Reference/model-mapping-matrix.md)
- [TODO And Improvements](03_Plans/todo-and-improvements.md)
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

These screenshots reflect the current snapshot-aware workflow: source network on the source, snapshot selection on the sync, and snapshot details on the ingestion.

## Current Built-In Coverage

The shipped built-in NQE maps currently cover:

- Sites
- Manufacturers
- Device roles
- Platforms
- Device types
- Devices
- Virtual chassis
- Interfaces
- MAC addresses
- Inventory items
- VLANs
- VRFs
- IPv4 and IPv6 prefixes
- IP addresses
