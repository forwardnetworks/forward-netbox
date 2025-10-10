---
description: Overview of the IP Fabric NetBox plugin, its capabilities, compatibility, and use cases.
---

# NetBox plugin Overview

The IP Fabric NetBox plugin enables data synchronization between IP Fabric and NetBox platforms. This plugin serves as a bridge between network discovery (IP Fabric) and source of truth (NetBox), allowing network engineers and operators to maintain a single source of truth for network infrastructure data.

## Key Features

### Data Synchronization
The plugin leverages the [IP Fabric Python SDK](https://gitlab.com/ip-fabric/integrations/python-ipfabric) to collect comprehensive network data from IP Fabric instances. It then transforms this data into NetBox's data model, enabling automated documentation of network infrastructure.

### Branches
Built on [NetBox Branching](https://docs.netboxlabs.com/netbox-extensions/branching/) feature, the plugin allows review of proposed changes before they are applied to the production database, providing an additional layer of validation and control.

### Scheduled Operations
Administrators can configure automated, periodic synchronization jobs to ensure NetBox data remains current with the actual network state discovered by IP Fabric.

### Visual Differencing
The plugin provides visual differencing capabilities to easily identify changes between synchronization operations, making it simple to track network infrastructure evolution over time.

### Multiple Source Support
Connect to multiple IP Fabric instances simultaneously, allowing for comprehensive network visibility across different environments or network segments.

## Use Cases

- **Network Documentation Automation**: Automatically document network devices, interfaces, IP addresses, and VLANs discovered by IP Fabric
- **Configuration Validation**: Compare intended network state (NetBox) with actual state (IP Fabric)
- **Change Management**: Review and approve network changes through the staged changes workflow
- **Network Inventory Management**: Maintain accurate inventory of network devices and components
- **IP Address Management**: Synchronize IP address allocation and usage information

## NetBox Compatibility

The plugin requires specific NetBox versions due to API changes and feature dependencies. Please ensure compatibility before installation.

| NetBox Version | Plugin Version | Release Date | Key Features                                   |
|----------------|----------------|--------------|------------------------------------------------|
| 4.4.0 and up   | 4.3.0 and up   | 2025-09-23   | NetBox 4.4, REST API and GraphQL               |
| 4.3.0 - 4.3.7  | 4.2.2          | 2025-09-12   | Fix for NetBox 4.3.7 bug in Source view        |
| 4.3.0 - 4.3.6  | 4.0.0 - 4.2.1  | 2025-05-27   | NetBox 4.3 support and netbox-branching plugin |
| 4.2.4 - 4.2.9  | 3.2.2 - 3.2.4  | 2025-03-15   | Enhanced transform maps, improved performance  |
| 4.2.0 - 4.2.3  | 3.2.0 - 3.2.1  | 2025-02-01   | NetBox 4.2 compatibility updates               |
| 4.1.5 - 4.1.11 | 3.1.1 - 3.1.3  | 2024-12-10   | Bug fixes, stability improvements              |
| 4.1.0 - 4.1.4  | 3.1.0          | 2024-11-05   | NetBox 4.1 compatibility                       |
| 4.0.1          | 3.0.1 - 3.0.3  | 2024-09-20   | Bug fixes                                      |
| 4.0.0          | 3.0.0          | 2024-08-15   | Major rewrite for NetBox 4.0                   |
| 3.7.0 - 3.7.8  | 2.0.0 - 2.0.6  | 2024-05-10   | Enhanced data synchronization                  |
| 3.4.0 - 3.6.9  | 1.0.0 - 1.0.11 | 2023-11-01   | Initial release                                |

## Important Notes

!!! danger "Installation Location"
    Integrations should be installed outside of the IP Fabric VM unless the IP
    Fabric team explicitly instructs otherwise.

!!! warning "System Access"
    Any action on the Command-Line Interface (CLI) using the `root`, `osadmin`,
    or `autoboss` account may cause irreversible, detrimental changes to the
    product. Actions taken without direct communication with the IP Fabric
    Support or Solution Architect teams can render the system unusable.

## Screenshots from NetBox UI

### Image 1: IP Fabric Source Details
This image shows the details page for an IP Fabric source named `IPFabric-Demo` in the NetBox interface. The page displays key configuration information including:

- Source type: `Local`
- Status: `New`
- URL: `https://demo2.eu.ipfabric.io`
- Authentication credentials (masked with asterisks)
- Verification and timeout settings
- No tags or comments have been assigned
- The source was created and last updated on May 22, 2025

The left sidebar shows the hierarchical navigation menu for the NetBox interface with IP Fabric functionality integrated as a section. Action buttons at the top allow for syncing, bookmarking, subscribing, editing, or deleting the source.
![IP Fabric source configuration page showing connection details and status](images/user_guide/source_synced.png)

### Image 2: IP Fabric Snapshots Management
This image displays the IP Fabric Snapshots management page, showing 6 snapshots from the IPFabric-Demo source. The snapshots are listed chronologically with the following details:

- Name (mostly following `netlab - [snapshot type]` naming convention)
- Source (all from `IPFabric Test`)
- Snapshot ID (unique identifiers)
- Status (`Loaded`/`Unloaded`)
- Date (ranging from April 2024 to July 2024)

The snapshots include various types such as `Post Change` and `Day 3`. Each snapshot has a delete option, and there's a `Delete Selected` button at the bottom for batch operations. The table is configurable and supports filtering and searching.

![IP Fabric snapshots list showing multiple snapshot entries with their details](images/user_guide/source_snapshots.png)

### Image 3: IP Fabric Sync Details
This image shows an active sync configuration named `IPF Demo2 Sync`. The page displays:

Sync information section showing:

- Name: `IPF Demo2 Sync`
- Source: `IPFabric Test`
- Snapshot: `04-Jul-24 08:00:01`
- Status: `Syncing`
- Latest branch timestamp
- No schedule or interval set
- User: `netbox`

Parameters section listing various network elements being ingested:

- All key network parameters (VRF, site, vlan, device, etc.) are set to `True`
- Sites parameter shows an empty array `[]`
- Groups parameter is set to `['Debugging', 'Site A']` in this order

The page has action buttons for `Adhoc Ingestion`, `Edit`, and `Delete` operations. There are also tabs for `IP Fabric Sync`, `Tranform Maps`, `Ingestions`, and `Changelog` with job information.

![IP Fabric sync configuration page showing sync status and parameters](images/user_guide/sync_detail.png)
