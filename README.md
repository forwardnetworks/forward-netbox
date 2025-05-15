# Forward Networks NetBox Plugin

## Forward Networks

Forward Networks is a vendor-neutral network assurance platform that automates the
holistic discovery, verification, visualization, and documentation of
large-scale enterprise networks, reducing the associated costs and required
resources whilst improving security and efficiency.

It supports your engineering and operations teams, underpinning migration and
transformation projects. Forward Networks will revolutionize how you approach network
visibility and assurance, security assurance, automation, multi-cloud
networking, and trouble resolution.

**Integrations or scripts should not be installed directly on the Forward Networks VM unless directly communicated from the
Forward Networks Support or Solution Architect teams. Any action on the Command-Line Interface (CLI) using the root, osadmin,
or autoboss account may cause irreversible, detrimental changes to the product and can render the system unusable.**

## Overview

This plugin allows the integration and data synchronization between Forward Networks and NetBox.

It uses Forward's Network Query Engine (NQE) to gather structured intent-based data directly from your modeled network,
and uses NetBox’s Staged Changes and Background Tasks capabilities to safely sync that data into NetBox.

### Key Features

- Multiple Forward Networks Sources
- Scheduled Ingestion (Adhoc or Periodic)
- Snapshot Browser with Metadata View
- Ingestion Parameter Selection per DCIM/IPAM model
- Full Job Log, Diff, and Change Preview via Staged Changes
- Snapshot ID and `$latestProcessed` support
- Integration with NetBox Branching (create → ingest → merge)

**Note:** Auto-merge and auto-delete functionality are not yet implemented in this release.

## NetBox Compatibility

| NetBox Version | Plugin Version |
|----------------|----------------|
| 4.3.0 and up   | 0.9.0 and up   |

## Contributing

Please see the [GitHub repo](https://github.com/forwardnetworks/forward-netbox) and [CONTRIBUTING.md](https://github.com/forwardnetworks/forward-netbox/blob/main/CONTRIBUTING.md) for contribution details.
