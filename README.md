# IP Fabric Netbox Plugin

## IP Fabric

IP Fabric is a vendor-neutral network assurance platform that automates the
holistic discovery, verification, visualization, and documentation of
large-scale enterprise networks, reducing the associated costs and required
resources whilst improving security and efficiency.

It supports your engineering and operations teams, underpinning migration and
transformation projects. IP Fabric will revolutionize how you approach network
visibility and assurance, security assurance, automation, multi-cloud
networking, and trouble resolution.

**Integrations or scripts should not be installed directly on the IP Fabric VM unless directly communicated from the
IP Fabric Support or Solution Architect teams.  Any action on the Command-Line Interface (CLI) using the root, osadmin,
or autoboss account may cause irreversible, detrimental changes to the product and can render the system unusable.**

## Overview

This plugin allows the integration and data synchronization between IP Fabric and NetBox.

The plugin uses IP Fabric collect network data utilizing the [IP Fabric Python SDK](https://gitlab.com/ip-fabric/integrations/python-ipfabric). This plugin relies on helpful features in NetBox like [Branches](https://docs.netboxlabs.com/netbox-extensions/branching/) and [Background Tasks](https://netboxlabs.com/docs/netbox/en/stable/plugins/development/background-tasks/) to make the job of bringing in data to NetBox easier.

- Multiple IP Fabric Sources
- Transform Maps
- Scheduled Synchronization
- Diff Visualization

## NetBox Compatibility
These are the required NetBox versions for corresponding plugin version. Any other versions won't work due to breaking changes in NetBox codebase.

| Netbox Version | Plugin Version |
|----------------|----------------|
| 4.4.0 and up   | 4.3.0 and up   |
| 4.3.0 - 4.3.7  | 4.2.2          |
| 4.3.0 - 4.3.6  | 4.0.0 - 4.2.1  |
| 4.2.4 - 4.2.9  | 3.2.2 - 3.2.4  |
| 4.2.0 - 4.2.3  | 3.2.0          |
| 4.1.5 - 4.1.11 | 3.1.1 - 3.1.3  |
| 4.1.0 - 4.1.4  | 3.1.0          |
| 4.0.1          | 3.0.1 - 3.0.3  |
| 4.0.0          | 3.0.0          |
| 3.7.0 - 3.7.8  | 2.0.0 - 2.0.6  |
| 3.4.0 - 3.6.9  | 1.0.0 - 1.0.11 |

## Screenshots

![Source](docs/images/user_guide/source_sync.png)

![Snapshots](docs/images/user_guide/snapshot_detail.png)

![Transform Maps](docs/images/user_guide/tm_edit_hostname.png)

![Ingesion](docs/images/user_guide/ingestion_detail.png)

![Diff](docs/images/user_guide/branch_changes_update_diff.png)

## Documentation

Full documentation for this plugin can be found at [IP Fabric Docs](https://docs.ipfabric.io/main/integrations/netbox/).

- User Guide
- Administrator Guide

## Contributing

If you would like to contribute to this plugin, please see the [CONTRIBUTING.md](CONTRIBUTING.md) file.
