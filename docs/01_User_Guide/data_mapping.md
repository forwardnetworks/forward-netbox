---
description: Data Mapping and how it works.
---

# Data Mapping

!!! note
    This information is based on the latest version of the Forward Networks NetBox plugin.

This document outlines the tables from Forward Networks that are imported into NetBox and their corresponding endpoints, including the specific properties that are mapped.


## Data Sources

| Forward Networks Table | Forward Networks Endpoint | NetBox Model | NetBox App |
|----------------|-------------------|--------------|------------|
| Sites | `inventory.sites` | Site | `dcim` |
| Devices | `inventory.devices` | Device | `dcim` |
| Virtual Chassis | `technology.platforms.stacks_members` | VirtualChassis | `dcim` |
| Interfaces | `inventory.interfaces` | Interface | `dcim` |
| Part Numbers | `inventory.pn` | InventoryItem | `dcim` |
| VLANs | `technology.vlans.site_summary` | VLAN | `ipam` |
| VRFs | `technology.routing.vrf_detail` | VRF | `ipam` |
| Networks | `technology.managed_networks.networks` | Prefix | `ipam` |
| IP Addresses | `technology.addressing.managed_ip_ipv4` | IPAddress | `ipam` |

## Property Mappings

### Site
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `siteName` | `name` |
| `siteName` | `slug` |

### Device
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `hostname` | `name` |
| `sn` | `serial` |
| `siteName` | `site` (relationship) |
| `model` | `device_type` (relationship) |
| `devType` | `role` (relationship) |
| `virtual_chassis.member` | `vc_position` |
| `virtual_chassis` | `virtual_chassis` (relationship) |

### Manufacturer
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `vendor` | `name` |
| `vendor` | `slug` |

### Device Type
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `model` | `model` |
| `model` | `slug` |
| `vendor` | `manufacturer` (relationship) |

### Device Role
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `devType` | `name` |
| `devType` | `slug` |
| N/A | `vm_role` (set to False) |

### Platform
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `family` or `vendor` | `name` |
| `vendor` + `family` | `slug` |
| `vendor` | `manufacturer` (relationship) |

### Interface
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `nameOriginal` or `intName` | `name` |
| `dscr` | `description` |
| `mtu` | `mtu` |
| `media` | `type` |
| `l1` | `enabled` |
| `primaryIp` + `loginIp` | `mgmt_only` |
| `speedValue` | `speed` |
| `duplex` | `duplex` |
| `sn` | `device` (relationship) |

### MAC Address
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `mac` | `mac_address` |
| `id` | `assigned_object_id` |
| N/A | `assigned_object_type` (set to Interface) |

### Inventory Item
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `pid` | `part_id` |
| `sn` | `serial` |
| `name` or `dscr` | `name` |
| `deviceSn` | `device` (relationship) |
| `vendor` | `manufacturer` (relationship) |

### VLAN
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `vlanName` | `name` |
| `dscr` | `description` |
| `vlanId` | `vid` |
| `siteName` | `site` (relationship) |

### VRF
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `vrf` | `name` |
| `rd` | `rd` |

### Prefix
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `net` | `prefix` |
| `siteName` | `scope_id` |
| `vrf` | `vrf` (relationship) |
| N/A | `scope_type` (set to Site) |

### IP Address
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `ip` + `net` | `address` |
| `sn` + `nameOriginal` | `assigned_object_id` |
| N/A | `assigned_object_type` (set to Interface) |
| `vrf` | `vrf` (relationship) |

### Virtual Chassis
| Forward Networks Property | NetBox Property |
|-------------------|----------------|
| `master` | `name` |
| `sn` | `master` (relationship to Device) |

## Data Transformation

Each sync uses Forward Enterprise NQE queries to return JSON objects that already match the fields expected by NetBox. The plugin ships with a default mapping of NetBox models to NQE query identifiers, which can be customised per sync from the Forward sync form. The parameters panel on a sync shows the active query IDs together with their enabled state.

## Sync Process

1. The configured NQE queries are executed against the Forward Networks API.
2. Returned records are upserted into the NetBox staging branch for every enabled model.
3. After review, the ingestion can be merged into the primary NetBox database.
