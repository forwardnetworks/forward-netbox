---
description: Data Mapping and how it works.
---

# Data Mapping

!!! note
    This information is based on the latest version of the IP Fabric NetBox plugin.

This document outlines the tables from IP Fabric that are imported into NetBox and their corresponding endpoints, including the specific properties that are mapped.


## Data Sources

| IP Fabric Table | IP Fabric Endpoint | NetBox Model | NetBox App |
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
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `siteName` | `name` |
| `siteName` | `slug` |

### Device
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `hostname` | `name` |
| `sn` | `serial` |
| `siteName` | `site` (relationship) |
| `model` | `device_type` (relationship) |
| `devType` | `role` (relationship) |
| `virtual_chassis.member` | `vc_position` |
| `virtual_chassis` | `virtual_chassis` (relationship) |

### Manufacturer
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `vendor` | `name` |
| `vendor` | `slug` |

### Device Type
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `model` | `model` |
| `model` | `slug` |
| `vendor` | `manufacturer` (relationship) |

### Device Role
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `devType` | `name` |
| `devType` | `slug` |
| N/A | `vm_role` (set to False) |

### Platform
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `family` or `vendor` | `name` |
| `vendor` + `family` | `slug` |
| `vendor` | `manufacturer` (relationship) |

### Interface
| IP Fabric Property | NetBox Property |
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
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `mac` | `mac_address` |
| `id` | `assigned_object_id` |
| N/A | `assigned_object_type` (set to Interface) |

### Inventory Item
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `pid` | `part_id` |
| `sn` | `serial` |
| `name` or `dscr` | `name` |
| `deviceSn` | `device` (relationship) |
| `vendor` | `manufacturer` (relationship) |

### VLAN
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `vlanName` | `name` |
| `dscr` | `description` |
| `vlanId` | `vid` |
| `siteName` | `site` (relationship) |

### VRF
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `vrf` | `name` |
| `rd` | `rd` |

### Prefix
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `net` | `prefix` |
| `siteName` | `scope_id` |
| `vrf` | `vrf` (relationship) |
| N/A | `scope_type` (set to Site) |

### IP Address
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `ip` + `net` | `address` |
| `sn` + `nameOriginal` | `assigned_object_id` |
| N/A | `assigned_object_type` (set to Interface) |
| `vrf` | `vrf` (relationship) |

### Virtual Chassis
| IP Fabric Property | NetBox Property |
|-------------------|----------------|
| `master` | `name` |
| `sn` | `master` (relationship to Device) |

## Data Transformation

Data is transformed from IP Fabric to NetBox using transform maps that define:
- Source fields from IP Fabric
- Target fields in NetBox
- Jinja2 templates to transform source fields to target fields
- Relationship mappings between models

## Sync Process

1. Data is collected from IP Fabric API
2. Transform maps convert IP Fabric data format to NetBox format
3. Data is synced to NetBox ingestion
4. Data is merged from NetBox ingestion to the main database
