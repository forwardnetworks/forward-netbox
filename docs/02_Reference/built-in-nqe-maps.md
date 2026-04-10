# Built-In NQE Maps

This reference lists the built-in NQE maps that ship with `forward_netbox`.

Each entry includes:

- the map name
- the target `NetBox Model`
- the expected output fields
- the shipped query file in the repository
- the exact shipped source text

All built-in maps are executed against the sync-selected Forward snapshot. The examples below are the shipped query source from this repository. Queries that import `netbox_utilities` are flattened by the plugin at execution time for bundled built-ins, but the source modules shown here can also be copied into the Forward Org Repository and tested by `query_id`.

## Summary

| Map | NetBox Model | Query File |
| --- | --- | --- |
| Forward Locations | `dcim.site` | [`forward_locations.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_locations.nqe) |
| Forward Device Vendors | `dcim.manufacturer` | [`forward_device_vendors.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_vendors.nqe) |
| Forward Device Types | `dcim.devicerole` | [`forward_device_types.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_types.nqe) |
| Forward Platforms | `dcim.platform` | [`forward_platforms.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_platforms.nqe) |
| Forward Device Models | `dcim.devicetype` | [`forward_device_models.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_models.nqe) |
| Forward Devices | `dcim.device` | [`forward_devices.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_devices.nqe) |
| Forward Virtual Chassis | `dcim.virtualchassis` | [`forward_virtual_chassis.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_virtual_chassis.nqe) |
| Forward Interfaces | `dcim.interface` | [`forward_interfaces.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_interfaces.nqe) |
| Forward MAC Addresses | `dcim.macaddress` | [`forward_mac_addresses.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_mac_addresses.nqe) |
| Forward VLANs | `ipam.vlan` | [`forward_vlans.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_vlans.nqe) |
| Forward VRFs | `ipam.vrf` | [`forward_vrfs.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_vrfs.nqe) |
| Forward IPv4 Prefixes | `ipam.prefix` | [`forward_prefixes_ipv4.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_prefixes_ipv4.nqe) |
| Forward IPv6 Prefixes | `ipam.prefix` | [`forward_prefixes_ipv6.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_prefixes_ipv6.nqe) |
| Forward IP Addresses | `ipam.ipaddress` | [`forward_ip_addresses.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_ip_addresses.nqe) |
| Forward Inventory Items | `dcim.inventoryitem` | [`forward_inventory_items.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_inventory_items.nqe) |

## Shared Module

- Shared helper module: [`netbox_utilities.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/netbox_utilities.nqe)
- Purpose: centralizes slug shaping plus the manufacturer override table used by the manufacturer-bearing maps.
- Customization note: if your NetBox already uses different curated manufacturer rows, copy the query set and adjust `manufacturer_name_overrides` in this shared module before syncing.

```nqe
manufacturer_name_overrides = [
  { vendor: Vendor.A10, name: "A10" },
  { vendor: Vendor.AMAZON, name: "Amazon" },
  { vendor: Vendor.ARISTA, name: "Arista" },
  { vendor: Vendor.ARUBA, name: "Aruba" },
  { vendor: Vendor.AVAYA, name: "Avaya" },
  { vendor: Vendor.AVI_NETWORKS, name: "Avi Networks" },
  { vendor: Vendor.AZURE, name: "Microsoft" },
  { vendor: Vendor.BLUECAT, name: "BlueCat" },
  { vendor: Vendor.BROCADE, name: "Brocade" },
  { vendor: Vendor.CHECKPOINT, name: "Check Point" },
  { vendor: Vendor.CISCO, name: "Cisco" },
  { vendor: Vendor.CITRIX, name: "Citrix" },
  { vendor: Vendor.CUMULUS, name: "Cumulus" },
  { vendor: Vendor.DELL, name: "Dell" },
  { vendor: Vendor.EDGE_CORE, name: "Edge Core" },
  { vendor: Vendor.EXTREME, name: "Extreme Networks" },
  { vendor: Vendor.F5, name: "F5" },
  { vendor: Vendor.FORCEPOINT, name: "Forcepoint" },
  { vendor: Vendor.FORTINET, name: "Fortinet" },
  { vendor: Vendor.GENERAL_DYNAMICS, name: "General Dynamics" },
  { vendor: Vendor.GOOGLE, name: "Google" },
  { vendor: Vendor.HP, name: "HPE" },
  { vendor: Vendor.HUAWEI, name: "Huawei" },
  { vendor: Vendor.JUNIPER, name: "Juniper" },
  { vendor: Vendor.LINUX_GENERIC, name: "Linux" },
  { vendor: Vendor.NOKIA, name: "Nokia" },
  { vendor: Vendor.PALO_ALTO_NETWORKS, name: "Palo Alto Networks" },
  { vendor: Vendor.PENSANDO, name: "Pensando" },
  { vendor: Vendor.PICA8, name: "Pica8" },
  { vendor: Vendor.RIVERBED, name: "Riverbed" },
  { vendor: Vendor.SILVER_PEAK, name: "Silver Peak" },
  { vendor: Vendor.SYMANTEC, name: "Symantec" },
  { vendor: Vendor.T128, name: "128T" },
  { vendor: Vendor.UNKNOWN, name: "Unknown" },
  { vendor: Vendor.VERSA, name: "Versa" },
  { vendor: Vendor.VIASAT, name: "Viasat" },
  { vendor: Vendor.VMWARE, name: "VMware" },
  { vendor: Vendor.ALKIRA, name: "Alkira" }
];

canonicalManufacturerOverride(vendor: Vendor) =
  max(
    foreach mapping in manufacturer_name_overrides
    where mapping.vendor == vendor
    select mapping.name
  );

export canonicalManufacturerName(vendor: Vendor) =
  if isPresent(canonicalManufacturerOverride(vendor))
  then canonicalManufacturerOverride(vendor)
  else replace(replace(toString(vendor), "Vendor.", ""), "_", " ");

export slugify(value: String) =
  replaceRegexMatches(
    replaceRegexMatches(
      replace(toLowerCase(value), "&", " and "),
      re`[^a-z0-9]+`,
      "-"
    ),
    re`^-+|-+$`,
    ""
  );
```

## Forward Locations

- `NetBox Model`: `dcim.site`
- Expected fields: `name`, `slug`, `status`, `physical_address`, `comments`
- Query file: [`forward_locations.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_locations.nqe)

```nqe
import "netbox_utilities";

deviceLocations =
  foreach d in network.devices
  select distinct d.locationName;

foreach location in network.locations
where location.name in deviceLocations
let location_name = toLowerCase(location.name)
let location_slug = slugify(location_name)
let address = join(", ", [if isPresent(location.city)
                          then location.city
                          else "city unknown",
                          if isPresent(location.country)
                          then location.country
                          else "country unknown"
                         ])
select {
  name: location_name,
  slug: location_slug,
  status: "active",
  physical_address: address,
  comments: "Site added or Updated by Forward Enterprise"
}
```

## Forward Device Vendors

- `NetBox Model`: `dcim.manufacturer`
- Expected fields: `name`, `slug`
- Query file: [`forward_device_vendors.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_vendors.nqe)
- Built-in behavior: canonicalizes Forward vendor enums into NetBox-ready manufacturer names and slugs directly in NQE.
- Customization note: if your NetBox already uses different curated manufacturer rows, copy this query set and update `manufacturer_name_overrides` in `netbox_utilities`.

```nqe
import "netbox_utilities";

foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
let vendor = device.platform.vendor
let manufacturer_name = canonicalManufacturerName(vendor)
let manufacturer_slug = slugify(manufacturer_name)
select distinct {
  name: manufacturer_name,
  slug: manufacturer_slug
}
```

## Forward Device Types

- `NetBox Model`: `dcim.devicerole`
- Expected fields: `name`, `slug`, `color`
- Query file: [`forward_device_types.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_types.nqe)

```nqe
import "netbox_utilities";

foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
let device_type = device.platform.deviceType
let role_name = replace(toString(device_type), "DeviceType.", "")
let role_slug = slugify(role_name)
select distinct {
  name: device_type,
  slug: role_slug,
  color: "9e9e9e"
}
```

## Forward Platforms

- `NetBox Model`: `dcim.platform`
- Expected fields: `name`, `manufacturer`, `manufacturer_slug`, `slug`
- Query file: [`forward_platforms.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_platforms.nqe)

```nqe
import "netbox_utilities";

foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
let platform_name = replace(toString(device.platform.os), "OS.", "")
let platform_slug = slugify(platform_name)
let manufacturer_name = canonicalManufacturerName(device.platform.vendor)
let manufacturer_slug = slugify(manufacturer_name)
select distinct {
  name: platform_name,
  manufacturer: manufacturer_name,
  manufacturer_slug: manufacturer_slug,
  slug: platform_slug
}
```

## Forward Device Models

- `NetBox Model`: `dcim.devicetype`
- Expected fields: `manufacturer`, `manufacturer_slug`, `model`, `part_number`, `slug`
- Query file: [`forward_device_models.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_models.nqe)

```nqe
import "netbox_utilities";

foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
let vendor = device.platform.vendor
let model = device.platform.model
let model_slug = slugify(toString(model))
let manufacturer_name = canonicalManufacturerName(vendor)
let manufacturer_slug = slugify(manufacturer_name)
select distinct {
  manufacturer: manufacturer_name,
  manufacturer_slug: manufacturer_slug,
  model: model,
  part_number: model,
  slug: model_slug
}
```

## Forward Devices

- `NetBox Model`: `dcim.device`
- Expected fields: `name`, `manufacturer`, `manufacturer_slug`, `device_type`, `device_type_slug`, `site`, `site_slug`, `role`, `role_slug`, `role_color`, `platform`, `platform_slug`, `status`
- Query file: [`forward_devices.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_devices.nqe)

```nqe
import "netbox_utilities";

foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
let location = device.locationName
let model = device.platform.model
let device_type = device.platform.deviceType
let site_name = if isPresent(location) then toLowerCase(location) else "unknown"
let site_slug = slugify(site_name)
let role_name = replace(toString(device_type), "DeviceType.", "")
let role_slug = slugify(role_name)
let platform_name = replace(toString(device.platform.os), "OS.", "")
let platform_slug = slugify(platform_name)
let device_type_slug = slugify(toString(model))
let manufacturer_name = canonicalManufacturerName(device.platform.vendor)
let manufacturer_slug = slugify(manufacturer_name)
select {
  name: device.name,
  manufacturer: manufacturer_name,
  device_type: model,
  device_type_slug: device_type_slug,
  site: site_name,
  site_slug: site_slug,
  role: device_type,
  role_slug: role_slug,
  role_color: "9e9e9e",
  platform: platform_name,
  platform_slug: platform_slug,
  status: "active",
  manufacturer_slug: manufacturer_slug
}
```

## Forward Virtual Chassis

- `NetBox Model`: `dcim.virtualchassis`
- Expected fields: `device`, `vc_name`, `name`, `vc_domain`
- Query file: [`forward_virtual_chassis.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_virtual_chassis.nqe)
- Current semantics: emits virtual chassis rows for Forward HA `vpc` domains and `mlagPeer` pairs.

```nqe
foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
let has_vpc = isPresent(device.ha) && isPresent(device.ha.vpc) && isPresent(device.ha.vpc.domainId) && device.ha.vpc.domainId > 0
let has_mlag_peer = isPresent(device.ha) && isPresent(device.ha.mlagPeer)
let mlag_peer_name = if has_mlag_peer then toString(device.ha.mlagPeer) else ""
where has_vpc || has_mlag_peer
let site_name = if isPresent(device.locationName) then toLowerCase(device.locationName) else "unknown"
let mlag_members = if has_mlag_peer then
  if mlag_peer_name > device.name
  then [device.name, mlag_peer_name]
  else [mlag_peer_name, device.name]
else [device.name]
select {
  device: device.name,
  vc_name: if has_vpc
    then join("-", [site_name, "vpc", toString(device.ha.vpc.domainId)])
  else
    join("-", [site_name, "mlag", join("--", mlag_members)]),
  vc_domain: if has_vpc
    then toString(device.ha.vpc.domainId)
  else
    join("--", mlag_members)
}
```

## Forward Interfaces

- `NetBox Model`: `dcim.interface`
- Expected fields: `device`, `name`, `type`, `enabled`, `mtu`, `description`, `speed`
- Query file: [`forward_interfaces.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_interfaces.nqe)

```nqe
foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach interface in device.interfaces
where interface.interfaceType == IfaceType.IF_ETHERNET
let speed = interface.ethernet.negotiatedPortSpeed
let speed_key = toString(speed)
let ethernet_by_speed = [
  { key: "PortSpeed.SPEED_10MB", type: "other", speed: 10000 },
  { key: "PortSpeed.SPEED_100MB", type: "100base-tx", speed: 100000 },
  { key: "PortSpeed.SPEED_1GB", type: "1000base-t", speed: 1000000 },
  { key: "PortSpeed.SPEED_2500MB", type: "2.5gbase-t", speed: 2500000 },
  { key: "PortSpeed.SPEED_5GB", type: "5gbase-t", speed: 5000000 },
  { key: "PortSpeed.SPEED_10GB", type: "10gbase-t", speed: 10000000 },
  { key: "PortSpeed.SPEED_25GB", type: "25gbase-x-sfp28", speed: 25000000 },
  { key: "PortSpeed.SPEED_40GB", type: "40gbase-x-qsfpp", speed: 40000000 },
  { key: "PortSpeed.SPEED_50GB", type: "50gbase-x-sfp56", speed: 50000000 },
  { key: "PortSpeed.SPEED_100GB", type: "100gbase-x-qsfp28", speed: 100000000 }
]
let interface_type = max(foreach profile in ethernet_by_speed
  where profile.key == speed_key
  select profile.type)
let interface_speed = max(foreach profile in ethernet_by_speed
  where profile.key == speed_key
  select profile.speed)
select {
  device: device.name,
  name: interface.name,
  type: if isPresent(interface_type) then interface_type else "other",
  enabled: interface.operStatus == OperStatus.UP,
  mtu: interface.mtu,
  description: if isPresent(interface.description) then interface.description else "",
  speed: if isPresent(interface_speed) then interface_speed else null : Integer
}
```

## Forward MAC Addresses

- `NetBox Model`: `dcim.macaddress`
- Expected fields: `device`, `interface`, `mac`, `mac_address`
- Query file: [`forward_mac_addresses.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_mac_addresses.nqe)

```nqe
foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach interface in device.interfaces
where interface.interfaceType == IfaceType.IF_ETHERNET
where isPresent(interface.ethernet.macAddress)
select {
  device: device.name,
  interface: interface.name,
  mac: toString(interface.ethernet.macAddress)
}
```

## Forward VLANs

- `NetBox Model`: `ipam.vlan`
- Expected fields: `site`, `site_slug`, `vid`, `name`, `status`
- Query file: [`forward_vlans.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_vlans.nqe)

```nqe
import "netbox_utilities";

foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach ni in device.networkInstances
foreach vlan in ni.vlans
let site_name = if isPresent(device.locationName) then toLowerCase(device.locationName) else "unknown"
let site_slug = slugify(site_name)
select distinct {
  site: site_name,
  site_slug: site_slug,
  vid: vlan.vlanId,
  name: if isPresent(vlan.name) then vlan.name else join(" ", ["VLAN", toString(vlan.vlanId)]),
  status: "active"
}
```

## Forward VRFs

- `NetBox Model`: `ipam.vrf`
- Expected fields: `name`, `rd`, `description`, `enforce_unique`
- Query file: [`forward_vrfs.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_vrfs.nqe)

```nqe
foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach ni in device.networkInstances
where ni.name != "default"
where toString(ni.instanceType) != "NetworkInstanceType.DEFAULT_INSTANCE"
select distinct {
  name: ni.name,
  rd: null : String,
  description: "",
  enforce_unique: false
}
```

## Forward IPv4 Prefixes

- `NetBox Model`: `ipam.prefix`
- Expected fields: `vrf`, `prefix`, `status`
- Query file: [`forward_prefixes_ipv4.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_prefixes_ipv4.nqe)

```nqe
foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach ni in device.networkInstances
where isPresent(ni.afts?.ipv4Unicast?.ipEntries)
foreach entry in ni.afts.ipv4Unicast.ipEntries
where length(entry.prefix) > 0
where length(entry.prefix) <= 32
where !(length(entry.nextHops) > 0
  && length((foreach hop in entry.nextHops
    where hop.nextHopType != NextHopType.RECEIVE && hop.nextHopType != NextHopType.DROP
    select hop.nextHopType)) == 0
  && length(entry.prefix) == 32)
select {
  vrf: if ni.name != "default"
    then if toString(ni.instanceType) != "NetworkInstanceType.DEFAULT_INSTANCE" then ni.name else null : String
    else null : String,
  prefix: ipSubnet(networkAddress(entry.prefix), length(entry.prefix)),
  status: "active"
}
```

## Forward IPv6 Prefixes

- `NetBox Model`: `ipam.prefix`
- Expected fields: `vrf`, `prefix`, `status`
- Query file: [`forward_prefixes_ipv6.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_prefixes_ipv6.nqe)

```nqe
foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach ni in device.networkInstances
where isPresent(ni.afts?.ipv6Unicast?.ipEntries)
foreach entry in ni.afts.ipv6Unicast.ipEntries
where !(length(entry.nextHops) > 0
  && length((foreach hop in entry.nextHops
    where hop.nextHopType != NextHopType.RECEIVE && hop.nextHopType != NextHopType.DROP
    select hop.nextHopType)) == 0
  && length(entry.prefix) == 128)
select {
  vrf: if ni.name != "default"
    then if toString(ni.instanceType) != "NetworkInstanceType.DEFAULT_INSTANCE" then ni.name else null : String
    else null : String,
  prefix: ipSubnet(networkAddress(entry.prefix), length(entry.prefix)),
  status: "active"
}
```

## Forward IP Addresses

- `NetBox Model`: `ipam.ipaddress`
- Expected fields: `device`, `interface`, `vrf`, `address`, `status`
- Query file: [`forward_ip_addresses.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_ip_addresses.nqe)

The shipped query combines rows from subinterfaces, bridge interfaces, tunnels, and routed VLAN interfaces. See the query file for the complete text:

- [`forward_ip_addresses.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_ip_addresses.nqe)

## Forward Inventory Items

- `NetBox Model`: `dcim.inventoryitem`
- Expected fields: `device`, `manufacturer`, `manufacturer_slug`, `name`, `part_id`, `serial`, `role`, `role_slug`, `role_color`, `status`, `discovered`, `description`
- Query file: [`forward_inventory_items.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_inventory_items.nqe)

```nqe
import "netbox_utilities";

foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach component in device.platform.components
let manufacturer_name = canonicalManufacturerName(device.platform.vendor)
let manufacturer_slug = slugify(manufacturer_name)
let role_name = replace(replace(toString(component.partType), "DevicePartType.", ""), "_", " ")
let role_slug = slugify(role_name)
select {
  device: device.name,
  manufacturer: manufacturer_name,
  manufacturer_slug: manufacturer_slug,
  name: component.name,
  part_id: if isPresent(component.partId) then component.partId else component.name,
  serial: if isPresent(component.serialNumber) then component.serialNumber else if isPresent(component.description) then component.description else role_name,
  role: role_name,
  role_slug: role_slug,
  role_color: "9e9e9e",
  status: "active",
  discovered: true,
  description: if isPresent(component.description) then component.description else ""
}
```
