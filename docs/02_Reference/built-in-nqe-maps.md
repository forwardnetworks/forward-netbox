# Built-In NQE Maps

This reference lists the built-in NQE maps that ship with `forward_netbox`.

Each entry includes:

- the map name
- the target `NetBox Model`
- the expected output fields
- the shipped query file in the repository
- the exact shipped source text

All built-in maps are executed against the sync-selected Forward snapshot. The shipped query set includes default maps that require no Forward data file and disabled data-file-aware variants that require the selected snapshot to expose fields such as `network.extensions.netbox_device_type_aliases.value` or `network.extensions.netbox_feature_tag_rules.value`. The examples below are the shipped query source from this repository. Queries that import `netbox_utilities` are flattened by the plugin at execution time for bundled built-ins, but the source modules shown here can also be copied into the Forward Org Repository and tested by `query_id`.

## Summary

| Map | NetBox Model | Query File |
| --- | --- | --- |
| Forward Locations | `dcim.site` | [`forward_locations.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_locations.nqe) |
| Forward Device Vendors | `dcim.manufacturer` | [`forward_device_vendors.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_vendors.nqe) |
| Forward Device Types | `dcim.devicerole` | [`forward_device_types.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_types.nqe) |
| Forward Platforms | `dcim.platform` | [`forward_platforms.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_platforms.nqe) |
| Forward Device Models | `dcim.devicetype` | [`forward_device_models.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_models.nqe) |
| Forward Device Models with NetBox Device Type Aliases | `dcim.devicetype` | [`forward_device_models_with_netbox_aliases.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_models_with_netbox_aliases.nqe) |
| Forward Devices | `dcim.device` | [`forward_devices.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_devices.nqe) |
| Forward Devices with NetBox Device Type Aliases | `dcim.device` | [`forward_devices_with_netbox_aliases.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_devices_with_netbox_aliases.nqe) |
| Forward Virtual Chassis | `dcim.virtualchassis` | [`forward_virtual_chassis.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_virtual_chassis.nqe) |
| Forward Device Feature Tags | `extras.taggeditem` | [`forward_device_feature_tags.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_feature_tags.nqe) |
| Forward Device Feature Tags with Rules | `extras.taggeditem` | [`forward_device_feature_tags_with_rules.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_feature_tags_with_rules.nqe) |
| Forward Interfaces | `dcim.interface` | [`forward_interfaces.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_interfaces.nqe) |
| Forward Inferred Interface Cables | `dcim.cable` | [`forward_inferred_interface_cables.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_inferred_interface_cables.nqe) |
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

export slugifyNetboxModel(value: String) =
  slugify(
    replace(
      replace(
        replace(value, "+", " plus "),
        "/", " slash "
      ),
      ".", " dot "
    )
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
- Default behavior: does not require a Forward data file.

```nqe
import "netbox_utilities";

foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
let vendor = device.platform.vendor
let model = device.platform.model
let model_slug = slugifyNetboxModel(toString(model))
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

## Forward Device Models with NetBox Device Type Aliases

- `NetBox Model`: `dcim.devicetype`
- Expected fields: `manufacturer`, `manufacturer_slug`, `model`, `part_number`, `slug`
- Query file: [`forward_device_models_with_netbox_aliases.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_models_with_netbox_aliases.nqe)
- Seed state: disabled by default.
- Requirement: Forward data file `netbox_device_type_aliases.json` with NQE name `netbox_device_type_aliases` must be uploaded, attached to the network, and visible in the selected snapshot.

Use this map only with `Forward Devices with NetBox Device Type Aliases`, so device type creation and device assignment use the same model and slug mapping.
The query intentionally starts with `foreach device in network.devices` so Forward can use its automatic per-device execution path where available.

```nqe
import "netbox_utilities";

foreach device in network.devices
let aliases = network.extensions.netbox_device_type_aliases
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
let vendor = device.platform.vendor
let raw_model = toString(device.platform.model)
let raw_model_slug = slugifyNetboxModel(raw_model)
let data_manufacturer_name = if isPresent(aliases.value) then max(
    foreach alias in aliases.value
    where alias.record_type == "manufacturer_override"
    where alias.forward_vendor == toString(vendor)
    select alias.manufacturer
  )
  else null : String
let data_manufacturer_slug = if isPresent(aliases.value) then max(
    foreach alias in aliases.value
    where alias.record_type == "manufacturer_override"
    where alias.forward_vendor == toString(vendor)
    select alias.manufacturer_slug
  )
  else null : String
let manufacturer_name = if isPresent(data_manufacturer_name) then data_manufacturer_name else canonicalManufacturerName(vendor)
let manufacturer_slug = if isPresent(data_manufacturer_slug) then data_manufacturer_slug else slugify(manufacturer_name)
let mapped_model = if isPresent(aliases.value) then max(
    foreach alias in aliases.value
    where alias.record_type == "device_type_alias"
    where alias.forward_manufacturer_slug == manufacturer_slug
    where alias.forward_model_slug == raw_model_slug
    select alias.netbox_model
  )
  else null : String
let mapped_slug = if isPresent(aliases.value) then max(
    foreach alias in aliases.value
    where alias.record_type == "device_type_alias"
    where alias.forward_manufacturer_slug == manufacturer_slug
    where alias.forward_model_slug == raw_model_slug
    select alias.netbox_slug
  )
  else null : String
let model = if isPresent(mapped_model) then mapped_model else raw_model
let model_slug = if isPresent(mapped_slug) then mapped_slug else raw_model_slug
select distinct {
  manufacturer: manufacturer_name,
  manufacturer_slug: manufacturer_slug,
  model: model,
  part_number: raw_model,
  slug: model_slug
}
```

## Forward Devices

- `NetBox Model`: `dcim.device`
- Expected fields: `name`, `manufacturer`, `manufacturer_slug`, `device_type`, `device_type_slug`, `site`, `site_slug`, `role`, `role_slug`, `role_color`, `platform`, `platform_slug`, `status`
- Query file: [`forward_devices.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_devices.nqe)
- Default behavior: does not require a Forward data file.

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
let device_type_slug = slugifyNetboxModel(toString(model))
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

## Forward Devices with NetBox Device Type Aliases

- `NetBox Model`: `dcim.device`
- Expected fields: `name`, `manufacturer`, `manufacturer_slug`, `device_type`, `device_type_slug`, `site`, `site_slug`, `role`, `role_slug`, `role_color`, `platform`, `platform_slug`, `status`
- Query file: [`forward_devices_with_netbox_aliases.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_devices_with_netbox_aliases.nqe)
- Seed state: disabled by default.
- Requirement: Forward data file `netbox_device_type_aliases.json` with NQE name `netbox_device_type_aliases` must be uploaded, attached to the network, and visible in the selected snapshot.

Use this map only with `Forward Device Models with NetBox Device Type Aliases`.
The query intentionally starts with `foreach device in network.devices` so Forward can use its automatic per-device execution path where available.

```nqe
import "netbox_utilities";

foreach device in network.devices
let aliases = network.extensions.netbox_device_type_aliases
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
let location = device.locationName
let raw_model = toString(device.platform.model)
let raw_model_slug = slugifyNetboxModel(raw_model)
let device_type = device.platform.deviceType
let site_name = if isPresent(location) then toLowerCase(location) else "unknown"
let site_slug = slugify(site_name)
let role_name = replace(toString(device_type), "DeviceType.", "")
let role_slug = slugify(role_name)
let platform_name = replace(toString(device.platform.os), "OS.", "")
let platform_slug = slugify(platform_name)
let vendor = device.platform.vendor
let data_manufacturer_name = if isPresent(aliases.value) then max(
    foreach alias in aliases.value
    where alias.record_type == "manufacturer_override"
    where alias.forward_vendor == toString(vendor)
    select alias.manufacturer
  )
  else null : String
let data_manufacturer_slug = if isPresent(aliases.value) then max(
    foreach alias in aliases.value
    where alias.record_type == "manufacturer_override"
    where alias.forward_vendor == toString(vendor)
    select alias.manufacturer_slug
  )
  else null : String
let manufacturer_name = if isPresent(data_manufacturer_name) then data_manufacturer_name else canonicalManufacturerName(vendor)
let manufacturer_slug = if isPresent(data_manufacturer_slug) then data_manufacturer_slug else slugify(manufacturer_name)
let mapped_model = if isPresent(aliases.value) then max(
    foreach alias in aliases.value
    where alias.record_type == "device_type_alias"
    where alias.forward_manufacturer_slug == manufacturer_slug
    where alias.forward_model_slug == raw_model_slug
    select alias.netbox_model
  )
  else null : String
let mapped_slug = if isPresent(aliases.value) then max(
    foreach alias in aliases.value
    where alias.record_type == "device_type_alias"
    where alias.forward_manufacturer_slug == manufacturer_slug
    where alias.forward_model_slug == raw_model_slug
    select alias.netbox_slug
  )
  else null : String
let device_type_model = if isPresent(mapped_model) then mapped_model else raw_model
let device_type_slug = if isPresent(mapped_slug) then mapped_slug else raw_model_slug
select {
  name: device.name,
  manufacturer: manufacturer_name,
  device_type: device_type_model,
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
- Current semantics: emits virtual chassis rows for Forward HA `vpc` domains and `mlagPeer` pairs, while bounding `name` and `domain` to NetBox field limits.
The query intentionally starts with `foreach device in network.devices` so Forward can use its automatic per-device execution path where available.

```nqe
truncate(value: String, max_len: Integer) =
  if length(value) <= max_len then value else substring(value, 0, max_len);

compactMemberKey(value: String) =
  if length(value) <= 14
  then value
  else join("", [substring(value, 0, 7), substring(value, length(value) - 7, length(value))]);

foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
let has_vpc = isPresent(device.ha) && isPresent(device.ha.vpc) && isPresent(device.ha.vpc.domainId) && device.ha.vpc.domainId > 0
let has_mlag_peer = isPresent(device.ha) && isPresent(device.ha.mlagPeer)
let mlag_peer_name = if has_mlag_peer then toString(device.ha.mlagPeer) else ""
where has_vpc || has_mlag_peer
let site_name = if isPresent(device.locationName) then toLowerCase(device.locationName) else "unknown"
let member_a = if has_mlag_peer
  then if mlag_peer_name > device.name then device.name else mlag_peer_name
  else device.name
let member_b = if has_mlag_peer
  then if mlag_peer_name > device.name then mlag_peer_name else device.name
  else ""
let raw_mlag_domain = join("--", [member_a, member_b])
let bounded_mlag_domain = if length(raw_mlag_domain) <= 30
  then raw_mlag_domain
  else join("--", [compactMemberKey(member_a), compactMemberKey(member_b)])
let vc_domain = if has_vpc
  then toString(device.ha.vpc.domainId)
  else bounded_mlag_domain
let vc_name = if has_vpc
  then join("-", [truncate(site_name, 48), "vpc", toString(device.ha.vpc.domainId)])
  else join("-", [truncate(site_name, 28), "mlag", vc_domain])
select distinct {
  device: device.name,
  vc_name: vc_name,
  name: vc_name,
  vc_domain: vc_domain
}
```

## Forward Device Feature Tags

- `NetBox Model`: `extras.taggeditem`
- Expected fields: `device`, `tag`, `tag_slug`, `tag_color`
- Query file: [`forward_device_feature_tags.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_feature_tags.nqe)

```nqe
foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach networkInstance in device.networkInstances
foreach protocol in networkInstance.protocols
where isPresent(protocol.bgp)
select distinct {
  device: device.name,
  tag: "Prot_BGP",
  tag_slug: "prot-bgp",
  tag_color: "2196f3"
}
```

The shipped query uses Forward protocol state as the feature source, so BGP tagging is driven by parsed network evidence rather than vendor-specific raw configuration text. The adapter creates or updates the NetBox tag by slug, attaches it to the exact matching device, and removes the device/tag association during diff deletes without deleting the global Tag object.

## Forward Device Feature Tags with Rules

- `NetBox Model`: `extras.taggeditem`
- Expected fields: `device`, `tag`, `tag_slug`, `tag_color`
- Query file: [`forward_device_feature_tags_with_rules.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_device_feature_tags_with_rules.nqe)
- Requirement: Forward data file `netbox_feature_tag_rules.json` with NQE name `netbox_feature_tag_rules` must be uploaded, attached to the network, and visible in the selected snapshot.
- Default state: disabled. Keep `Forward Device Feature Tags` enabled unless the selected snapshot exposes the data file value.
The query intentionally starts with `foreach device in network.devices` so Forward can use its automatic per-device execution path where available.

```nqe
foreach device in network.devices
let rules = network.extensions.netbox_feature_tag_rules
let empty_rules = (foreach x in fromTo(1, 0) select {
  record_type: "",
  enabled: false,
  feature: "",
  tag: "",
  tag_slug: "",
  tag_color: ""
})
let rule_rows = if isPresent(rules.value) then rules.value else empty_rules
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach networkInstance in device.networkInstances
foreach protocol in networkInstance.protocols
foreach rule in rule_rows
where rule.record_type == "structured_feature_tag_rule"
where rule.enabled
where rule.feature == "bgp"
where isPresent(protocol.bgp)
select distinct {
  device: device.name,
  tag: rule.tag,
  tag_slug: rule.tag_slug,
  tag_color: rule.tag_color
}
```

The rules-aware query keeps matching on Forward structured protocol state while moving tag names, slugs, colors, and enabled/disabled policy into a data file. The initial supported structured feature is `bgp`; unsupported feature values are ignored by this query.

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
  let speed_mbps = interface.ethernet.speedMbps
  let ethernet_by_speed_mbps = [
    { mbps: 10, type: "other" },
    { mbps: 100, type: "100base-tx" },
    { mbps: 1000, type: "1000base-t" },
    { mbps: 2500, type: "2.5gbase-t" },
    { mbps: 5000, type: "5gbase-t" },
    { mbps: 10000, type: "10gbase-t" },
    { mbps: 25000, type: "25gbase-x-sfp28" },
    { mbps: 40000, type: "40gbase-x-qsfpp" },
    { mbps: 50000, type: "50gbase-x-sfp56" },
    { mbps: 100000, type: "100gbase-x-qsfp28" }
  ]
  let interface_type = max(foreach profile in ethernet_by_speed_mbps
    where profile.mbps == speed_mbps
    select profile.type)
  select {
    device: device.name,
    name: interface.name,
    type: if isPresent(interface_type) then interface_type else "other",
    enabled: interface.operStatus == OperStatus.UP,
    mtu: interface.mtu,
    description: if isPresent(interface.description) then interface.description else "",
    speed: if isPresent(speed_mbps) then speed_mbps * 1000 else null : Integer
  }
```

The shipped query uses `speedMbps` as the authoritative interface speed and only maps well-known Ethernet rates to NetBox interface types. Unknown or aggregated rates still preserve the actual speed while falling back to interface type `other`. A final `select distinct` over the combined ethernet and loopback interface rows suppresses exact duplicates before NetBox ingestion.

## Forward Inferred Interface Cables

- `NetBox Model`: `dcim.cable`
- Expected fields: `device`, `interface`, `remote_device`, `remote_interface`, `status`
- Query file: [`forward_inferred_interface_cables.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_inferred_interface_cables.nqe)

```nqe
foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach interface in device.interfaces
foreach link in interface.links
where link.deviceName != ""
where link.ifaceName != ""
where device.name != link.deviceName || interface.name != link.ifaceName
let local_first = device.name < link.deviceName ||
  (device.name == link.deviceName && interface.name < link.ifaceName)
select distinct {
  device: if local_first then device.name else link.deviceName,
  interface: if local_first then interface.name else link.ifaceName,
  remote_device: if local_first then link.deviceName else device.name,
  remote_interface: if local_first then link.ifaceName else interface.name,
  status: "connected"
}
```

The shipped query uses Forward-resolved interface links, which are derived from topology discovery data such as LLDP, CDP, and other topology inference where available. The adapter reuses an existing cable between the same two interfaces, refuses to overwrite a different existing cable, and lets Branching expose the resulting cable changes for review before merge.

## Forward MAC Addresses

- `NetBox Model`: `dcim.macaddress`
- Expected fields: `device`, `interface`, `mac`, `mac_address`
- Query file: [`forward_mac_addresses.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_mac_addresses.nqe)

```nqe
candidate_rows =
  foreach device in network.devices
  where device.snapshotInfo.result == DeviceSnapshotResult.completed
  where device.platform.vendor != Vendor.FORWARD_CUSTOM
  foreach interface in device.interfaces
  where interface.interfaceType == IfaceType.IF_ETHERNET
  where isPresent(interface.ethernet.macAddress)
  select distinct {
    device: device.name,
    interface: interface.name,
    mac: toString(interface.ethernet.macAddress),
    mac_address: toString(interface.ethernet.macAddress)
  };

@primaryKey(mac_address)
foreach row in candidate_rows
where row.mac_address != "00:00:00:00:00:00"
group row as grouped_rows by row.mac_address as mac_address
let chosen_device = min(foreach candidate in grouped_rows
  select candidate.device)
let chosen_interface = min(foreach candidate in grouped_rows
  where candidate.device == chosen_device
  select candidate.interface)
select {
  device: chosen_device,
  interface: chosen_interface,
  mac: mac_address,
  mac_address: mac_address
}
```

The shipped MAC query removes exact duplicate rows, filters the all-zero placeholder MAC, and then projects a single deterministic row per NetBox MAC identity before ingestion.

## Forward VLANs

- `NetBox Model`: `ipam.vlan`
- Expected fields: `site`, `site_slug`, `vid`, `name`, `status`
- Query file: [`forward_vlans.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_vlans.nqe)

```nqe
import "netbox_utilities";

candidate_rows =
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
  };

foreach grouped in (
  foreach row in candidate_rows
  group row as grouped_rows by {
    site: row.site,
    site_slug: row.site_slug,
    vid: row.vid
  } as key
  let preferred_name = min(
    foreach candidate in grouped_rows
    where candidate.name != join(" ", ["VLAN", toString(key.vid)])
    where toLowerCase(candidate.name) != "default"
    select candidate.name
  )
  let chosen_name = if isPresent(preferred_name)
    then preferred_name
    else min(foreach candidate in grouped_rows select candidate.name)
  select {
    site: key.site,
    site_slug: key.site_slug,
    vid: key.vid,
    name: chosen_name,
    status: "active"
  }
)
select grouped
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

The shipped query combines rows from subinterfaces, bridge interfaces, tunnels, and routed VLAN interfaces, applies a final `select distinct` over the merged result, and then projects a single deterministic row per NetBox IP identity. VRF-scoped rows keep the normal `(address, vrf)` identity. Global-table rows are canonicalized by bare host IP so the plugin does not try to create multiple global IP objects for the same host with different masks; when that happens, the most specific mask wins. It still skips subnet network IDs and IPv4 broadcast addresses that NetBox cannot assign to interfaces, while preserving point-to-point endpoint prefixes such as IPv4 `/31` and IPv6 `/127`. These rows are skipped rather than rewritten because there is no NetBox-native host address to infer safely from the device configuration.

When `ipam.ipaddress` is enabled, the sync also runs an internal read-only diagnostic query that reports how many Forward interface addresses were filtered for this reason and logs capped examples. This diagnostic query is not seeded as a NetBox import map and does not create, update, or delete NetBox objects. See the query file for the complete import text:

- [`forward_ip_addresses.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_ip_addresses.nqe)

## Forward Inventory Items

- `NetBox Model`: `dcim.inventoryitem`
- Expected fields: `device`, `manufacturer`, `manufacturer_slug`, `name`, `label`, `part_id`, `serial`, `asset_tag`, `role`, `role_slug`, `role_color`, `part_type`, `module_component`, `status`, `discovered`, `description`
- Query file: [`forward_inventory_items.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_inventory_items.nqe)

The query intentionally starts with `foreach device in network.devices` so Forward can use its automatic per-device execution path where available. It imports hardware component part types as inventory items, excludes application pseudo-parts, and leaves unknown part IDs or serial numbers blank instead of synthesizing identifiers. When `dcim.module` is enabled, rows marked `module_component` are cleaned out of generic inventory and modeled by the module adapter.

Forward may expose component lifecycle support fields under `component.support`, but the built-in map does not copy those dates into descriptions because NetBox `InventoryItem` has no native lifecycle fields. Use a custom query or custom fields if you want lifecycle reporting in NetBox.

```nqe
import "netbox_utilities";

truncate(value: String, max_len: Integer) =
  if length(value) <= max_len then value else substring(value, 0, max_len);

isInventoryHardwareRole(role_name: String) =
  role_name != "APPLICATION" &&
  role_name != "UNMODELED BACKUP DEVICE";

isNetBoxModuleRole(role_name: String) =
  role_name == "LINE CARD" ||
  role_name == "SUPERVISOR" ||
  role_name == "FABRIC MODULE" ||
  role_name == "ROUTING ENGINE";

inventoryRoleColor(role_name: String) =
  if role_name == "TRANSCEIVER" then "2196f3"
  else if role_name == "POWER SUPPLY" then "ff9800"
  else if role_name == "FAN MODULE" then "00bcd4"
  else if role_name == "CHASSIS" then "607d8b"
  else if role_name == "MOTHERBOARD" then "673ab7"
  else if role_name == "STACK" || role_name == "STACK SWITCH" || role_name == "STACK PORT" || role_name == "STACK MODULE" then "4caf50"
  else if isNetBoxModuleRole(role_name) then "3f51b5"
  else "9e9e9e";

foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach component in device.platform.components
let manufacturer_name = canonicalManufacturerName(device.platform.vendor)
let manufacturer_slug = slugify(manufacturer_name)
let role_name = replace(replace(toString(component.partType), "DevicePartType.", ""), "_", " ")
where isInventoryHardwareRole(role_name)
let role_slug = slugify(role_name)
let component_name = if isPresent(component.name) && component.name != "" then component.name else null : String
let component_part_id = if isPresent(component.partId) && component.partId != "" then component.partId else null : String
let component_serial = if isPresent(component.serialNumber) && component.serialNumber != "" then component.serialNumber else null : String
let component_description = if isPresent(component.description) && component.description != "" then component.description else null : String
let component_version = if isPresent(component.versionId) && component.versionId != "" then component.versionId else null : String
let inventory_name = if isPresent(component_name) then component_name else if isPresent(component_part_id) then component_part_id else if isPresent(component_description) then component_description else role_name
let inventory_description = if isPresent(component_description) && isPresent(component_version) then truncate(join(" | ", [component_description, join(": ", ["Version", component_version])]), 200) else if isPresent(component_description) then truncate(component_description, 200) else if isPresent(component_version) then truncate(join(": ", ["Version", component_version]), 200) else ""
select distinct {
  device: device.name,
  manufacturer: manufacturer_name,
  manufacturer_slug: manufacturer_slug,
  name: truncate(inventory_name, 64),
  label: if isPresent(component_name) then truncate(component_name, 64) else "",
  part_id: if isPresent(component_part_id) then truncate(component_part_id, 50) else "",
  serial: if isPresent(component_serial) then truncate(component_serial, 50) else "",
  asset_tag: null : String,
  role: role_name,
  role_slug: role_slug,
  role_color: inventoryRoleColor(role_name),
  part_type: role_name,
  module_component: isNetBoxModuleRole(role_name),
  status: "active",
  discovered: true,
  description: inventory_description
}
```

## Forward Modules

- `NetBox Model`: `dcim.module`
- Expected fields: `device`, `module_bay`, `manufacturer`, `manufacturer_slug`, `model`, `part_number`, `status`, `serial`, `asset_tag`, `description`
- Query file: [`forward_modules.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_modules.nqe)
- Enabled: disabled by default
- Stability: beta in `v0.6.x`; review staged module and module-bay changes carefully before merging

The module map uses the same device-first parallel shape as the inventory-item map, but keeps the target model separate so bay-aware chassis hardware can be modeled without overlapping the generic inventory-item fallback. NQE is the classification layer: this map emits only `LINE_CARD`, `SUPERVISOR`, `FABRIC_MODULE`, and `ROUTING_ENGINE` components. Transceivers, fans, power supplies, chassis records, stack artifacts, and motherboards remain inventory-item candidates; application pseudo-parts are not imported as inventory items by default.

```nqe
import "netbox_utilities";

truncate(value: String, max_len: Integer) =
  if length(value) <= max_len then value else substring(value, 0, max_len);

isNetBoxModuleComponent(component: DevicePart) =
  component.partType == DevicePartType.LINE_CARD ||
  component.partType == DevicePartType.SUPERVISOR ||
  component.partType == DevicePartType.FABRIC_MODULE ||
  component.partType == DevicePartType.ROUTING_ENGINE;

foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach component in device.platform.components
where isNetBoxModuleComponent(component)
let manufacturer_name = canonicalManufacturerName(device.platform.vendor)
let manufacturer_slug = slugify(manufacturer_name)
let component_name = if isPresent(component.name) && component.name != "" then component.name else null : String
let component_part_id = if isPresent(component.partId) && component.partId != "" then component.partId else null : String
let component_serial = if isPresent(component.serialNumber) && component.serialNumber != "" then component.serialNumber else null : String
let component_description = if isPresent(component.description) && component.description != "" then component.description else null : String
let module_bay_name = if isPresent(component_name) then component_name else if isPresent(component_part_id) then component_part_id else replace(replace(toString(component.partType), "DevicePartType.", ""), "_", " ")
let module_model = if isPresent(component_part_id) then component_part_id else if isPresent(component_description) then component_description else module_bay_name
select distinct {
  device: device.name,
  module_bay: truncate(module_bay_name, 100),
  manufacturer: manufacturer_name,
  manufacturer_slug: manufacturer_slug,
  model: truncate(module_model, 100),
  part_number: if isPresent(component_part_id) then truncate(component_part_id, 50) else truncate(module_model, 50),
  status: "active",
  serial: if isPresent(component_serial) then truncate(component_serial, 50) else null : String,
  asset_tag: null : String,
  description: if isPresent(component_description) then component_description else ""
}
```

## Important Caveats

- `dcim.inventoryitem` remains the default best-fit path for generic components.
- `dcim.module` is disabled by default and should only be enabled when you want bay-aware hardware modeled as modules instead of inventory items.
- The module path uses native NetBox model operations to create missing module bays from the NQE `module_bay` field when `dcim.module` is enabled. Those bay creations are part of the normal Branching diff.
- Before enabling module sync, run `python manage.py forward_module_readiness --sync-name "<sync name>"` to generate a readiness summary and an optional native NetBox module-bay import CSV for missing bays.
- `dcim.module` uses a default branch density of 2 changes per NQE row because a new row can create both a module bay and a module.
- SFP/transceiver rows remain in the inventory-item path by default; do not enable module import expecting optics to become NetBox modules unless the query is customized for device types that expose matching module bays.
