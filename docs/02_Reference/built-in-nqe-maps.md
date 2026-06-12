# Built-In NQE Maps

This reference lists the built-in NQE maps that ship with `forward_netbox`.

Each entry includes:

- the map name
- the target `NetBox Model`
- the expected output fields
- the shipped query file in the repository
- the exact shipped source text

All built-in maps are executed against the sync-selected Forward snapshot. The shipped query set includes default maps that require no Forward data file and disabled data-file-aware variants that require the selected snapshot to expose fields such as `network.extensions.netbox_device_type_aliases.value` or `network.extensions.netbox_feature_tag_rules.value`. The examples below are the shipped query source from this repository. Queries that import `netbox_utilities` are flattened by the plugin at execution time for bundled built-ins, but the source modules shown here can also be copied into the Forward Org Repository and tested by `query_id`.

When a sync uses the local device tag filter mode, the plugin now passes the selected include/exclude tags into tag-aware built-in queries for sites and prefixes. This keeps site and prefix collection aligned with the selected device scope at the Forward NQE source instead of fetching broad rows and pruning only after the API call. Custom org queries that do not declare these parameters continue to use the existing local row filter behavior.

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
| Forward HSRP Groups | `ipam.fhrpgroup` | [`forward_hsrp_groups.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_hsrp_groups.nqe) |
| Forward Inventory Items | `dcim.inventoryitem` | [`forward_inventory_items.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_inventory_items.nqe) |
| Forward ACI Fabrics | `netbox_cisco_aci.acifabric` | [`forward_aci_fabrics.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_fabrics.nqe) |
| Forward ACI Pods | `netbox_cisco_aci.acipod` | [`forward_aci_pods.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_pods.nqe) |
| Forward ACI Nodes | `netbox_cisco_aci.acinode` | [`forward_aci_nodes.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_nodes.nqe) |
| Forward ACI APIC Nodes | `netbox_cisco_aci.acinode` | [`forward_aci_apic_nodes.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_apic_nodes.nqe) |
| Forward ACI APIC CIMC Inventory | `dcim.inventoryitem` | [`forward_aci_apic_cimc_inventory.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_apic_cimc_inventory.nqe) |
| Forward ACI Tenants | `netbox_cisco_aci.acitenant` | [`forward_aci_tenants.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_tenants.nqe) |
| Forward ACI VRFs | `netbox_cisco_aci.acivrf` | [`forward_aci_vrfs.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_vrfs.nqe) |
| Forward ACI Bridge Domains | `netbox_cisco_aci.acibridgedomain` | [`forward_aci_bridge_domains.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_bridge_domains.nqe) |
| Forward ACI Application Profiles | `netbox_cisco_aci.aciappprofile` | [`forward_aci_app_profiles.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_app_profiles.nqe) |
| Forward ACI Endpoint Groups | `netbox_cisco_aci.aciendpointgroup` | [`forward_aci_endpoint_groups.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_endpoint_groups.nqe) |
| Forward ACI Contracts | `netbox_cisco_aci.acicontract` | [`forward_aci_contracts.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_contracts.nqe) |
| Forward ACI Filters | `netbox_cisco_aci.acifilter` | [`forward_aci_filters.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_filters.nqe) |
| Forward ACI L3Outs | `netbox_cisco_aci.acil3out` | [`forward_aci_l3outs.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_l3outs.nqe) |
| Forward ACI Static Port Bindings | `netbox_cisco_aci.acistaticportbinding` | [`forward_aci_static_port_bindings.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_static_port_bindings.nqe) |
| Forward ACI Command Inventory | `dcim.device` | [`forward_aci_command_inventory.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_aci_command_inventory.nqe) |

## Optional Cisco ACI Plugin Maps

The `1.3.3` ACI maps are disabled by default and require the optional
`netbox-cisco-aci` plugin. They do not use sync-time Forward column filters.
All maps declare `forward_netbox_shard_keys`, seed an empty default for UI
execution, and constrain rows only when shard keys are provided.

The proven write path covers ACI fabrics, pods, nodes, tenants, VRFs, filters,
and APIC CIMC inventory items. The CIMC inventory map targets native NetBox
`dcim.inventoryitem` rows and requires Forward to collect the APIC custom
command `moquery -c eqptCh -a all`; it joins those chassis rows to APIC
controller detail output in NQE before emitting normalized inventory rows.
Bridge domains, application profiles, EPGs, contracts, L3Outs, and static
port bindings are present as disabled model/query contracts but remain
conservative no-op maps until bounded source identity and repeat-sync
idempotence are proven. The active maps parse selected command output in NQE
and emit small normalized rows rather than returning raw command responses.
The separate `Forward ACI Command Inventory` map is discovery-only and reports
which bounded ACI/APIC command families are present on each device without
returning raw response payloads.

| Map | Expected Fields |
| --- | --- |
| Forward ACI Fabrics | `name`, `fabric_id`, `description` |
| Forward ACI Pods | `fabric_name`, `name`, `pod_id`, `description` |
| Forward ACI Nodes | `fabric_name`, `pod_name`, `pod_id`, `node_id`, `name`, `role`, `node_type`, `serial_number`, `pod_tep_pool`, `firmware_version`, `node_object_name`, `description` |
| Forward ACI APIC Nodes | `fabric_name`, `pod_name`, `pod_id`, `node_id`, `name`, `role`, `node_type`, `serial_number`, `pod_tep_pool`, `firmware_version`, `node_object_name`, `description` |
| Forward ACI APIC CIMC Inventory | `device`, `manufacturer`, `manufacturer_slug`, `name`, `label`, `part_id`, `serial`, `asset_tag`, `role`, `role_slug`, `role_color`, `part_type`, `module_component`, `status`, `discovered`, `description` |
| Forward ACI Tenants | `fabric_name`, `name`, `description` |
| Forward ACI VRFs | `fabric_name`, `tenant_name`, `name`, `policy_enforcement_preference`, `policy_enforcement_direction`, `bd_enforcement_enabled`, `preferred_group_enabled`, `description` |
| Forward ACI Bridge Domains | `fabric_name`, `tenant_name`, `vrf_tenant_name`, `vrf_name`, `name`, `unicast_routing_enabled`, `arp_flooding_enabled`, `limit_ip_learn_to_subnets`, `l2_unknown_unicast`, `l3_unknown_multicast`, `multi_destination_flooding`, `mac_address`, `description` |
| Forward ACI Application Profiles | `fabric_name`, `tenant_name`, `name`, `description` |
| Forward ACI Endpoint Groups | `fabric_name`, `tenant_name`, `app_profile_name`, `bridge_domain_name`, `vrf_name`, `name`, `admin_shutdown`, `is_useg`, `intra_epg_isolation`, `preferred_group_member`, `qos_class`, `description` |
| Forward ACI Contracts | `fabric_name`, `tenant_name`, `name`, `scope`, `description` |
| Forward ACI Filters | `fabric_name`, `tenant_name`, `name`, `description` |
| Forward ACI L3Outs | `fabric_name`, `tenant_name`, `vrf_name`, `name`, `protocol_bgp`, `protocol_ospf`, `protocol_eigrp`, `protocol_static`, `target_dscp`, `description` |
| Forward ACI Static Port Bindings | `fabric_name`, `tenant_name`, `app_profile_name`, `endpoint_group_name`, `device_name`, `interface_name`, `encap_vlan`, `deployment_immediacy`, `mode`, `binding_type`, `description` |

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

export platformOsName(platform_os: String) =
  replace(platform_os, "OS.", "");

export isAciNxosVersion(platform_os: String, platform_os_version: String) =
  platformOsName(platform_os) == "NXOS"
  && (
    matches(platform_os_version, "14.*")
    || matches(platform_os_version, "15.*")
    || matches(platform_os_version, "16.*")
  );

export normalizePlatformName(platform_os: String, platform_os_version: String) =
  if matches(toLowerCase(platformOsName(platform_os)), "*apic*")
    || matches(toLowerCase(platformOsName(platform_os)), "*nxos_aci*")
    || isAciNxosVersion(platform_os, platform_os_version)
  then "ACI"
  else platformOsName(platform_os);

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

@query
f(device_tag_include_tags: List<String>, device_tag_include_match: String, device_tag_exclude_tags: List<String>) =
foreach location in network.locations
where location.name in (
  foreach d in network.devices
  where d.snapshotInfo.result == DeviceSnapshotResult.completed
  where d.platform.vendor != Vendor.FORWARD_CUSTOM
  where isEmpty(device_tag_include_tags)
    || (device_tag_include_match == "all"
      && all(foreach tag in device_tag_include_tags select tag in d.tagNames))
    || (device_tag_include_match != "all"
      && any(foreach tag in device_tag_include_tags select tag in d.tagNames))
  where isEmpty(device_tag_exclude_tags)
    || !any(foreach tag in device_tag_exclude_tags select tag in d.tagNames)
  select distinct d.locationName
)
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
};
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
let platform_name = normalizePlatformName(toString(device.platform.os), device.platform.osVersion)
let platform_slug = slugify(platform_name)
let manufacturer_name = canonicalManufacturerName(device.platform.vendor)
let manufacturer_slug = slugify(manufacturer_name)
select distinct {
  name: platform_name,
  manufacturer: manufacturer_name,
  manufacturer_slug: manufacturer_slug,
  slug: platform_slug
};
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
let platform_name = normalizePlatformName(toString(device.platform.os), device.platform.osVersion)
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
};
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
let platform_name = normalizePlatformName(toString(device.platform.os), device.platform.osVersion)
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
};
```

## Forward Virtual Chassis

- `NetBox Model`: `dcim.virtualchassis`
- Expected fields: `device`, `vc_name`, `name`, `vc_domain`, `vc_position`
- Query file: [`forward_virtual_chassis.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_virtual_chassis.nqe)
- Current semantics: conservative no-op. Forward HA peer relationships such
  as vPC, MLAG, and active/standby clusters are separate control-plane
  relationships, not native NetBox virtual chassis membership.
- Chassis internals from Forward are handled by the module and inventory item
  maps through `device.platform.components`.
- The query intentionally starts with `foreach device in network.devices` and
  declares the required fields so custom or restored virtual chassis maps keep
  the same model contract.

```nqe
foreach device in network.devices
where false
select distinct {
  device: device.name,
  vc_name: "",
  name: "",
  vc_domain: "",
  vc_position: 1
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
- Expected fields: `device`, `name`, `type`, `lag`, `mode`, `untagged_vlan`, `enabled`, `mtu`, `description`, `speed`
- Query file: [`forward_interfaces.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_interfaces.nqe)

```nqe
/**
 * @intent Device interfaces collected by Forward
 * @description It provides a list of device interfaces collected by Forward to be added
 * in NetBox using POST and PATCH requests to the /api/dcim/interfaces/ REST API endpoint.
 */

ethernet_by_speed_mbps = [
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
];

foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
foreach interface in device.interfaces
  let is_ethernet = interface.interfaceType == IfaceType.IF_ETHERNET
  let speed_mbps = if is_ethernet then interface.ethernet.speedMbps else null : Integer
  let interface_type = max(foreach profile in ethernet_by_speed_mbps
    where profile.mbps == speed_mbps
    select profile.type)
  let netbox_type =
    if is_ethernet
      then if isPresent(interface_type) then interface_type else "other"
    else if interface.interfaceType == IfaceType.IF_LOOPBACK
      then "virtual"
    else if interface.interfaceType == IfaceType.IF_AGGREGATE
      then "lag"
    else "other"
  let vlan_mode =
    if isPresent(interface.ethernet?.switchedVlan?.interfaceMode)
      then if interface.ethernet.switchedVlan.interfaceMode == VlanModeType.ACCESS then "access"
      else if interface.ethernet.switchedVlan.interfaceMode == VlanModeType.TRUNK then "tagged"
      else null : String
    else null : String
  let untagged_vlan =
    if vlan_mode == "access" && isPresent(interface.ethernet.switchedVlan.accessVlan)
      then interface.ethernet.switchedVlan.accessVlan
    else if vlan_mode == "tagged" && isPresent(interface.ethernet.switchedVlan.nativeVlan)
      then interface.ethernet.switchedVlan.nativeVlan
    else null : Integer
  select {
    device: device.name,
    name: interface.name,
    type: netbox_type,
    lag: if is_ethernet && isPresent(interface.ethernet.aggregateId) then interface.ethernet.aggregateId else null : String,
    mode: vlan_mode,
    untagged_vlan: untagged_vlan,
    enabled: interface.operStatus == OperStatus.UP,
    mtu: interface.mtu,
    description: if isPresent(interface.description) then interface.description else "",
    speed: if isPresent(speed_mbps) then speed_mbps * 1000 else null : Integer
  }
```

The shipped query uses `speedMbps` as the authoritative interface speed and only maps well-known Ethernet rates to NetBox interface types. Unknown physical rates still preserve the actual speed while falling back to interface type `other`. Forward `IF_AGGREGATE` interfaces are emitted as native NetBox LAG interfaces, and physical members attach through the native NetBox `lag` relationship when Forward reports `ethernet.aggregateId`. During sharded imports, a member row can create a minimal native LAG placeholder if its aggregate row is applied by a later branch; the aggregate row updates that placeholder when it is processed. MTU is preserved from Forward `interface.mtu`, which is the normalized L2 MTU value exposed by the NQE data model.

Forward switched access mode and trunk native VLANs are mapped to native NetBox `Interface.mode` and `Interface.untagged_vlan`. The interface adapter only attaches a VLAN that already exists for the device site; a missing VLAN logs an aggregated warning and the interface still imports. Tagged trunk VLAN expansion is intentionally not performed by this map because Forward can represent ranges and implicit all-VLAN trunks, which would create unnecessary NetBox relationship volume.

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
@query
f(forward_netbox_shard_keys: List<String>, device_tag_include_tags: List<String>, device_tag_include_match: String, device_tag_exclude_tags: List<String>) =
foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
where isEmpty(device_tag_include_tags)
  || (device_tag_include_match == "all"
    && all(foreach tag in device_tag_include_tags select tag in device.tagNames))
  || (device_tag_include_match != "all"
    && any(foreach tag in device_tag_include_tags select tag in device.tagNames))
where isEmpty(device_tag_exclude_tags)
  || !any(foreach tag in device_tag_exclude_tags select tag in device.tagNames)
foreach ni in device.networkInstances
where isPresent(ni.afts?.ipv4Unicast?.ipEntries)
foreach entry in ni.afts.ipv4Unicast.ipEntries
where length(entry.prefix) > 0
where length(entry.prefix) < 32
where !(toNumber(networkAddress(entry.prefix)) >= toNumber(ipAddress("0.0.0.0"))
  && toNumber(networkAddress(entry.prefix)) <= toNumber(ipAddress("0.255.255.255")))
where !(toNumber(networkAddress(entry.prefix)) >= toNumber(ipAddress("127.0.0.0"))
  && toNumber(networkAddress(entry.prefix)) <= toNumber(ipAddress("127.255.255.255")))
let prefix = ipSubnet(networkAddress(entry.prefix), length(entry.prefix))
where length(forward_netbox_shard_keys) == 0 || toString(prefix) in forward_netbox_shard_keys
select distinct {
  vrf: if ni.name != "default"
    then if toString(ni.instanceType) != "NetworkInstanceType.DEFAULT_INSTANCE" then ni.name else null : String
    else null : String,
  prefix: prefix,
  status: "active"
};
```

The IPv4 prefix map excludes host routes and clearly non-importable route-table
artifacts in `0.0.0.0/8` and `127.0.0.0/8`. It does not rewrite those rows into
different prefixes; the query simply leaves them out of the NetBox prefix feed.
It also accepts the built-in shard key parameter and the selected device tag
include/exclude parameters so large scoped imports can reduce Forward NQE result
volume before NetBox branch planning.

## Forward IPv6 Prefixes

- `NetBox Model`: `ipam.prefix`
- Expected fields: `vrf`, `prefix`, `status`
- Query file: [`forward_prefixes_ipv6.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_prefixes_ipv6.nqe)

```nqe
@query
f(forward_netbox_shard_keys: List<String>, device_tag_include_tags: List<String>, device_tag_include_match: String, device_tag_exclude_tags: List<String>) =
foreach device in network.devices
where device.snapshotInfo.result == DeviceSnapshotResult.completed
where device.platform.vendor != Vendor.FORWARD_CUSTOM
where isEmpty(device_tag_include_tags)
  || (device_tag_include_match == "all"
    && all(foreach tag in device_tag_include_tags select tag in device.tagNames))
  || (device_tag_include_match != "all"
    && any(foreach tag in device_tag_include_tags select tag in device.tagNames))
where isEmpty(device_tag_exclude_tags)
  || !any(foreach tag in device_tag_exclude_tags select tag in device.tagNames)
foreach ni in device.networkInstances
where isPresent(ni.afts?.ipv6Unicast?.ipEntries)
foreach entry in ni.afts.ipv6Unicast.ipEntries
where length(entry.prefix) > 0
where length(entry.prefix) < 128
let prefix = ipSubnet(networkAddress(entry.prefix), length(entry.prefix))
where length(forward_netbox_shard_keys) == 0 || toString(prefix) in forward_netbox_shard_keys
select distinct {
  vrf: if ni.name != "default"
    then if toString(ni.instanceType) != "NetworkInstanceType.DEFAULT_INSTANCE" then ni.name else null : String
    else null : String,
  prefix: prefix,
  status: "active"
};
```

The IPv6 prefix map accepts the built-in shard key parameter and the selected
device tag include/exclude parameters so scoped imports avoid collecting
prefixes from devices outside the selected Forward tag scope.

## Forward IP Addresses

- `NetBox Model`: `ipam.ipaddress`
- Expected fields: `device`, `interface`, `vrf`, `address`, `status`
- Query file: [`forward_ip_addresses.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_ip_addresses.nqe)

The shipped query combines rows from subinterfaces, bridge interfaces, tunnels, and routed VLAN interfaces, filters those candidates through the importable Forward interface set, applies a final `select distinct` over the merged result, and then projects a single deterministic row per NetBox IP identity. VRF-scoped rows keep the normal `(address, vrf)` identity. Global-table rows are canonicalized by bare host IP so the plugin does not try to create multiple global IP objects for the same host with different masks; when that happens, the most specific mask wins. It still skips subnet network IDs and IPv4 broadcast addresses that NetBox cannot assign to interfaces, while preserving point-to-point endpoint prefixes such as IPv4 `/31` and IPv6 `/127`. These rows are skipped rather than rewritten because there is no NetBox-native host address to infer safely from the device configuration. If an IP row still targets an interface that was not imported, the NetBox adapter records an aggregated skip warning instead of treating the row as a fatal sync failure.

For shard-scoped Branching retries, the plugin applies native Forward column
filters and then enforces the shard boundary again in NetBox before applying
rows. Prefix maps use live-validated parameterized built-ins for shard keys and
device tag scope; custom query maps that do not declare these parameters keep
the existing local enforcement path.

When `ipam.ipaddress` is enabled, the sync also runs an internal read-only diagnostic query that reports how many Forward interface addresses were filtered for this reason and logs capped examples. This diagnostic query is not seeded as a NetBox import map and does not create, update, or delete NetBox objects. See the query file for the complete import text:

On full baseline runs where both `ipam.prefix` and `ipam.ipaddress` are enabled,
the sync also records a read-only diagnostic when an imported IP address does
not have a covering imported prefix in the same VRF. This is advisory visibility
for source or query coverage gaps; it does not create parent prefixes or mutate
the IP address row.

- [`forward_ip_addresses.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_ip_addresses.nqe)

## Forward HSRP Groups

- `NetBox Model`: `ipam.fhrpgroup`
- Expected fields: `protocol`, `group_id`, `name`, `device`, `interface`, `vrf`, `address`, `state`, `priority`, `status`
- Query file: [`forward_hsrp_groups.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_hsrp_groups.nqe)

This optional map imports Forward native HSRP and VRRP group state from
subinterfaces and routed VLAN interfaces. The sync creates native NetBox
`FHRPGroup` rows,
`FHRPGroupAssignment` rows for participating interfaces, and one VIP
`IPAddress` assigned to the group. Multiple participants for the same
protocol/group/address/VRF share one group and VIP. IPv4 VRRP rows map to
NetBox `vrrp2`, IPv6 VRRP rows map to NetBox `vrrp3`, and VIP IP addresses use
the native `vrrp` role. Same group/address values in different VRFs remain
separate. If a target VIP host already exists in NetBox and is assigned to
another object, the row is skipped with an aggregated warning rather than
reassigning the IP address.

The built-in query is a single paged NQE result set and does not add per-device,
per-interface, or per-group Forward API calls.

```nqe
/**
 * @intent Forward HSRP Groups
 * @description NetBox FHRP group rows derived from Forward native HSRP and VRRP state.
 */

subinterface_ipv4 =
  foreach device in network.devices
  where device.snapshotInfo.result == DeviceSnapshotResult.completed
  where device.platform.vendor != Vendor.FORWARD_CUSTOM
  foreach interface in device.interfaces
  foreach subinterface in interface.subinterfaces
  where isPresent(subinterface.ipv4?.fhrp?.hsrp?.fhrpGroups)
  foreach group in subinterface.ipv4.fhrp.hsrp.fhrpGroups
  select {
    protocol: "hsrp",
    group_id: group.virtualRouterId,
    name: "hsrp",
    device: device.name,
    interface: interface.name,
    vrf: if isPresent(subinterface.networkInstanceName)
      then if subinterface.networkInstanceName != "default" then subinterface.networkInstanceName else null : String
      else null : String,
    address: ipSubnet(group.virtualAddress, 32),
    state: group.state,
    priority: 100,
    status: "active"
  };

subinterface_ipv6 =
  foreach device in network.devices
  where device.snapshotInfo.result == DeviceSnapshotResult.completed
  where device.platform.vendor != Vendor.FORWARD_CUSTOM
  foreach interface in device.interfaces
  foreach subinterface in interface.subinterfaces
  where isPresent(subinterface.ipv6?.fhrp?.hsrp?.fhrpGroups)
  foreach group in subinterface.ipv6.fhrp.hsrp.fhrpGroups
  select {
    protocol: "hsrp",
    group_id: group.virtualRouterId,
    name: "hsrp",
    device: device.name,
    interface: interface.name,
    vrf: if isPresent(subinterface.networkInstanceName)
      then if subinterface.networkInstanceName != "default" then subinterface.networkInstanceName else null : String
      else null : String,
    address: ipSubnet(group.virtualAddress, 128),
    state: group.state,
    priority: 100,
    status: "active"
  };

routed_vlan_ipv4 =
  foreach device in network.devices
  where device.snapshotInfo.result == DeviceSnapshotResult.completed
  where device.platform.vendor != Vendor.FORWARD_CUSTOM
  foreach interface in device.interfaces
  where isPresent(interface.routedVlan?.ipv4?.fhrp?.hsrp?.fhrpGroups)
  foreach group in interface.routedVlan.ipv4.fhrp.hsrp.fhrpGroups
  select {
    protocol: "hsrp",
    group_id: group.virtualRouterId,
    name: "hsrp",
    device: device.name,
    interface: interface.name,
    vrf: if isPresent(interface.routedVlan.networkInstanceName)
      then if interface.routedVlan.networkInstanceName != "default" then interface.routedVlan.networkInstanceName else null : String
      else null : String,
    address: ipSubnet(group.virtualAddress, 32),
    state: group.state,
    priority: 100,
    status: "active"
  };

routed_vlan_ipv6 =
  foreach device in network.devices
  where device.snapshotInfo.result == DeviceSnapshotResult.completed
  where device.platform.vendor != Vendor.FORWARD_CUSTOM
  foreach interface in device.interfaces
  where isPresent(interface.routedVlan?.ipv6?.fhrp?.hsrp?.fhrpGroups)
  foreach group in interface.routedVlan.ipv6.fhrp.hsrp.fhrpGroups
  select {
    protocol: "hsrp",
    group_id: group.virtualRouterId,
    name: "hsrp",
    device: device.name,
    interface: interface.name,
    vrf: if isPresent(interface.routedVlan.networkInstanceName)
      then if interface.routedVlan.networkInstanceName != "default" then interface.routedVlan.networkInstanceName else null : String
      else null : String,
    address: ipSubnet(group.virtualAddress, 128),
    state: group.state,
    priority: 100,
    status: "active"
  };

subinterface_vrrp_ipv4 =
  foreach device in network.devices
  where device.snapshotInfo.result == DeviceSnapshotResult.completed
  where device.platform.vendor != Vendor.FORWARD_CUSTOM
  foreach interface in device.interfaces
  foreach subinterface in interface.subinterfaces
  where isPresent(subinterface.ipv4?.fhrp?.vrrp?.fhrpGroups)
  foreach group in subinterface.ipv4.fhrp.vrrp.fhrpGroups
  select {
    protocol: "vrrp2",
    group_id: group.virtualRouterId,
    name: "vrrp",
    device: device.name,
    interface: interface.name,
    vrf: if isPresent(subinterface.networkInstanceName)
      then if subinterface.networkInstanceName != "default" then subinterface.networkInstanceName else null : String
      else null : String,
    address: ipSubnet(group.virtualAddress, 32),
    state: group.state,
    priority: 100,
    status: "active"
  };

subinterface_vrrp_ipv6 =
  foreach device in network.devices
  where device.snapshotInfo.result == DeviceSnapshotResult.completed
  where device.platform.vendor != Vendor.FORWARD_CUSTOM
  foreach interface in device.interfaces
  foreach subinterface in interface.subinterfaces
  where isPresent(subinterface.ipv6?.fhrp?.vrrp?.fhrpGroups)
  foreach group in subinterface.ipv6.fhrp.vrrp.fhrpGroups
  select {
    protocol: "vrrp3",
    group_id: group.virtualRouterId,
    name: "vrrp",
    device: device.name,
    interface: interface.name,
    vrf: if isPresent(subinterface.networkInstanceName)
      then if subinterface.networkInstanceName != "default" then subinterface.networkInstanceName else null : String
      else null : String,
    address: ipSubnet(group.virtualAddress, 128),
    state: group.state,
    priority: 100,
    status: "active"
  };

routed_vlan_vrrp_ipv4 =
  foreach device in network.devices
  where device.snapshotInfo.result == DeviceSnapshotResult.completed
  where device.platform.vendor != Vendor.FORWARD_CUSTOM
  foreach interface in device.interfaces
  where isPresent(interface.routedVlan?.ipv4?.fhrp?.vrrp?.fhrpGroups)
  foreach group in interface.routedVlan.ipv4.fhrp.vrrp.fhrpGroups
  select {
    protocol: "vrrp2",
    group_id: group.virtualRouterId,
    name: "vrrp",
    device: device.name,
    interface: interface.name,
    vrf: if isPresent(interface.routedVlan.networkInstanceName)
      then if interface.routedVlan.networkInstanceName != "default" then interface.routedVlan.networkInstanceName else null : String
      else null : String,
    address: ipSubnet(group.virtualAddress, 32),
    state: group.state,
    priority: 100,
    status: "active"
  };

routed_vlan_vrrp_ipv6 =
  foreach device in network.devices
  where device.snapshotInfo.result == DeviceSnapshotResult.completed
  where device.platform.vendor != Vendor.FORWARD_CUSTOM
  foreach interface in device.interfaces
  where isPresent(interface.routedVlan?.ipv6?.fhrp?.vrrp?.fhrpGroups)
  foreach group in interface.routedVlan.ipv6.fhrp.vrrp.fhrpGroups
  select {
    protocol: "vrrp3",
    group_id: group.virtualRouterId,
    name: "vrrp",
    device: device.name,
    interface: interface.name,
    vrf: if isPresent(interface.routedVlan.networkInstanceName)
      then if interface.routedVlan.networkInstanceName != "default" then interface.routedVlan.networkInstanceName else null : String
      else null : String,
    address: ipSubnet(group.virtualAddress, 128),
    state: group.state,
    priority: 100,
    status: "active"
  };

@primaryKey(protocol, group_id, address, device, interface, vrf)
foreach row in (
  subinterface_ipv4
  + subinterface_ipv6
  + routed_vlan_ipv4
  + routed_vlan_ipv6
  + subinterface_vrrp_ipv4
  + subinterface_vrrp_ipv6
  + routed_vlan_vrrp_ipv4
  + routed_vlan_vrrp_ipv6
)
select distinct row
```

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
- Enabled: enabled by default
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

## Forward BGP Peers

- `NetBox Model`: `netbox_routing.bgppeer`
- Expected fields: `device`, `vrf`, `local_asn`, `neighbor_address`, `peer_asn`, `enabled`, `status`
- Query file: [`forward_bgp_peers.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_bgp_peers.nqe)
- Enabled: enabled by default
- Feature flag: enabled unless `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = False`
- Optional dependency: requires the `netbox-routing` NetBox plugin
- Stability: beta

The BGP peer map uses Forward structured BGP neighbor state, not raw configuration parsing and not BGP RIB data. The query prefers explicit `localAS`, falls back to the BGP process `asNumber`, then uses reciprocal Forward peer evidence or explicit internal-BGP peer AS when those are uniquely available. The adapter creates or reuses native NetBox `RIR`, `ASN`, `VRF`, and peer `IPAddress` records, then creates `netbox-routing` routers, scopes, and peers. Missing optional plugin models are recorded as row failures so the rest of the shard can continue.

For shard-scoped Branching retries, the plugin applies native Forward column
filters for the row-owning device and then enforces the shard boundary again in
NetBox before applying rows. Reciprocal peer lookups intentionally remain part
of the full query semantics because local AS and OSPF router identity inference
can depend on peer evidence outside the current shard.

## Forward BGP Address Families

- `NetBox Model`: `netbox_routing.bgpaddressfamily`
- Expected fields: `device`, `vrf`, `local_asn`, `afi_safi`
- Query file: [`forward_bgp_address_families.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_bgp_address_families.nqe)
- Enabled: enabled by default
- Feature flag: enabled unless `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = False`
- Optional dependency: requires the `netbox-routing` NetBox plugin
- Stability: beta

The BGP address-family map joins Forward BGP RIB AFI/SAFI state back to structured BGP neighbor rows. It creates native `netbox-routing` address-family objects on the matching BGP scope without importing route-table entries. Forward `L3VPN_*` address-family names are normalized to the native `netbox-routing` `vpnv4-*` and `vpnv6-*` values.

## Forward BGP Peer Address Families

- `NetBox Model`: `netbox_routing.bgppeeraddressfamily`
- Expected fields: `device`, `vrf`, `local_asn`, `neighbor_address`, `peer_asn`, `afi_safi`, `enabled`
- Query file: [`forward_bgp_peer_address_families.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_bgp_peer_address_families.nqe)
- Enabled: enabled by default
- Feature flag: enabled unless `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = False`
- Optional dependency: requires the `netbox-routing` NetBox plugin
- Stability: beta

The BGP peer address-family map links each native BGP peer to the AFI/SAFI rows observed in Forward's BGP RIB metadata. It does not import individual prefixes, AS paths, communities, or route attributes.

## Forward OSPF Instances

- `NetBox Model`: `netbox_routing.ospfinstance`
- Expected fields: `device`, `vrf`, `process_id`, `router_id`
- Query file: [`forward_ospf_instances.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_ospf_instances.nqe)
- Enabled: enabled by default
- Feature flag: enabled unless `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = False`
- Optional dependency: requires the `netbox-routing` NetBox plugin
- Stability: beta

The OSPF instance map uses Forward structured OSPF neighbor state and inferred reverse-neighbor relationships to source a unique process-level local router ID. Forward named process IDs are converted to deterministic numeric NetBox process IDs, with the original Forward process label preserved in comments.

## Forward OSPF Areas

- `NetBox Model`: `netbox_routing.ospfarea`
- Expected fields: `area_id`, `area_type`
- Query file: [`forward_ospf_areas.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_ospf_areas.nqe)
- Enabled: enabled by default
- Feature flag: enabled unless `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = False`
- Optional dependency: requires the `netbox-routing` NetBox plugin
- Stability: beta

The OSPF area map creates native NetBox routing areas from Forward structured OSPF areas and maps Forward area type values to NetBox routing choices.

## Forward OSPF Interfaces

- `NetBox Model`: `netbox_routing.ospfinterface`
- Expected fields: `device`, `process_id`, `router_id`, `area_id`, `area_type`, `local_interface`
- Query file: [`forward_ospf_interfaces.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_ospf_interfaces.nqe)
- Enabled: enabled by default
- Feature flag: enabled unless `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = False`
- Optional dependency: requires the `netbox-routing` NetBox plugin
- Stability: beta

The OSPF interface map binds native OSPF instances and areas to exact NetBox interfaces. It can import local OSPF interface rows even when a specific neighbor lacks remote-peer inference, provided Forward exposes enough reciprocal evidence elsewhere in the same process to infer a unique local router ID. Missing interfaces are recorded as row failures so the rest of the shard can continue.

## Forward Peering Sessions

- `NetBox Model`: `netbox_peering_manager.peeringsession`
- Expected fields: `device`, `vrf`, `local_asn`, `neighbor_address`, `peer_asn`, `enabled`, `status`, `relationship`, `relationship_slug`, `service_reference`
- Query file: [`forward_peering_sessions.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/forward_peering_sessions.nqe)
- Enabled: enabled by default
- Feature flag: enabled unless `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = False`
- Optional dependencies: requires `netbox-routing` and `netbox-peering-manager`
- Stability: beta

The peering session map is an overlay on top of `netbox-routing`. Applying a peering-session row first ensures the matching BGP peer exists, then links a `netbox-peering-manager` session to it. The shipped query uses Forward peer type only as a simple relationship hint; richer peering policy, prefix-list, and IRR modeling should remain a separate feature.

When any optional routing map is enabled, the sync also runs an internal read-only diagnostic query that reports routing rows the beta maps cannot import safely, including BGP neighbors without explicit or inferred local AS, unsupported BGP address families, and OSPF neighbor rows that lack unique process-level router ID inference needed for native OSPF objects. This diagnostic query is not seeded as a NetBox import map and does not create, update, or delete NetBox objects. See the query file for the complete diagnostic text:

```text
forward_netbox/queries/forward_routing_import_diagnostics.nqe
```

For routing maps on large datasets, publish the NQE into the Forward NQE library and bulk bind the NetBox maps to the committed repository query paths. The first successful run establishes a full baseline; later `latestProcessed` runs can use Forward NQE diffs when every enabled map for the model is backed by a repository path or direct query ID.

## Important Caveats

- `dcim.inventoryitem` remains the default best-fit path for generic components.
- `dcim.module` is enabled by default and remains beta; set the feature flag to `False` only when you want bay-aware hardware kept in inventory items instead of modules.
- The module path requires module bays to already exist on the target device. Rows whose `module_bay` value is missing in NetBox are skipped with a non-blocking warning instead of creating `dcim.modulebay` side effects during module sync.
- Before enabling module sync, run `python manage.py forward_module_readiness --sync-name "<sync name>"` to generate a readiness summary and native NetBox module-bay import CSV for missing bays.
- `dcim.module` uses a conservative branch density because module rows still depend on device, module type, and bay readiness.
- SFP/transceiver rows remain in the inventory-item path by default; do not enable module import expecting optics to become NetBox modules unless the query is customized for device types that expose matching module bays.
- Optional routing maps are visible unless `enable_bgp_sync` is false, and they still require the target optional NetBox plugins to be installed.
- BGP policy objects such as route maps, prefix lists, and communities are not part of the normalized built-in path. Those objects require a separate config-derived query layer with vendor-specific contracts.
