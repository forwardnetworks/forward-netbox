# Model Mapping Matrix

This matrix summarizes how the shipped Forward NQE maps populate each NetBox model.

`Exact` means the Forward data maps cleanly to the NetBox object without a major semantic compromise. `Best-fit` means the plugin intentionally approximates the Forward concept to the closest useful NetBox model.

| NetBox Model | Built-In Query | Forward Source Shape | Mapping Style | Notes |
| --- | --- | --- | --- | --- |
| `dcim.site` | `Forward Locations` | `network.locations` filtered by collected device locations | Exact | Site names/slugs are shaped in NQE. |
| `dcim.manufacturer` | `Forward Device Vendors` | `device.platform.vendor` | Exact | One manufacturer per Forward vendor enum, with built-in names and slugs canonicalized in NQE. |
| `dcim.devicerole` | `Forward Device Types` | `device.platform.deviceType` | Best-fit | Forward device type is used as the NetBox device role. |
| `dcim.platform` | `Forward Platforms` | `device.platform.os` plus vendor | Exact | Platform slugs are shaped in NQE. |
| `dcim.devicetype` | `Forward Device Models` | `device.platform.model` plus vendor | Best-fit | Forward model is used as both model and part number. |
| `dcim.device` | `Forward Devices` | `network.devices` | Exact | Device lookup is by NetBox device name. |
| `dcim.virtualchassis` | `Forward Virtual Chassis` | `device.ha.vpc.domainId` and `device.ha.mlagPeer` | Best-fit | Maps vPC/MLAG-style HA to NetBox virtual chassis with deterministic naming and domain keys. |
| `dcim.interface` | `Forward Interfaces` | Ethernet interfaces under `device.interfaces` | Best-fit | NetBox interface type is derived from negotiated speed lookup values. |
| `dcim.macaddress` | `Forward MAC Addresses` | `interface.ethernet.macAddress` | Exact | MACs are assigned to interfaces by exact interface name. |
| `dcim.inventoryitem` | `Forward Inventory Items` | `device.platform.components` | Best-fit | Forward component part type is used as the inventory item role. |
| `ipam.vlan` | `Forward VLANs` | `device.networkInstances[].vlans` | Best-fit | VLANs are site-scoped using the device location. |
| `ipam.vrf` | `Forward VRFs` | non-default `device.networkInstances` | Best-fit | Route distinguisher is not supplied by Forward and remains null. |
| `ipam.prefix` | `Forward IPv4 Prefixes` | `networkInstances[].afts.ipv4Unicast.ipEntries` | Best-fit | Host-route receive/drop entries are filtered in NQE. |
| `ipam.prefix` | `Forward IPv6 Prefixes` | `networkInstances[].afts.ipv6Unicast.ipEntries` | Best-fit | Host-route receive/drop entries are filtered in NQE. |
| `ipam.ipaddress` | `Forward IP Addresses` | interface, subinterface, bridge, tunnel, and routed VLAN L3 addresses | Best-fit | Subinterface addresses are anchored to the parent interface name so NetBox interface lookups stay deterministic. |

## Important Caveats

- `dcim.virtualchassis` is a pragmatic approximation for HA pairs and domains, not a claim that all Forward HA constructs are shared-control-plane switch stacks.
- The plugin intentionally keeps NetBox-ready shaping in NQE where possible. Python adapters apply rows and enforce object lookups; they should not silently normalize meaning after the query runs.
- Manufacturer-bearing built-in queries intentionally canonicalize vendor names and slugs in NQE through the shared `netbox_utilities` module. If your NetBox already uses different curated manufacturer rows, copy the query set and update `manufacturer_name_overrides` there before syncing.
- Interface and IP assignment remain intentionally strict: if the built-in queries drift from exact interface names, the sync should record issues rather than guessing.
