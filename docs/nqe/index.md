# Forward Enterprise → NetBox NQE Queries

This directory tracks the NQE source files used to populate NetBox through the Forward Enterprise plugin. Each query emits JSON that satisfies `nqe-json-reference.md`. All queries were validated against `https://fwd.app` on network `172989` (snapshot `753593`).

## Running the Queries

```bash
curl -sS \
  -H 'Authorization: Basic Y3JhaWdqb2huc29uK3Nld2VzdEBmb3J3YXJkbmV0d29ya3MuY29tOlNoNG1yMDtja0BA' \
  -H 'Content-Type: application/json' \
  -X POST 'https://fwd.app/api/nqe?networkId=172989' \
  --data '{"query": "<paste query text>", "queryOptions": {"limit": 5}}'
```

Adjust `limit`/`offset` in `queryOptions` to paginate large datasets.

## Dataset Reference

All code samples below inline the `.nqe` files using `pymdownx.snippets`.

### `dcim.manufacturer`

```nqe
--8<-- "dcim.manufacturer.nqe"
```

Sample (`limit: 5`):

```json
[
  {"name": "F5", "slug": "f5"},
  {"name": "JUNIPER", "slug": "juniper"},
  {"name": "ARISTA", "slug": "arista"},
  {"name": "CISCO", "slug": "cisco"},
  {"name": "PALO ALTO NETWORKS", "slug": "palo-alto-networks"}
]
```

### `dcim.devicerole`

```nqe
--8<-- "dcim.devicerole.nqe"
```

```json
[
  {"name": "LOAD BALANCER", "slug": "load-balancer"},
  {"name": "ROUTER", "slug": "router"},
  {"name": "FIREWALL", "slug": "firewall"},
  {"name": "SWITCH", "slug": "switch"},
  {"name": "OTHER", "slug": "other"}
]
```

### `dcim.devicetype`

```nqe
--8<-- "dcim.devicetype.nqe"
```

```json
[
  {"manufacturer": "F5", "model": "BIG-IP Virtual Edition", "slug": "big-ip-virtual-edition"},
  {"manufacturer": "JUNIPER", "model": "vmx", "slug": "vmx"},
  {"manufacturer": "ARISTA", "model": "vEOS", "slug": "veos"}
]
```

### `dcim.device`

```nqe
--8<-- "dcim.device.nqe"
```

```json
[
  {"name": "sjc-dc12-acc307", "serial": null, "manufacturer": "ARISTA", "device_type": "vEOS", "platform": "ARISTA_EOS", "role": "ROUTER", "site": "San Jose", "status": "active", "tags": []},
  {"name": "atl-dc01-acc01", "serial": null, "manufacturer": "ARISTA", "device_type": "vEOS", "platform": "ARISTA_EOS", "role": "ROUTER", "site": "Atlanta", "status": "active", "tags": []},
  {"name": "pan-fw1b", "serial": "unknown", "manufacturer": "PALO ALTO NETWORKS", "device_type": "PA-VM", "platform": "PAN_OS", "role": "FIREWALL", "site": "AWS US West (Oregon)", "status": "active", "tags": []}
]
```

### `dcim.interface`

```nqe
--8<-- "dcim.interface.nqe"
```

```json
[
  {"device": "sjc-dc12-acc307", "name": "ma1", "type": "1000base-t", "speed": 1000, "enabled": true, "mtu": 1500, "mac_address": "00:09:00:00:00:3b", "description": null},
  {"device": "sjc-dc12-acc307", "name": "et3", "type": "1000base-t", "speed": 1000, "enabled": true, "mtu": 9214, "mac_address": "00:09:00:00:00:3e", "description": null}
]
```

### `dcim.cable`

```nqe
--8<-- "dcim.cable.nqe"
```

```json
[
  {"a_device": "sjc-dc12-acc307", "a_interface": "et1", "b_device": "sjc-dc12-acc304", "b_interface": "et1"}
]
```

### `dcim.virtualchassis`

```nqe
--8<-- "dcim.virtualchassis.nqe"
```

```json
[
  {"name": "sjc-fabric-vc", "master": "sjc-dc12-acc304", "domain": "default", "members": ["sjc-dc12-acc304", "sjc-dc12-acc307"]},
  {"name": "atl-core-vc", "master": "atl-dc01-acc01", "domain": "default", "members": ["atl-dc01-acc01", "atl-dc01-acc07"]}
]
```

### `dcim.location`

```nqe
--8<-- "dcim.location.nqe"
```

```json
[
  {"name": "AWS", "city": null, "admin_division": null, "country": null},
  {"name": "Azure", "city": null, "admin_division": null, "country": null},
  {"name": "atl-acc", "city": "Atlanta", "admin_division": "Georgia", "country": "United States"}
]
```

### `ipam.prefix`

```nqe
--8<-- "ipam.prefix.nqe"
```

```json
[
  {"prefix": "10.110.3.90/31", "site": "Atlanta", "vrf": "8"},
  {"prefix": "0.0.0.0/0", "site": "Atlanta", "vrf": "8"},
  {"prefix": "10.110.3.72/31", "site": "Atlanta", "vrf": "1"}
]
```

### `ipam.ipaddress`

```nqe
--8<-- "ipam.ipaddress.nqe"
```

```json
[
  {"address": "10.100.0.117/32", "device": "atl-app-lb01", "interface": "mgmt"},
  {"address": "10.100.0.112/31", "device": "atl-ce01", "interface": "ge-0/0/0"}
]
```

### `ipam.vlan`

```nqe
--8<-- "ipam.vlan.nqe"
```

```json
[
  {"name": "/Common/vlan101", "vid": 101, "site": "Atlanta"},
  {"name": "/Common/vlan102", "vid": 102, "site": "Atlanta"}
]
```

### `ipam.vrf`

```nqe
--8<-- "ipam.vrf.nqe"
```

```json
[
  {"name": "8", "enforce_unique": false},
  {"name": "1", "enforce_unique": false}
]
```

### `dcim.inventoryitem`

```nqe
--8<-- "dcim.inventoryitem.nqe"
```

```json
[
  {"device": "sjc-dc12-acc307", "name": "Chassis", "serial": "JPE12345678", "manufacturer": "ARISTA", "part_id": "DCS-7050", "description": null}
]
```

### `extras.os_lifecycle`

```nqe
--8<-- "extras.os_lifecycle.nqe"
```

```json
[
  {"device": "sjc-dc12-acc307", "os": "ARISTA_EOS", "version": "4.29.2F", "last_maintenance": "2023-11-01", "last_support": "2025-12-31", "last_vulnerability": "2024-05-12"}
]
```
