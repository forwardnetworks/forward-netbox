# Forward Enterprise NQE JSON Reference

This guide documents the JSON payloads expected by the Forward Enterprise NetBox plugin. Use it when building or validating Forward NQE queries so the plugin’s data loaders receive the fields they expect.

## 1. Global Requirements

- Queries must be paginable: the plugin requests batches using `offset` and `limit` (default 1000).
- Return JSON objects (not arrays) with a top-level `items`/`results` or a bare array; the client normalises both patterns.
- Field names are case-sensitive; follow the tables below exactly.

---

## 2. Snapshot Discovery

Used by the **Sync Snapshots** job (`ForwardSource.sync`).

**Required fields per snapshot item:**

| Key                     | Type(s)      | Description                                              |
|-------------------------|--------------|----------------------------------------------------------|
| `snapshot_id` or `id`   | string/int   | Unique Forward snapshot identifier                       |
| `name` (or `note`)      | string       | Human-friendly label; note takes precedence if present   |
| `status` / `state`      | string       | Expected values: `loaded`, `done`, `PROCESSED`           |
| `start` / `started_at`  | string       | ISO-8601 timestamp                                       |
| `end` / `finished_at`   | string       | ISO-8601 timestamp                                       |
| `processed_at_millis`   | integer      | Millisecond epoch; used to determine the “latest” snapshot |
| `creation_date_millis`  | integer      | Fallback ordering metric                                 |
| `network_id`            | string       | Forward network identifier (matches NetBox source field) |

**Optional fields:**

| Key             | Description                       |
|-----------------|-----------------------------------|
| `note` / `notes`| User-defined note; becomes the snapshot name |
| `snapshot_ref`  | Alternative reference; used if `snapshot_id` missing |

**Plugin behaviour:**
- Stores every snapshot under its native `snapshot_id`.
- Derives the display name from `note` (trimmed) or falls back to `name`/`(no note)`.
- Creates/updates a special `$last` record whose `data.resolved_snapshot_id` references the latest snapshot.

---

## 3. Device Dataset (`dcim.device`)

Used when device syncing is enabled (always enabled in current workflow).

| Key            | Type(s)                 | Description                                   |
|----------------|-------------------------|-----------------------------------------------|
| `name`         | string                  | Device name (unique per site)                 |
| `serial`       | string / list[str]      | Serial number(s); first non-empty used        |
| `manufacturer` | string                  | Vendor name                                   |
| `device_type`  | string                  | Hardware model                                |
| `platform`     | string (optional)       | Device OS/platform                            |
| `role`         | string (optional)       | Device role label                             |
| `site` / `location` | string             | Site/location name                            |
| `status`       | string (optional)       | Ignored; NetBox sets status to “Active”       |
| `tags`         | list[str]/list[dict]    | Optional tags (`"core"` or `{ "name": "core" }`) |

Notes:
- Filter out Forward-generated synthetic devices.
- Ensure `site` names match existing NetBox sites or can be created via location sync.

---

## 4. Manufacturer Dataset (`dcim.manufacturer`)

| Key    | Type   | Description                |
|--------|--------|----------------------------|
| `name` | string | Manufacturer name          |
| `slug` | string (optional) | Slugified name |

---

## 5. Device Role Dataset (`dcim.devicerole`)

| Key    | Type   | Description          |
|--------|--------|----------------------|
| `name` | string | Device role name     |
| `slug` | string (optional) | Role slug |

---

## 6. Device Type Dataset (`dcim.devicetype`)

| Key            | Type   | Description                 |
|----------------|--------|-----------------------------|
| `model`        | string | Model name                  |
| `manufacturer` | string | Manufacturer name           |
| `slug`         | string (optional) | Model slug |

---

## 7. Location Dataset (`dcim.location`)

| Key            | Type   | Description                                     |
|----------------|--------|-------------------------------------------------|
| `name`         | string | Location/site name                              |
| `parent`       | string (optional) | Parent location name              |
| `status`       | string (optional) | Not currently consumed             |

---

## 8. Interface Dataset (`dcim.interface`)

| Key            | Type(s)                | Description                                                                                |
|----------------|------------------------|--------------------------------------------------------------------------------------------|
| `device`       | string                 | Associated device name                                                                     |
| `name`         | string                 | Interface name                                                                             |
| `type`         | string                 | NetBox interface type slug (`1000base-t`, `10gbase-x-sfpp`, etc.); defaults to `1000base-t` when speed unknown |
| `speed`        | integer                | Provisioned speed in Mbps; defaults to `1000` when the device does not report a speed      |
| `enabled`      | bool/string            | Truthy means admin up                                                                      |
| `mtu`          | integer (optional)     | Defaults to 1500                                                                           |
| `mac_address`  | string (optional)      | Canonical MAC (AA:BB:CC:DD:EE:FF)                                                          |
| `description`  | string (optional)      | Interface description                                                                      |

Notes:
- When `speed` cannot be inferred from Forward’s dataset, populate it with the fallback value defined at the top of the query (`defaultInterfaceSpeedMbps`). The same constant drives the default `type` slug (`defaultInterfaceType`), which you can adjust to match your environment.
- Supply the admin-state as either a boolean or string; the importer coerces truthy/falsey values.

---

## 9. Cable Dataset (`dcim.cable`)

| Key            | Type(s) | Description                                                   |
|----------------|---------|---------------------------------------------------------------|
| `a_device`     | string  | Name of the first device                                      |
| `a_interface`  | string  | Interface on the first device                                 |
| `b_device`     | string  | Name of the peer device                                       |
| `b_interface`  | string  | Interface on the peer device                                  |
| `type`         | string (optional) | NetBox cable type slug (e.g. `cat6a`, `smf-1310nm`) |
| `label`        | string (optional) | Human-readable label                                 |

Notes:
- The Forward dataset only exposes physical adjacency; cable attributes such as type and label are optional. Use defaults or enrich with your own metadata.
- Ensure the query de-duplicates bidirectional links so each cable record is emitted once.

---

## 10. Virtual Chassis Dataset (`dcim.virtualchassis`)

| Key        | Type(s)     | Description                                                            |
|------------|-------------|------------------------------------------------------------------------|
| `name`     | string      | Virtual chassis name                                                   |
| `master`   | string      | Name of the device acting as VC master (optional)                      |
| `members`  | list[string]| Device names participating in the chassis (minimum two members)        |
| `domain`   | string      | Virtual chassis domain label (optional)                                |

Notes:
- Provide device names that match the `dcim.device` dataset to allow lookups.
- If you cannot determine the master, omit the field and handle the assignment during the NetBox import (for example, by promoting the first member).

---

## 11. Prefix Dataset (`ipam.prefix`)

| Key            | Type(s) | Description                                       |
|----------------|---------|---------------------------------------------------|
| `prefix`       | string  | Network CIDR (e.g. `10.0.0.0/24`)                 |
| `site`         | string (optional) | Site name                             |
| `tenant`       | string (optional) | Tenant name                            |
| `vrf`          | string (optional) | VRF name                               |

---

## 12. IP Address Dataset (`ipam.ipaddress`)

| Key            | Type(s) | Description                                       |
|----------------|---------|---------------------------------------------------|
| `address`      | string  | Host address CIDR (`10.0.0.1/24`)                 |
| `device`       | string (optional) | Device binding (via interface)     |
| `interface`    | string (optional) | Interface name                      |
| `status`       | string (optional) | Ignored; defaults to Active          |

---

## 13. VLAN Dataset (`ipam.vlan`)

| Key            | Type   | Description                                       |
|----------------|--------|---------------------------------------------------|
| `name`         | string | VLAN name                                        |
| `vid` / `id`   | int    | VLAN ID                                          |
| `site`         | string (optional) | Site association                       |
| `tenant`       | string (optional) | Tenant                                 |

---

## 14. VRF Dataset (`ipam.vrf`)

| Key               | Type(s) | Description                                                     |
|-------------------|---------|-----------------------------------------------------------------|
| `name`            | string  | VRF name                                                        |
| `rd`              | string (optional) | Route distinguisher (include if available)           |
| `tenant`          | string (optional) | Tenant name                                            |
| `enforce_unique`  | bool (optional)   | Whether to enforce unique IP space within the VRF     |

Notes:
- Populate `rd` and `tenant` when Forward can derive them; otherwise leave them empty and the loader will skip those attributes.
- Default `enforce_unique` to `false` unless you require strict uniqueness semantics in NetBox. The provided starter query emits only `name` and `enforce_unique`; extend it as you surface additional fields.

---

## 15. Inventory Item Dataset (`dcim.inventoryitem`)

| Key            | Type(s) | Description                                                          |
|----------------|---------|----------------------------------------------------------------------|
| `device`       | string  | Owning device name                                                   |
| `name`         | string  | Display label for the component                                      |
| `manufacturer` | string  | Manufacturer name (optional; defaults to the parent device vendor)   |
| `serial`       | string  | Component serial number                                              |
| `part_id`      | string  | Part/PID identifier                                                  |
| `description`  | string (optional) | Free-form notes                                       |

Notes:
- Only emit components with at least one of `name`, `serial`, or `part_id` populated; others are ignored by the loader.
- Trim overly long serials or names to NetBox model limits (the provided query caps serial length at 255 characters).

---

## 16. OS Lifecycle Dataset (`extras.os_lifecycle`)

| Key                 | Type(s) | Description                                                            |
|---------------------|---------|------------------------------------------------------------------------|
| `device`            | string  | Device name                                                            |
| `os`                | string  | Operating system identifier (e.g., `ARISTA_EOS`)                       |
| `version`           | string  | OS version string                                                      |
| `last_maintenance`  | string (optional) | Vendor last maintenance date (ISO-8601)              |
| `last_support`      | string (optional) | Vendor last support date (ISO-8601)                  |
| `last_vulnerability`| string (optional) | Last known vulnerability date (ISO-8601)             |

Notes:
- These fields map to Forward’s `osSupport` structure and can be stored in NetBox custom fields or an auxiliary model.
- Dates should be emitted as strings in ISO-8601 format; omit fields if Forward does not supply the values.

---

## 12. Tips for Building Queries

1. **Normalize strings**: trim whitespace, set consistent casing for site names, etc.
2. **Avoid empty placeholders**: replace `"unknown"`/`"none"` serials with an empty string; the loader will skip them gracefully.
3. **Limit results**: exclude transient or Forward-generated synthetic objects directly in the query.
4. **Test iteratively**: validate each dataset against a staging NetBox before running full syncs.

By following these structures, Forward NQE responses will import cleanly into NetBox via the plugin.
