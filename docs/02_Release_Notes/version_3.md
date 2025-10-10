---
description: v3 Release Notes
---

# v3 Release Notes

## v3.2.4 (2025-04-07)

#### Fixes
- `0010` migration raised `DoesNotExist` when object from transform map was manually removed.

## v3.2.3 (2025-04-03)

#### Fixes
- Empty `site` filter caused `API_FAILED` when pulling inventory items.

## v3.2.2 (2025-04-03)

#### Changes
- Switch `uv` in docker development environment following NetBox container change.
- Remove passing `uuid` to `get_model_or_update()` and instead moving the logic to transform maps.
- Fields and relationships set to `coalesce` are now able to search for empty values when `None` is returned from transform map template.
- Most errors during ingestion are now more descriptive and don't abort the whole process.
- Statistics during ingestion run show total number from the very beginning and only count successful items.
- Ingestion process now respects settings for models.
- Failed merge will now provide more information about the error (logged to console).

#### Fixes
- NetBox v4.2.4 support:
  - [NetBox #18241](https://github.com/netbox-community/netbox/pull/18501) removed `LogLevelChoices.LOG_DEFAULT` causing `AttributeError`.
  - [NetBox #18286](https://github.com/netbox-community/netbox/pull/18642) removed `DurationChoices` causing `ImportError`.
- Inventory item `part_id=None` in IPF no longer fails, the value is set `unknown` instead.
- `coalesce` relationships are now respected when searching for model without syncing it.
- Allowed multiple IPs on single interface.
- Added missing site filter support to the `site`, `interface`, `inventoryitem`, and `ipaddress` models.

## v3.2.1 (2025-04-03)
YANKED

## v3.2.0 (2025-02-24)

### Enhancements
- `NIM-17843` - `transform_map.json` inside shipped package.
- `NIM-17847` - Transform maps are delivered as migration.
- `NIM-17848` - Custom fields are created in migration.
- `NIM-17851` - Update to support NetBox 4.2.0.
- `NIM-18142` - Allow to lock ipfabric SDK dependency to minor version.

### Bugs
- `NIM-18045` - TypeError when listed columns in branches contain ID.
- [#11](https://gitlab.com/ip-fabric/integrations/ipfabric-netbox/-/issues/11) - Fails to install in current docker container.

## v3.1.3 (2024-12-20)

### Bugs
- `SD-736` - Fix vulnerabilities with requests add timeout.

## v3.1.2 (2024-12-17)

### Enhancements
- `SD-733` - Migrate to Poetry for dependency management.
- `SD-734` - Add contributing guidelines.
- `SD-735` - Add development environment with docker.

## v3.1.1 (2024-12-10)

### Bug Fixes
- `SD-729` - Fix issue merging branch due to job validation.

### Enhancements
- `SD-725` - Add better logging and enable inventory item sync.


## v3.1.0 (2024-09-20)

### Enhancements
- `SD-702` - Update to support NetBox 4.1.0

## v3.0.3 (2024-06-16)

### Bug Fixes

- [Issue-1](https://gitlab.com/ip-fabric/integrations/ipfabric-netbox/-/issues/1) - Fixes issues with interfaces with `nameOriginal` in TM not syncing IP Addresses.

## v3.0.2 (2024-05-28)

### Enhancements

- `SD-649` - Maintain change logs during merge.

## v3.0.1 (2024-05-23)

### Bug Fixes

- `SD-647` - Alter sync template to support NetBox 4.0.1.

## v3.0.0b0 (2024-05-17)

### Enhancements

- `SD-646` - Support NetBox 4.0.

## v3.0.0 (2024-05-23

Release GA. No changes since v3.0.0b0.
