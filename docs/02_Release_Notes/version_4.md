---
description: v4 Release Notes
---

# v4 Release Notes

## v4.3.0 (2025-09-23)

!!! warning

    This release requires at least NetBox v4.4.0.

#### Changes
- Support for NetBox v4.4.0+.
- Optimization: Objects are checked for changes before validation and saving to the database. This should significantly reduce the time needed for syncing when there are only few changes.
- Replace custom `Sync` table with NetBox's built-in list table.
- Added option to completely skip syncing custom fields to avoid empty updates.
- Added full GraphQL support for all models.
- `ForwardSource` API now supports `brief` parameter.
- API serializers now support `depth` parameter to control how many nested objects are serialized.
- All direct REST API endpoints now support full CRUD operations except for `ForwardSnapshot`, `ForwardIngestion` and `ForwardIngestionIssue` which are read-only.
- Removed `type` from `ForwardSyncForm`, all models are now visible by default.
- `ForwardTransformMap` and `ForwardTransformMapGroup` forms now allows for tagging.
- Custom permissions now follow NetBox's standard naming scheme _`<app_label>.<action>_<model_name>`_. Following renames were made:
  - `forward_netbox.tm_restore` -> `forward_netbox.restore_forwardtransformmap`
  - `forward_netbox.forwardtransformmap_add` -> `forward_netbox.clone_forwardtransformmap`
  - `forward_netbox.sync_source` -> `forward_netbox.sync_forwardsource`
  - `forward_netbox.start_sync` -> `forward_netbox.start_forwardsync`
  - `forward_netbox.merge_ingestion` -> `forward_netbox.merge_forwardingestion`
- When cloning an `ForwardTransformMap`, the redirect now points to the clones map.
- Source is found using `Site` when custom field containing `ForwardSource` ID is empty. This happens when custom fields are ignored during sync.
- All sync parameters are now enabled by default on new sync creation.
- Updated `netbox-branching` to 0.7.0.

#### Fixes
- Improved docs section regarding upgrade issues with NetBox v4.3.0+.
- `extra` group for FWD SDK version `7.3` was incorrectly pointing to `7.2`.
- Fixed wrong filter set on `ForwardRelationshipField` API view.
- `ForwardSource.status` API serializer is now correctly read-only.
- Added missing filter sets for `ForwardSync` and `ForwardRelationshipField` API views.
- `brief` parameter in most API serializers will now correctly return simplified data.
- Fixed several small bugs in forms backend found with new tests.
- Removed obsolete GitLab reference when restoring transform maps.
- All views now correctly handle situation where non HTMX request is made when only HTMX is expected.
- `ForwardSync` bulk deletion was using `ForwardSnapshot` table instead `ForwardSync` table.
- Since NetBox 4.3.0 Topology button was show on all models. It's back to only `Site`.
- Values in `ForwardSync` form were not populated from existing instance on edit.
- `ForwardSync` form HTMX GET URL was getting longer with each change of `source` field.

## v4.2.2 (2025-09-12)

#### Fixes
- `Source` detail view no longer crashes with `'str' object has no attribute '_meta'` error on NetBox 4.3.7+.

## v4.2.1 (2025-08-27)

#### Fixes
- Topology no longer crashes when opening `Site` detail.

## v4.2.0 (2025-07-18)

#### Changes

- Enhanced [Installation Guide](../01_User_Guide/installation.md).
- All data in Ingestion detail views (newly statistics and tabs) are now updated every 5 seconds with the logs.
- Most issues during ingestion are now stored as `ForwardIngestionIssue` objects and can be viewed in separate Ingestion detail tab view.
- Ingestion will show as passed when no errors occurred and only some issues were found.
- Added `extra` group for FWD SDK version `7.3`.

#### Fixes
- Device topology view was using the wrong data for Site ID.
- Sync settings are more strictly respected in the sync process. This applies for all models but `Device` and `Interface` which need to be obtained from the database if their child models are asked to be synced.

## v4.1.0 (2025-06-24)

#### Changes

- Added `ForwardTransformMapGroup` model to group `ForwardTransformMap` objects together.
- Restoring transform maps restores only those with no group assigned.
- Sync now prioritizes using transform maps from groups specified in the sync settings.
- Dropped `extra` group support for FWD SDK < `6.10` and added version `7.2`.

#### Fixes
- Fixed topology view hanging on error.
- Updated docs to incorporate `local_settings.py` into `configuration.py`.

## v4.0.1 (2025-06-03)

#### Fixes
- NetBox plugin list showed the previous version as installed.

## v4.0.0 (2025-05-27)

#### Changes

- We are now using [netbox-branching](https://github.com/netboxlabs/netbox-branching) to handle staging.

!!! danger "Required upgrade steps"
    For successful migration please follow [netbox-branching installation instructions](https://docs.netboxlabs.com/netbox-extensions/branching/#plugin-installation). Step 3 and 4 should be enough since the plugin is installed as dependency and just needs enabling and configuring.

    Simplified installation instructions:

    Modify your `configuration.py` with the following content. Replace `$ORIGINAL_DATABASE_CONFIG` with your original `DATABASE` configuration dictionary. If you are using other `DATABASE_ROUTERS`, make sure to include them in the list.
    ```python
    from netbox_branching.utilities import DynamicSchemaDict

    # Wrap DATABASES with DynamicSchemaDict for dynamic schema support
    DATABASES = DynamicSchemaDict({
        'default': $ORIGINAL_DATABASE_CONFIG,
    })

    # Employ netbox-branching custom database router
    DATABASE_ROUTERS = [
        'netbox_branching.database.BranchAwareRouter',
    ]

    # Add `netbox-branching` to plugins list (must be last!)
    PLUGINS = [
        # ...
        'netbox_branching',
    ]
    ```

!!! danger
    When upgrading to v4.0.0+, make sure to upgrade at the same step as NetBox to 4.3.0+. If you already upgraded your NetBox instance, follow [Cannot resolve bases for `[<ModelState: 'forward_netbox.ForwardBranch'>]`](../01_User_Guide/FAQ.md#cannot-resolve-bases-for-modelstate-forward_netboxforwardbranch) instructions.

- Renamed `ingestion -> sync` and `branch -> ingestion` to avoid conflicts with [netbox-branching](https://github.com/netboxlabs/netbox-branching).
- Renamed `Name -> Object` in list of ingestion changes and linked to the item if it exists.
- Removed unused `status` field from `ForwardTransformMap` and cleaned all references.

#### Fixes
- Quick search for staged changes was ignoring their values.
- Sync for `InventoryItem` failed when name was longer than 64 characters.
- Unable to delete Forward Ingestion in UI.
- Data were not validated on model save.

!!! warning

    This will cause more failures during data ingestion. These are not new, but are not hidden anymore. It would cause error when manipulating the faulty object synced by previous versions.

- Ingestion Job was listed in Sync and not in Ingestion.
- The Default value for template is empty string instead of `None`.
- `duplex` and `speed` on `Interface` take `unknown` value as `None`.
- Correctly set `DeviceType` when `model` is empty.
- Convert platform to `slug` when using it for `DeviceType`.
- Ignored `VLAN` with `VLAN ID` 0.
- Ignore `InventoryItem`s with `None` serial number.
