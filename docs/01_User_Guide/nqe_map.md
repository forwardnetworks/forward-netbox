---
description: Managing Forward Enterprise NQE queries for the NetBox plugin.
---

# NQE Maps

The Forward NetBox plugin uses Forward Enterprise NQE queries to collect data for each NetBox model. The **NQE Maps** page lists every model that can be synced, the query ID that is executed, and whether the model is currently enabled. Use this view to adjust the default queries or to add additional DCIM/IPAM models that you want to sync.

## Viewing and Editing Entries

Each entry displays the app label and model (for example, `dcim.device`) together with the configured NQE query identifier. Click an entry to view its details or to edit the query ID, toggle whether the model is enabled, or add comments using the description field.

## Adding New Models

Select **Add** to create a new mapping. Choose the target model from the DCIM/IPAM content type list, then provide the NQE query ID that returns the required fields. Newly added models become available as toggles on the Forward Sync form and can be enabled per sync run.

## Restoring Defaults

If you need to revert to the shipped defaults, choose **Restore Defaults**. This action replaces the NQE map with the values from `forward_netbox/data/nqe_map.json`. Custom mappings are removed during the restore, so export any custom values you want to keep before running it.

## Forward Sync Toggles

Forward Syncs now reference the shared NQE map automatically. The sync form
exposes checkboxes for each model so you can choose which datasets to ingest,
but the underlying query IDs always come from the global NQE map. Update the
map here whenever you need to change the query for a given model.
