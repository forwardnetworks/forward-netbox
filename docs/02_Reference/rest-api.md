# REST API Reference

The plugin registers standard NetBox REST endpoints under the plugin API root:

```
/api/plugins/forward/
```

All endpoints require NetBox API authentication (token) and honor NetBox model
permissions. Standard DRF list/detail/create/update/delete verbs apply to each
model endpoint; the browsable API and the schema at `/api/schema/` document field
shapes.

## Model endpoints

| Endpoint | Model | Notes |
| --- | --- | --- |
| `/api/plugins/forward/source/` | Forward source | Connection + sync-scope config. |
| `/api/plugins/forward/sync/` | Forward sync | A configured sync; triggerable (see actions). |
| `/api/plugins/forward/ingestion/` | Ingestion run | One executed sync run + its results. |
| `/api/plugins/forward/ingestion-issues/` | Ingestion issue | Per-run diagnostics. |
| `/api/plugins/forward/nqe-map/` | NQE map | Model ↔ NQE query bindings. |
| `/api/plugins/forward/device-analysis/` | Device analysis | Per-device analysis records. |
| `/api/plugins/forward/drift-policy/` | Drift policy | Bidirectional-drift rules. |
| `/api/plugins/forward/validation-run/` | Validation run | Query-validation run records. |

## Custom actions

Read-only source discovery helpers (GET, list-level — used by the UI form,
available over the API too):

| Action | Path |
| --- | --- |
| Available networks | `GET /api/plugins/forward/source/available-networks/` |
| Available device tags | `GET /api/plugins/forward/source/available-tags/` |
| Available query folders | `GET /api/plugins/forward/source/available-query-folders/` |
| Available queries | `GET /api/plugins/forward/source/available-queries/` |
| Available query commits | `GET /api/plugins/forward/source/available-query-commits/` |
| Available snapshots | `GET /api/plugins/forward/source/available-snapshots/` |

Sync operations (POST, detail-level — `<id>` is the sync PK):

| Action | Path | Effect |
| --- | --- | --- |
| Trigger sync | `POST /api/plugins/forward/sync/<id>/sync/` | Enqueues a sync run. Inventory-wide write — see the trust boundary in [SECURITY.md](https://github.com/forwardnetworks/forward-netbox/blob/main/SECURITY.md). |
| Validate | `POST /api/plugins/forward/sync/<id>/validate/` | Runs the query-validation checks for the sync. |
| Force allow | `POST /api/plugins/forward/ingestion/<id>/force_allow/` | Overrides a blocked ingestion (operator action). |

## Example

```bash
# List sources
curl -s -H "Authorization: Token $NETBOX_TOKEN" \
  https://netbox.example.com/api/plugins/forward/source/

# Trigger sync 5
curl -s -X POST -H "Authorization: Token $NETBOX_TOKEN" \
  https://netbox.example.com/api/plugins/forward/sync/5/sync/
```

> Do not paste API responses containing customer names, Forward network IDs, or
> snapshot IDs into issues — see the reporting note in `SECURITY.md`.
