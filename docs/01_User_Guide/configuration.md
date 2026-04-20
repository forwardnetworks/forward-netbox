# Configuration

The plugin is configured through three primary objects:

- `Forward Source`
- `Forward NQE Map`
- `Forward Sync`

## Forward Sources

Create a `Forward Source` for each Forward deployment or tenant you want to sync from.

### Source Fields

- `Type`
  - `Forward SaaS` forces the base URL to `https://fwd.app` and keeps certificate verification enabled.
  - `Custom Forward deployment` lets you enter a custom base URL and control certificate verification.
- `Username`
  - Forward username used for basic authentication.
- `Password`
  - Forward password used for basic authentication.
  - Leave this blank only when editing an existing source to preserve the stored password.
  - New Forward accounts must have an active password before `Network` can be discovered.
- `Network`
  - Required default Forward network for syncs that use this source.
  - The field is populated dynamically from the authenticated tenant when valid credentials are supplied.
- `Timeout`
  - Forward API timeout in seconds.
  - Defaults to `1200` (20 minutes), aligned to the NQE timeout boundary.
- `Verify`
  - Only shown for custom deployments.
  - Leave enabled unless the custom deployment uses a self-signed certificate.

### Source Behavior

- A source must have valid Forward credentials.
- If credentials are incomplete, the network list remains empty until username/password are provided.
- SaaS sources always use `https://fwd.app`.
- Syncs always use the source `Network`.
- The source detail page masks the stored password.

## Forward NQE Maps

`Forward NQE Maps` define how a specific NetBox model is populated.

### NQE Map Fields

- `Name`
  - Operator-facing label for the query.
- `NetBox Model`
  - The NetBox object type the map populates.
- `Query ID`
  - Optional published Forward query reference.
  - Use this or `Query`, not both.
- `Query`
  - Optional raw NQE text.
  - Use this or `Query ID`, not both.
- `Commit ID`
  - Optional published query revision to pin when `Query ID` is used.
- `Coalesce Fields`
  - Ordered identity key sets used to match existing NetBox rows before create/update.
  - Example: `[["slug"], ["name"]]`.
  - Must be valid for the selected `NetBox Model`.
- `Enabled`
  - Disabled maps are skipped.
- `Weight`
  - Lower values run first when multiple selected maps target different models.

### Execution Modes

Each map must define exactly one of:

- `query_id`
- raw `query`

Use `query_id` when you want the map to call a named or published Forward query. Use raw `query` when you want the exact NQE text stored directly in NetBox.

`query_id` values are resolved from the Forward Org Repository. They are not properties of a `Forward Source`, and they should be tracked in custom `Forward NQE Maps` instead of source configuration.

### Identity Contract Validation

The plugin enforces a strict identity contract:

- Save-time map validation:
  - `coalesce_fields` must be valid for the selected model.
  - Raw `query` maps must include required output fields and coalesce fields.
- Sync-time validation:
  - Rows must include required identity fields.
  - Rows must satisfy at least one configured coalesce field set.
  - Ambiguous coalesce matches fail the sync to prevent duplicate or inconsistent object resolution.

### Built-In Maps

Built-in maps are seeded automatically after migration. They are stored as the shipped source text from this repository and can be used directly or copied into custom maps.

Several built-in queries import the shared [`netbox_utilities.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/netbox_utilities.nqe) module. The plugin flattens those local imports at execution time for bundled built-ins, but the source files remain modular so the same query set can be uploaded into the Forward Org Repository and tested by `query_id`/`commit_id`.

Manufacturer-bearing built-in maps canonicalize vendor names and slugs in NQE. If your NetBox already uses different curated manufacturer rows, copy the query set and adjust `manufacturer_name_overrides` in `netbox_utilities` before syncing.

Large datasets should prefer saved queries plus `latestProcessed`. That keeps the first run as a full baseline, then lets later runs use Forward `nqe-diffs` directly. The current built-ins also collapse NetBox identities in NQE where the source emits many raw rows for one object, such as prefix, IP, MAC, and VLAN records.

The current built-in map set is:

- `Forward Locations`
- `Forward Device Vendors`
- `Forward Device Types`
- `Forward Platforms`
- `Forward Device Models`
- `Forward Devices`
- `Forward Virtual Chassis`
- `Forward Interfaces`
- `Forward MAC Addresses`
- `Forward VLANs`
- `Forward VRFs`
- `Forward IPv4 Prefixes`
- `Forward IPv6 Prefixes`
- `Forward IP Addresses`
- `Forward Inventory Items`

See the [Built-In NQE Reference](../02_Reference/built-in-nqe-maps.md) for the exact shipped query text and expected output fields.
See the [Model Mapping Matrix](../02_Reference/model-mapping-matrix.md) for the current exact vs best-fit mapping semantics per NetBox model.

The repository query files are also linked directly under [`forward_netbox/queries`](https://github.com/forwardnetworks/forward-netbox/tree/main/forward_netbox/queries).

### Org Repository Workflow

If you want Forward to own the modular source instead of storing raw NQE inside NetBox:

1. Upload the query set, including `netbox_utilities`, into an Org Repository folder.
2. Commit the folder in Forward.
3. Use the resulting `query_id` and optional `commit_id` in a custom `Forward NQE Map`.
4. Validate against a known snapshot before enabling the map in production syncs.

### Large Dataset Recommendation

For large datasets, prefer Org Repository-backed `query_id` maps over bundled raw `query` maps.

- Keep the query source in Forward by committing the modular query set into the Org Repository.
- Point custom `Forward NQE Maps` at the committed `query_id` values.
- Leave the sync `Snapshot` at `latestProcessed`.
- Run and merge one clean baseline ingestion first.
- After that merged baseline exists, later `latestProcessed` runs can use Forward `nqe-diffs` for eligible `query_id` maps instead of rerunning every model as a full snapshot sync.

This keeps NQE as the source of truth, lets Forward own the row-diff computation, and is the recommended operating mode for larger inventories.

For very large inventories, expect the first full baseline to remain the slow path even after query optimization because NetBox must still materialize every staged object change. The largest steady-state win comes from switching later `latestProcessed` runs onto Forward `nqe-diffs`.

## Forward Syncs

Create a `Forward Sync` to bind a source, a NetBox model selection, and the branch-backed ingestion workflow.

### Sync Fields

- `Source`
  - The Forward source the sync runs against.
- `Snapshot`
  - Select a specific snapshot from the source network, or leave the default `latestProcessed` option.
  - The selected snapshot is what NQE runs against.
  - `latestProcessed` resolves at runtime to the network's latest processed snapshot.
- Model toggles
  - Enable or disable individual NetBox models for the sync.
  - The checked models define what this sync runs.
- `Auto merge`
  - Automatically merges the staged branch after a successful sync.
- `Schedule at` / `Recurs every`
  - Optional scheduled execution controls.

### Sync Execution Behavior

- Syncs always use the `Network` selected on the source.
- Syncs run NQE against the selected `Snapshot`.
- The default snapshot selector is `latestProcessed`, which resolves to the latest processed snapshot in the source network at runtime.
- Each ingestion records both the selected snapshot mode and the resolved snapshot ID used for the run.
- Snapshot metrics returned by Forward are stored on the ingestion for later review.
- `NQE Maps` are managed globally under `Plugins > Forward Networks > NQE Maps`.
- For each enabled NetBox model:
  - enabled custom maps for that model take precedence
  - otherwise enabled built-in maps for that model are used

### Recommended Starting Point

For the first validation:

1. Create a `Forward Source`.
2. Set the source `Network`.
3. Leave `Snapshot` at `latestProcessed` for the first validation.
4. Keep the default model selection enabled.
5. Run an adhoc ingestion from the sync detail page.

For large datasets, treat that first run as the baseline establishment step: merge it before expecting later `latestProcessed` ingestions to switch to the incremental `nqe-diffs` path.
