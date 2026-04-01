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
  - On edit, leave this blank to preserve the stored password.
- `Network`
  - Optional default Forward network for syncs that use this source.
  - The field is populated dynamically from the authenticated tenant.
- `Timeout`
  - Forward API timeout in seconds.
- `Verify`
  - Only shown for custom deployments.
  - Leave enabled unless the custom deployment uses a self-signed certificate.

### Source Behavior

- A source must have valid Forward credentials.
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
- `Enabled`
  - Disabled maps are skipped.
- `Weight`
  - Lower values run first when multiple selected maps target different models.

### Execution Modes

Each map must define exactly one of:

- `query_id`
- raw `query`

Use `query_id` when you want the map to call a named or published Forward query. Use raw `query` when you want the exact NQE text stored directly in NetBox.

### Built-In Maps

Built-in maps are seeded automatically after migration. They are stored as raw shipped NQE and can be used directly or copied into custom maps.

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

The repository query files are also linked directly under [`forward_netbox/queries`](https://github.com/forwardnetworks/forward-netbox/tree/main/forward_netbox/queries).

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
