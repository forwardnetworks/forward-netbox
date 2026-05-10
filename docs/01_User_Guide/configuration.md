# Configuration

The plugin is configured through three primary objects:

- `Forward Source`
- `Forward NQE Map`
- `Forward Sync`

Optional plugin-wide flags live in NetBox `configuration.py`:

```python
PLUGINS_CONFIG = {
    "forward_netbox": {
        "enable_bgp_sync": False,
    }
}
```

`enable_bgp_sync` defaults to `False`. Set it to `True` only when testing the beta BGP path with the optional `netbox-routing` and, if desired, `netbox-peering-manager` plugins installed and migrated.

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
  - Defaults to `1200` (20 minutes), aligned to Forward's default NQE
    compute timeout for the public API path.
- `Retries`
  - Forward API retry count for transient disconnects, timeouts, and transient
    HTTP responses (`408`, `429`, `502`, `503`, `504`).
  - Defaults to `2`; valid source parameters are clamped to `0..5`.
  - Retries do not mask NQE row validation failures or non-transient HTTP error
    responses.
- `NQE Page Size`
  - Rows requested per `/api/nqe` page.
  - Defaults to `10000`; valid range is `1..10000`.
  - This controls request paging (`queryOptions.offset/limit`) only. It does not change query semantics.
- `Verify`
  - Only shown for custom deployments.
  - Leave enabled unless the custom deployment uses a self-signed certificate.
- Outbound proxy
  - Forward API calls use NetBox's native outbound proxy routing.
  - Set `HTTP_PROXIES` or `PROXY_ROUTERS` in NetBox `configuration.py`; do not configure a separate Forward plugin proxy.

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
- `Enabled`
  - Disabled maps are skipped.
- `Weight`
  - Lower values run first when multiple selected maps target different models.

### Execution Modes

Each map must define exactly one of:

- repository `query_path`
- `query_id`
- raw `query`

Use repository `query_path` when you want the map to call a committed Forward
query by path. This is the preferred mode because it survives different Forward
orgs: the plugin resolves the org-specific query ID from the selected source at
sync time. Use direct `query_id` only when the map is tied to one Forward org.
Use raw `query` when you want the exact NQE text stored directly in NetBox.

`query_path` and direct `query_id` values are map properties, not source
configuration. The `Forward Source` supplies credentials and org context for
selector lookup and runtime path resolution.

When editing an NQE map in the UI, choose `Repository Query Path`, `Direct Query
ID`, or `Raw Query Text` under `Query Definition Mode`.

For repository-path queries, the UI uses `Forward Source for Query Lookup` to
populate selectors. Pick the query repository, then a folder, then the query
path. The plugin detects Org Repository queries and Forward Library queries
from the selected source. After a query is selected, the `Commit ID` selector
can pin a specific committed revision; leave it blank to resolve the latest
committed revision at sync time.

For raw queries, paste the NQE in `Query`. The form clears `query_id` and
`commit_id` before saving so each map has exactly one execution mode.

### Identity Contract Validation

The plugin enforces a strict identity contract:

- Save-time map validation:
  - identity keys default from the selected NetBox model.
  - advanced `coalesce_fields` values, when present from existing data or API input, must be valid for the selected model.
  - Raw `query` maps must include required output fields and coalesce fields.
- Sync-time validation:
  - Rows must include required identity fields.
  - Rows must satisfy at least one configured coalesce field set.
  - Ambiguous coalesce matches fail the sync to prevent duplicate or inconsistent object resolution.

`coalesce_fields` are intentionally not exposed as a normal form field. They remain visible on map detail/API responses for troubleshooting, but day-to-day custom maps should rely on the model defaults unless a maintainer intentionally changes the advanced identity contract.

### Built-In Maps

Built-in maps are seeded automatically after migration. They are stored as the shipped source text from this repository and can be used directly or copied into custom maps.

Several built-in queries import the shared [`netbox_utilities.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/netbox_utilities.nqe) module. The plugin flattens those local imports at execution time for bundled built-ins, but the source files remain modular so the same query set can be uploaded into the Forward Org Repository and tested by `query_id`/`commit_id`.

Manufacturer-bearing built-in maps canonicalize vendor names and slugs in NQE. If your NetBox already uses different curated manufacturer rows with the default maps, copy the query set and adjust `manufacturer_name_overrides` in `netbox_utilities` before syncing.

The plugin seeds two query families for device type matching:

- Default maps that do not require a Forward data file.
- Disabled alias-aware variants for `Forward Device Models` and `Forward Devices`.

The alias-aware variants require a Forward JSON data file named `netbox_device_type_aliases.json` with NQE name `netbox_device_type_aliases`. That file can carry both Device Type Library aliases and manufacturer override rows, so alias-aware customizations stay data-driven instead of embedded in query code. Upload and attach the data file, then run or reprocess a Forward snapshot before enabling the alias-aware maps for plugin syncs. The plugin executes public `/api/nqe` against the selected snapshot and cannot force Forward's latest-data-file mode. Leave the default non-data-file maps enabled unless the selected snapshot exposes the data file value. See [Device Type Alias Data File](../02_Reference/device-type-alias-data-file.md).

The plugin also seeds a default `Forward Device Feature Tags` map and a disabled `Forward Device Feature Tags with Rules` variant. The default map requires no data file and tags BGP-enabled devices as `Prot_BGP` from Forward's structured protocol state. The rules-aware variant requires a Forward JSON data file named `netbox_feature_tag_rules.json` with NQE name `netbox_feature_tag_rules`; use it when operators need to rename tags, change colors, or apply multiple tags from the same structured feature. See [Feature Tag Rules Data File](../02_Reference/feature-tag-rules-data-file.md).

Optional routing sync is behind `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = True`. When enabled and the optional NetBox plugins are installed, the sync form exposes disabled-by-default maps for `netbox_routing.bgppeer`, `netbox_routing.bgpaddressfamily`, `netbox_routing.bgppeeraddressfamily`, `netbox_routing.ospfinstance`, `netbox_routing.ospfarea`, `netbox_routing.ospfinterface`, and `netbox_peering_manager.peeringsession`. The `netbox-routing` maps are the primary native BGP/OSPF targets; the `netbox-peering-manager` map creates an overlay session linked to the BGP peer. Leave this flag off unless those optional plugins are installed and the routing beta path is intentionally being tested. The routing queries use explicit local identity when Forward provides it, then apply conservative native-query inference from reciprocal Forward peer evidence. During planning, routing diagnostics report BGP neighbors skipped because no explicit or safely inferred local AS exists, unsupported BGP address families, and OSPF rows skipped because no unique process-level local router ID can be inferred safely.

For large routing datasets, publish the routing NQE into the Forward NQE library and bind each enabled NetBox map to the repository query path. The first run still performs a full baseline. Later `latestProcessed` runs can use Forward NQE diffs only when all enabled maps for that model are backed by a repository path or direct query ID; inline query text falls back to full execution.

Large datasets should prefer saved queries plus `latestProcessed`. That keeps the first run as a full baseline, then lets later runs use Forward `nqe-diffs` directly. The current built-ins also collapse NetBox identities in NQE where the source emits many raw rows for one object, such as prefix, IP, MAC, and VLAN records.

The current built-in map set is:

- `Forward Locations`
- `Forward Device Vendors`
- `Forward Device Types`
- `Forward Platforms`
- `Forward Device Models`
- `Forward Devices`
- `Forward Virtual Chassis`
- `Forward Device Feature Tags`
- `Forward Interfaces`
- `Forward Inferred Interface Cables`
- `Forward MAC Addresses`
- `Forward VLANs`
- `Forward VRFs`
- `Forward IPv4 Prefixes`
- `Forward IPv6 Prefixes`
- `Forward IP Addresses`
- `Forward Inventory Items`

The disabled optional map set also includes `Forward Modules`, `Forward BGP Peers`, `Forward BGP Address Families`, `Forward BGP Peer Address Families`, `Forward OSPF Instances`, `Forward OSPF Areas`, `Forward OSPF Interfaces`, and `Forward Peering Sessions` when their target ContentTypes exist.

See the [Built-In NQE Reference](../02_Reference/built-in-nqe-maps.md) for the exact shipped query text and expected output fields.
See the [Model Mapping Matrix](../02_Reference/model-mapping-matrix.md) for the current exact vs best-fit mapping semantics per NetBox model.

The repository query files are also linked directly under [`forward_netbox/queries`](https://github.com/forwardnetworks/forward-netbox/tree/main/forward_netbox/queries).

### Org Repository Workflow

If you want Forward to own the modular source instead of storing raw NQE inside NetBox:

1. In NetBox, open `Plugins > Forward Networks > NQE Maps`.
2. Select the maps to publish and bind. To publish every visible built-in map,
   use the table header checkbox, then click the native `Edit Selected` bulk
   action.
3. In the bulk edit form, set `Query Bulk Operation` to `Publish bundled
   queries to Org Repository and bind selected maps`.
4. Set `Forward Source for Query Lookup` and select the destination
   `Repository Folder`.
5. Leave `Overwrite existing repository queries` disabled to publish only
   missing files. Enable it when you want the bundled source from this plugin
   release to replace existing files at the same paths.
6. Set a `Commit message`, then apply the edit.
7. Validate against a known snapshot before enabling the map in production syncs.

This publishes the selected maps' bundled NQE source plus required local imports
such as `netbox_utilities` into the selected Forward source's Org Repository,
commits those changes, and binds the selected NetBox maps to the resulting
repository paths. The selected Forward source credentials must have Forward
Network Operator or equivalent NQE-library write permission. Read-only or
query-only credentials can still bind existing repository queries, but cannot
publish or commit new Org Repository content.

If you prefer to manage the query files in Forward directly:

1. Upload the query set, including `netbox_utilities`, into an Org Repository folder.
2. Commit the folder in Forward.
3. In NetBox, open `Plugins > Forward Networks > NQE Maps`.
4. Select the maps to bind. To bind every visible map, use the table header
   checkbox, then click the native `Edit Selected` bulk action.
5. In the bulk edit form, set `Query Bulk Operation` to `Bind selected maps to
   repository query paths`.
6. Set `Forward Source for Query Lookup`, confirm `Query Repository`, and select
   the `Repository Folder`.
7. Under `Map Query Choices`, choose the repository query path for each selected
   NetBox map that should be bound. The folder limits the choices, and each
   selector is filtered to the selected map's NetBox model.
8. Apply the edit.
9. Validate against a known snapshot before enabling the map in production syncs.

The native bulk edit workflow applies only to the maps selected in the table.
For each selected map, the operator explicitly chooses the committed query path
to bind. The plugin verifies that the selected query path targets the same
NetBox model as the map before saving it. Matched maps clear direct `query_id`
and raw `query`, and optional `commit_id` is stored only when `Pin current
commit` is selected.

The same native bulk edit form can move selected maps back to bundled raw query
text. Select the maps, choose `Restore bundled raw query text`, and apply the
edit. The plugin restores the shipped NQE source and clears `query_id`,
`query_path`, `query_repository`, and `commit_id` for maps it can identify
unambiguously. Custom or ambiguous maps are skipped and reported instead of
being guessed.

This workflow intentionally does not store static query IDs on every map.
Repository paths are portable across Forward orgs; the plugin resolves each
path to the correct query ID from the selected `Forward Source` during sync and
diff execution.

For a single map, the equivalent API shape is:

```bash
curl -X PATCH \
  -H "Authorization: Bearer $NETBOX_TOKEN" \
  -H "Content-Type: application/json" \
  https://netbox.example.com/api/plugins/forward/nqe-map/123/ \
  --data '{"query_repository":"org","query_path":"/forward_netbox_validation/forward_ip_addresses","query_id":"","query":"","commit_id":""}'
```

Use `Token $NETBOX_TOKEN` instead of `Bearer $NETBOX_TOKEN` only for legacy
NetBox v1 API tokens.

### Initial Baseline Strategy

Choose the initial sync path before the first production run. The choice is not
about NQE shape: both paths use the same enabled NQE maps, preflight validation,
drift policy, coalesce contracts, row adapters, ingestion issues, and model
results. The choice is about whether the first NetBox writes must be reviewed in
native Branching branches.

| Situation | Execution backend | What happens | What to do after it succeeds |
| --- | --- | --- | --- |
| Small or reviewable baseline | `Branching` | Stages the baseline in native NetBox Branching shards using `Max changes per branch`. | Review or auto-merge the shards, then keep using `Branching` for steady-state diffs. |
| Large but still reviewable baseline | `Branching` with `Auto merge` | Creates and merges one bounded Branching shard at a time. | Let the final successful shard become the diff baseline. |
| Very large trusted baseline that is impractical to review shard-by-shard | `Fast bootstrap` | Runs validation first, then writes directly to NetBox without creating Branching branches. | Inspect validation, ingestion issues, model results, and NetBox state; then switch the sync back to `Branching` for reviewable steady-state diffs. |

`Fast bootstrap` is intended only for trusted initial baseline loads. It does
not provide a Branching diff for review, and `Auto merge` / `Max changes per
branch` do not apply to that backend. The fast path still runs inside NetBox
change tracking: the branchless ingestion stores the NetBox request id used for
direct writes, its statistics are derived from native `ObjectChange` rows, and
the ingestion `Changes` tab shows those direct NetBox changes instead of branch
diffs.

For large datasets, prefer Org Repository-backed `query_path` maps over bundled raw `query` maps.

- Keep the query source in Forward by committing the modular query set into the Org Repository.
- Bulk bind `Forward NQE Maps` to the committed repository query paths.
- Leave the sync `Snapshot` at `latestProcessed`.
- Run one clean baseline ingestion first, either by merging the Branching baseline or completing a trusted fast-bootstrap baseline.
- After that baseline exists, switch or keep the sync on `Branching`; later `latestProcessed` runs can use Forward `nqe-diffs` for eligible repository-path or direct-query-ID maps instead of rerunning every model as a full snapshot sync.

This keeps NQE as the source of truth, lets Forward own the row-diff computation, and is the recommended operating mode for larger inventories.

For very large inventories, expect the first full baseline to remain the slow path even after query optimization because NetBox must still materialize every staged object change. The largest steady-state win comes from switching later `latestProcessed` runs onto Forward `nqe-diffs`.

NetBox Branching guidance favors smaller review branches. The default backend uses native multi-branch execution. If a full baseline would stage tens or hundreds of thousands of changes, the planner splits large model workloads into ordinary NetBox Branching branches using stable shard keys, with device-scoped models grouped by device.

### Runtime Sizing For Large Syncs

The Forward source `Timeout` controls individual Forward API/NQE calls. In
current Forward builds, the public NQE API path has a default query-compute
timeout of 20 minutes; the web response wrapper can wait longer, but it does not
make a single query compute indefinitely. NetBox worker timeout controls how
long the NetBox background job is allowed to run. For large baselines, size
both:

- Set NetBox `RQ_DEFAULT_TIMEOUT` higher than the Forward source `Timeout`.
- Use a long enough `RQ_DEFAULT_TIMEOUT` for initial Branching baselines and
  merge jobs; the default NetBox value can be too short for large imports.
- Keep `Max changes per branch` at or below the operator's Branching guidance.
- Prefer `Fast bootstrap` only for trusted first baselines that are impractical
  to review as branches, then switch back to `Branching`.
- Ensure NetBox workers and Postgres have enough capacity for the selected
  concurrency and the number of simultaneous syncs.

The plugin logs a non-blocking warning when it can see that
`RQ_DEFAULT_TIMEOUT` is shorter than the Forward source timeout, or when a large
Branching plan is being run with a short worker timeout. These warnings do not
change sync behavior; they are intended to make timeout failures easier to
avoid before the run has been waiting for a long time.

For command-line validation, run the smoke sync with `--plan-only` first:

```bash
invoke forward_netbox.smoke-sync --plan-only --max-changes-per-branch 10000
```

If the plan is acceptable, run the same sync. Each shard is staged in a native Branching branch and merged before the next shard runs. Only the final successful shard becomes the incremental diff baseline, so later `latestProcessed` runs can use Forward `nqe-diffs`.

```bash
invoke forward_netbox.smoke-sync --max-changes-per-branch 10000
```

To review each shard before continuing, disable `Auto merge` in the UI or run:

```bash
invoke forward_netbox.smoke-sync --no-auto-merge --max-changes-per-branch 10000
```

This stages one shard and leaves the sync at `Ready to merge`. After you review and merge that shard, click `Continue Ingestion` to stage the next shard using the same resolved Forward snapshot.

If the planner reports that one shard key exceeds the branch budget, reduce that source model with a narrower NQE map or split the source data so the largest device or coalesce-key group fits under the operational branch size.

For initial imports that are too large to review shard-by-shard, select `Fast bootstrap` as the sync execution backend. Use it for the baseline load, inspect the resulting validation and ingestion records, then edit the sync back to `Branching` before steady-state runs.

## Forward Syncs

Create a `Forward Sync` to bind a source, a NetBox model selection, and the ingestion workflow.

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
- `Execution backend`
  - `Branching` is the default and stages changes in native NetBox Branching shards.
  - `Fast bootstrap` writes directly to NetBox after validation and is intended for very large trusted initial baselines.
  - Fast bootstrap does not provide a Branching diff for review; use validation runs, ingestion issues, and model results as the review surface.
- `Max changes per branch`
  - Defaults to `10000`.
  - Keep this aligned with local NetBox Branching operational guidance.
  - Applies to the `Branching` backend only.
- `Auto merge`
  - Enabled by default.
  - When enabled, each native Branching shard is merged automatically before the next shard runs.
  - When disabled, the sync stages one shard, pauses for review, and continues only after the user merges that shard and clicks `Continue Ingestion`.
  - Only the final successful shard is marked as the incremental diff baseline.
  - Applies to the `Branching` backend only.
- `Schedule at` / `Recurs every`
  - Optional scheduled execution controls.

### Sync Execution Behavior

- Syncs always use the `Network` selected on the source.
- Syncs run NQE against the selected `Snapshot`.
- The default snapshot selector is `latestProcessed`, which resolves to the latest processed snapshot in the source network at runtime.
- `Branching` syncs use native multi-branch execution.
- Each Branching shard is a native NetBox Branching branch.
- `Auto merge` controls whether Branching shards advance automatically or pause for review after each shard.
- `Fast bootstrap` syncs create a branchless ingestion and write rows directly through the same NetBox adapters after validation.
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
