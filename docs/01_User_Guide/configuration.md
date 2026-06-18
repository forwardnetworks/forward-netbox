# Configuration

The plugin is configured through three primary objects:

- `Forward Source`
- `Forward NQE Map`
- `Forward Sync`

Optional plugin-wide flags live in NetBox `configuration.py`:

```python
PLUGINS_CONFIG = {
    "forward_netbox": {
        "enable_bgp_sync": True,
    }
}
```

`enable_bgp_sync` defaults to `True`. Set it to `False` only if you want to hide the beta module/routing surface from the sync UI.
Optional plugin capability reporting is shared across routing, peering, and
Cisco ACI. The architecture audit, sync health page, and support bundle all
surface the same installed/available/version status so operators can see why a
plugin surface is unavailable without opening the database. Optional maps are
still disabled by default unless the target plugin ContentTypes are installed.

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
- `Query Fetch Concurrency`
  - Maximum concurrent NQE map fetch jobs during preflight/workload fetch.
  - Defaults to `6`; valid range is `1..16`.
  - Increase gradually only when NetBox worker and database telemetry show headroom.
- `Query Preflight`
  - Runs the sample preflight query phase before full workload fetch.
  - Defaults to enabled.
  - Disable on very large runs when you need faster startup and can accept that
    query issues are first surfaced during workload fetch instead of preflight.
- `Query Diagnostics`
  - Runs additional diagnostics queries for importability summaries (IP/routing diagnostics).
  - Defaults to enabled.
  - Disable on very large runs to reduce query overhead during ingestion.
- `Async NQE`
  - Uses Forward's async NQE execution API when the source is pointed at Forward 26.6 or newer.
  - `nqe_async_poll_interval_seconds` defaults to `1.0`; `nqe_async_max_polls` defaults to `1200`.
  - Async mode only runs when the sync has a network, a processed snapshot, and a JSON response format.
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

### Large-Ingestion Tuning Baseline

For large first-time imports, start with a conservative, NetBox-native baseline:

- `timeout`: `1200`
- `nqe_page_size`: `10000`
- `query_fetch_concurrency`: default `10` for Branching; `Fast bootstrap` uses a higher default when unset (bounded by plugin max). Typical tuning range `6` to `12`; increase only when workers and Postgres have headroom.
- `api_requests_per_minute`: Forward SaaS sources default to `1800` requests/minute to stay below the SaaS hard-block threshold of `2000` requests/minute per user. Exceeding the SaaS threshold can return HTTP `429 Too Many Requests` and block the user account for 5 minutes. Custom/on-prem sources default to `0` (disabled); on-prem rollout is planned for a future release.
- `nqe_fetch_all_max_pages`: default `5000` (hard stop for runaway paginated NQE fetches where Forward keeps returning full pages)
- `nqe_identical_full_page_streak_limit`: default `25` (fails fast when fetch-all receives repeated identical full pages with no observed pagination progress)
- `query_preflight_enabled`: `true` for safer rollout, `false` for faster large-run startup
- `query_preflight_row_limit`: `5` by default (lower values reduce startup sampling cost; higher values increase preflight coverage at higher query cost)
- `query_diagnostics_enabled`: `true` for richer diagnostics, `false` for faster large-run execution
- `max_changes_per_branch`: keep near your Branching guidance (typically `10000`)

Recommended workflow:

1. Run first baseline in `Fast bootstrap` when the dataset is very large and trusted.
2. Switch to `Branching` for steady-state runs with review/merge controls.
3. Use repository `query_path` or direct `query_id` for maps that need Forward API diff execution on later runs.
4. Avoid inline raw `query` for long-term diff-based operations because raw text maps fall back to full model fetch.

Capacity notes:

- Raising `query_fetch_concurrency` helps preflight/query fetch only; it does not remove Branching merge serialization.
- Lower `api_requests_per_minute` before increasing concurrency when the same Forward user is also used by other test jobs or integrations. The cap is a SaaS protection guardrail, not a capacity SLA.
- Increasing `max_changes_per_branch` can reduce shard count but make each shard/merge slower and riskier.
- Keep one source of truth for row shaping in NQE; avoid Python-side normalization for performance tuning.

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

Several built-in queries import the shared [`netbox_utilities.nqe`](https://github.com/forwardnetworks/forward-netbox/blob/main/forward_netbox/queries/netbox_utilities.nqe) module. The plugin flattens those local imports at execution time for bundled built-ins and when publishing built-ins into the Forward Org Repository, so saved `query_id`/`commit_id` execution does not depend on local module import resolution.

Manufacturer-bearing built-in maps canonicalize vendor names and slugs in NQE. If your NetBox already uses different curated manufacturer rows with the default maps, copy the query set and adjust `manufacturer_name_overrides` in `netbox_utilities` before syncing.

The plugin seeds two query families for device type matching:

- Default maps that do not require a Forward data file.
- Disabled alias-aware variants for `Forward Device Models` and `Forward Devices`.

The alias-aware variants require a Forward JSON data file named `netbox_device_type_aliases.json` with NQE name `netbox_device_type_aliases`. That file can carry both Device Type Library aliases and manufacturer override rows, so alias-aware customizations stay data-driven instead of embedded in query code. Upload and attach the data file, then run or reprocess a Forward snapshot before enabling the alias-aware maps for plugin syncs. The plugin executes public `/api/nqe` against the selected snapshot and cannot force Forward's latest-data-file mode. Leave the default non-data-file maps enabled unless the selected snapshot exposes the data file value. See [Device Type Alias Data File](../02_Reference/device-type-alias-data-file.md).

The plugin also seeds a default `Forward Device Feature Tags` map and a disabled `Forward Device Feature Tags with Rules` variant. The default map requires no data file and tags BGP-enabled devices as `Prot_BGP` from Forward's structured protocol state. The rules-aware variant requires a Forward JSON data file named `netbox_feature_tag_rules.json` with NQE name `netbox_feature_tag_rules`; use it when operators need to rename tags, change colors, or apply multiple tags from the same structured feature. See [Feature Tag Rules Data File](../02_Reference/feature-tag-rules-data-file.md).

Optional routing sync is enabled by default through `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = True`. When the optional NetBox plugins are installed, the sync form exposes the beta `netbox_routing.bgppeer`, `netbox_routing.bgpaddressfamily`, `netbox_routing.bgppeeraddressfamily`, `netbox_routing.ospfinstance`, `netbox_routing.ospfarea`, `netbox_routing.ospfinterface`, and `netbox_peering_manager.peeringsession` maps. The `netbox-routing` maps are the primary native BGP/OSPF targets; the `netbox-peering-manager` map creates an overlay session linked to the BGP peer. Set the flag to `False` only if you want to hide that beta surface. The routing queries use explicit local identity when Forward provides it, then apply conservative native-query inference from reciprocal Forward peer evidence. During planning, routing diagnostics report BGP neighbors skipped because no explicit or safely inferred local AS exists, unsupported BGP address families, and OSPF rows skipped because no unique process-level local router ID can be inferred safely.

The optional-plugin framework is shared with Cisco ACI as well. The generic
capability report treats `netbox-routing`, `netbox-peering-manager`, and
`netbox-cisco-aci` as first-class optional integrations, and the same
reporting path appears in the architecture audit, sync health view, and support
bundle exports.

For large routing datasets, publish the routing NQE into the Forward NQE library and bind each enabled NetBox map to the repository query path. The first run still performs a full baseline. Later `latestProcessed` runs can use Forward NQE diffs only when all enabled maps for that model are backed by a repository path or direct query ID; inline query text falls back to full execution.

Optional Cisco ACI sync requires the `netbox-cisco-aci` plugin. When that
plugin is installed and migrated, the map list exposes disabled `Forward ACI
Fabrics`, `Forward ACI Pods`, `Forward ACI Nodes`, `Forward ACI Tenants`,
`Forward ACI APIC Nodes`, `Forward ACI APIC CIMC Inventory`,
`Forward ACI VRFs`, `Forward ACI Bridge Domains`, `Forward ACI Application
Profiles`, `Forward ACI Endpoint Groups`, `Forward ACI Contracts`,
`Forward ACI Filters`, `Forward ACI L3Outs`, and `Forward ACI Static Port
Bindings` maps. Enable them only when you want Forward to create or update the
plugin's ACI inventory and policy objects. The ACI maps use Forward
saved-query/raw-query execution and `forward_netbox_shard_keys`; they do not
use sync-time column filters or per-tenant/per-node Forward API calls. The
fabric/pod/node, tenant/VRF, filter, and APIC CIMC inventory maps parse
selected command output in NQE and emit normalized fields instead of raw command
responses. The APIC CIMC inventory map targets native `dcim.inventoryitem` rows
and requires the APIC custom command `moquery -c eqptCh -a all` to be collected
by Forward. A separate `Forward ACI Command Inventory` discovery map reports
bounded APIC/ACI command family presence without exposing raw payloads. Bridge
domain, application profile, EPG, contract, L3Out, and static binding maps are
seeded disabled as conservative no-op contracts until their bounded parser
identity and repeat-sync behavior are proven.

Large datasets should prefer saved queries plus `latestProcessed`. That keeps the first run as a full baseline, then lets later runs use Forward `nqe-diffs` directly. The current built-ins also collapse NetBox identities in NQE where the source emits many raw rows for one object, such as prefix, IP, MAC, and VLAN records.

The current built-in map set is:

- `Forward Locations`
- `Forward Device Vendors`
- `Forward Device Types`
- `Forward Platforms`
- `Forward Device Models`
- `Forward Devices`
- `Forward Virtual Chassis` (conservative no-op unless customized)
- `Forward Device Feature Tags`
- `Forward Interfaces`
- `Forward Inferred Interface Cables`
- `Forward MAC Addresses`
- `Forward VLANs`
- `Forward VRFs`
- `Forward IPv4 Prefixes`
- `Forward IPv6 Prefixes`
- `Forward IPv4 IP Addresses`
- `Forward IPv6 IP Addresses`
- `Forward HSRP Groups` (optional `ipam.fhrpgroup`)
- `Forward Inventory Items`
- `Forward ACI Fabrics` (optional `netbox_cisco_aci.acifabric`)
- `Forward ACI Pods` (optional `netbox_cisco_aci.acipod`)
- `Forward ACI Nodes` (optional `netbox_cisco_aci.acinode`)
- `Forward ACI APIC Nodes` (optional `netbox_cisco_aci.acinode`)
- `Forward ACI APIC CIMC Inventory` (native `dcim.inventoryitem`)
- `Forward ACI Tenants` (optional `netbox_cisco_aci.acitenant`)
- `Forward ACI VRFs` (optional `netbox_cisco_aci.acivrf`)
- `Forward ACI Bridge Domains` (optional `netbox_cisco_aci.acibridgedomain`)
- `Forward ACI Application Profiles` (optional `netbox_cisco_aci.aciappprofile`)
- `Forward ACI Endpoint Groups` (optional `netbox_cisco_aci.aciendpointgroup`)
- `Forward ACI Contracts` (optional `netbox_cisco_aci.acicontract`)
- `Forward ACI Filters` (optional `netbox_cisco_aci.acifilter`)
- `Forward ACI L3Outs` (optional `netbox_cisco_aci.acil3out`)
- `Forward ACI Static Port Bindings` (optional `netbox_cisco_aci.acistaticportbinding`)

`Forward HSRP Groups` is optional and disabled unless `ipam.fhrpgroup` is selected
for a sync. It imports Forward native HSRP and VRRP group state into NetBox
native FHRP objects with one paged NQE result set. It does not perform
per-device, per-interface, or per-group Forward API calls. IPv4 VRRP rows map to
NetBox `vrrp2`, IPv6 VRRP rows map to NetBox `vrrp3`, and VIP IP addresses use
the native `vrrp` role. Existing NetBox IP addresses that are assigned to
another object are treated as conflicts and skipped instead of being reassigned.

The optional beta map set also includes `Forward Modules`, `Forward BGP Peers`, `Forward BGP Address Families`, `Forward BGP Peer Address Families`, `Forward OSPF Instances`, `Forward OSPF Areas`, `Forward OSPF Interfaces`, `Forward Peering Sessions`, and the optional Forward ACI map family when their target ContentTypes exist.

See the [Built-In NQE Reference](../02_Reference/built-in-nqe-maps.md) for the exact shipped query text and expected output fields.
See the [Model Mapping Matrix](../02_Reference/model-mapping-matrix.md) for the current exact vs best-fit mapping semantics per NetBox model.

The repository query files are also linked directly under [`forward_netbox/queries`](https://github.com/forwardnetworks/forward-netbox/tree/main/forward_netbox/queries).

### Org Repository Workflow

If you want Forward to own the modular source instead of storing raw NQE inside NetBox:

1. In NetBox, open `Plugins > Forward > NQE Maps`.
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

This publishes the selected maps' bundled NQE source as executable flattened
queries into the selected Forward source's Org Repository, commits those
changes, and binds the selected NetBox maps to the resulting repository paths.
The selected Forward source credentials must have Forward Network Operator or
equivalent NQE-library write permission. Read-only or query-only credentials can
still bind existing repository queries, but cannot publish or commit new Org
Repository content.

If you prefer to manage the query files in Forward directly:

1. Upload the query set, including `netbox_utilities`, into an Org Repository folder.
2. Commit the folder in Forward.
3. In NetBox, open `Plugins > Forward > NQE Maps`.
4. Select the maps to bind. To bind every visible map, use the table header
   checkbox, then click the native `Edit Selected` bulk action.
5. In the bulk edit form, set `Query Bulk Operation` to `Use repository query
   paths (query IDs resolve at sync time)`.
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

This workflow intentionally does not store static query IDs on every map, so
there is no direct query-ID selector in the native bulk edit form. Repository
paths are portable across Forward orgs; the plugin resolves each path to the
correct query ID from the selected `Forward Source` during sync and diff
execution.

Use `invoke validation-org-query-audit` when you want to verify that the
bundled query set is still published in the validation org repository folder
and that the committed source still matches the repo copy.

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

The sync detail page shows an initial-baseline lane advisory when planning or
execution evidence is available. Its `initial_baseline_lane` workload field
reports the current backend, recommended backend, planned shard count,
estimated changes, runtime class, delete-heavy models, risk level, and any
Fast bootstrap confirmation text. Treat this as advisory evidence: use
`Fast bootstrap` only for a trusted first baseline, and use `Branching` when
reviewable diffs are required or the projected shard count is bounded.

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

Use these profiles as starting points, then tune from Sync Health and support
bundle evidence rather than from row count alone:

| Profile | Typical use | Backend guidance | Starting knobs | Watch first |
| --- | --- | --- | --- | --- |
| Small | Low-change validation or narrow model syncs | `Branching` | `query_fetch_concurrency=2-4`, default page size, ordinary worker timeout | Query/map validation failures |
| Medium | Reviewable baselines or steady-state diffs with moderate shard counts | `Branching` with repository-backed `query_path` or `query_id` maps | `query_fetch_concurrency=4-8`, `nqe_page_size=5000-10000`, worker timeout above source timeout | Diff eligibility, fallback rate, shard duration |
| Large | High-volume baselines that still need Branching review | `Branching` with native multi-branch shards; `Auto merge` only when review policy allows | `query_fetch_concurrency=6-12`, `nqe_page_size=10000`, long worker timeout, dedicated worker capacity | Branch budget pressure, merge wait, Postgres headroom |
| Very large trusted baseline | First load is too large to review shard-by-shard | `Fast bootstrap` for the first trusted baseline, then switch back to `Branching` for diff runs | `query_fetch_concurrency=8-16`, `nqe_page_size=10000`, long worker timeout, Postgres tuned for bulk object changes | Validation issues, object-change statistics, first Branching diff after baseline |

Treat these as capacity profiles, not correctness modes. NQE remains the source
of truth, row validation stays shared, and the execution backend only changes
whether NetBox stages review branches or writes a trusted first baseline
directly.

For local Docker performance runs, use the built-in runtime optimizer before
running smoke or scale tests:

```bash
invoke forward_netbox.optimize-runtime --worker-replicas 0 --query-fetch-concurrency 16 --nqe-page-size 10000 --apply-postgres
```

What this does:

- auto-scales `netbox-worker` replicas from host CPU count (or use an explicit
  value with `--worker-replicas`)
- applies practical Postgres `ALTER SYSTEM` defaults for larger ingestion runs
  (`shared_buffers`, `effective_cache_size`, `work_mem`, WAL/checkpoint
  settings, parallel worker settings)
- optionally updates one Forward source's `query_fetch_concurrency` and
  `nqe_page_size` when `--source-name <name>` is supplied

Recommended rollout for large baselines:

1. Run `--plan-only` first and confirm shard counts are operationally acceptable.
2. Start with `query_fetch_concurrency=6` if your DB is shared, then raise
   toward `16` only when DB and worker telemetry show headroom.
3. Keep `Max changes per branch` near operator guidance (commonly 10k).
4. For trusted very large first loads, use `Fast bootstrap`, then switch back to
   `Branching` for steady-state diff runs.
5. Use Sync Health `Large Run Tuning` before reruns. Its `Backend advice`
   field separates timeout-risk baselines that may need Fast bootstrap from
   steady-state Branching runs where diff/pushdown fixes should come first.

Shard-scoped Branching retries can reuse temporary run-local fetch artifacts
instead of repeating the same Forward query. This is an internal retry/resume
optimization; NQE and the selected snapshot remain the source of truth, and
support bundles include only artifact status/count/size metadata. Optional
environment variables:

When a scoped shard fetch falls back to full model execution, later shards in
the same run can reuse a run-local full-model artifact and then apply the normal
local shard filter. This avoids repeatedly running the same full NQE fallback
without changing row shape, coalesce behavior, or NetBox write semantics.

- `FORWARD_NETBOX_FETCH_ARTIFACT_DIR`: runtime artifact directory. Defaults to
  the system temporary directory under `forward_netbox_fetch_artifacts`.
- `FORWARD_NETBOX_FETCH_ARTIFACT_TTL_SECONDS`: artifact TTL. Defaults to
  `86400`.
- `FORWARD_NETBOX_FETCH_ARTIFACT_MAX_BYTES`: maximum serialized artifact size.
  Defaults to `52428800`.

For large local performance tests, point `FORWARD_NETBOX_FETCH_ARTIFACT_DIR` at
fast local storage. Do not point it at durable shared storage; artifacts contain
temporary row payloads and are pruned when runs complete or fail through the
current ledger paths.

The repository's local Docker compose setup mounts a scratch artifact directory
into NetBox and workers automatically. Set
`FORWARD_NETBOX_FETCH_ARTIFACT_HOST_PATH` in `development/.env` when the host has
a dedicated fast scratch device. The compose default artifact size cap is
`536870912` bytes so large local fallback artifacts can be reused during scale
tests without changing production defaults.

Live smoke syncs created through `invoke smoke-sync` also enable the safe
bulk-ORM set by default. Pass `--enable-bulk-orm=False` when you intentionally
need adapter-only comparison evidence.

Filtered syncs that prune out-of-scope rows can create large delete waves. The
Branching plan preview includes a `delete_dependency_plan` section with delete
row counts, delete shard counts, dependency-ordered model execution, and warning
codes for delete waves, near-budget delete shards, and dependency-anchor models
that may hit reference blockers. Review this summary before merging destructive
branches, especially after changing device tag filters.

Device tag filters run in local mode by default so existing custom query maps do
not need to accept extra NQE parameters. For bundled site and prefix maps,
newer releases also pass the selected include/exclude tags into the shipped
tag-aware NQE parameters. This prevents sites and prefixes from being collected
from devices outside the selected Forward tag scope. Custom org queries that
declare the same parameters receive the same source-side scope; custom queries
that do not declare them continue to rely on local row filtering.

The device-scope tag (e.g. `Prod_Core`) is a Forward-side selector and is not
written to NetBox by default. Enable `Apply Device Scope Tags`
(`apply_device_scope_tags`) on the source to tag each synced device in NetBox
with its scope include tag(s). This lets you filter the NetBox device list by
scope and visually identify out-of-scope leftovers (which only the
`forward_device_scope_reconciliation_audit` command surfaces otherwise). The tag
is added only when missing, so steady-state re-syncs do not churn. Scope tagging
applies when there is a single include tag or the include match mode is `all`
(every in-scope device then carries every include tag). With multiple include
tags in `any` match mode, tagging is skipped with a warning, because a device may
match only one tag and the device row does not carry its Forward tag names.

Branching runs are staged as resumable NetBox jobs. The initial sync job records
the snapshot, validation result, branch plan, and next shard in the sync state.
Each shard and merge then runs as its own NetBox background job. Newer releases
also mirror that plan into execution run/step records so support exports can
show the full multi-shard run, linked jobs, retry counts, branch names, and
last-error state. If a worker timeout, restart, or transient failure interrupts
a shard, use `Continue Ingestion` to resume from the current plan item instead
of starting over from the first shard. If the interrupted item was already
staged, requeue the merge from the ingestion detail page.

The plugin logs a non-blocking warning when it can see that
`RQ_DEFAULT_TIMEOUT` is shorter than the Forward source timeout, or when a large
Branching plan is being run with a short worker timeout. These warnings do not
change sync behavior; they are intended to make timeout failures easier to
avoid before the run has been waiting for a long time.

For command-line validation, run the smoke sync with `--plan-only` first:

```bash
invoke forward_netbox.smoke-sync --plan-only --max-changes-per-branch 10000
```

If the plan is acceptable, run the same sync. Each shard is staged in a native
Branching branch and merged before the next shard runs. The sync stores the next
plan index so interrupted baselines can resume without discarding completed
shards. Only the final successful shard becomes the incremental diff baseline,
so later `latestProcessed` runs can use Forward `nqe-diffs`.
`max_changes_per_branch` is an operational guideline, not a hard fail cutoff:
small overruns (up to 5%) are accepted and merged, while larger overruns still
trigger automatic shard splitting and retry.
For later runs, high-confidence density learning can pack more source rows into
one shard when prior evidence shows those rows produce fewer actual NetBox
changes. Delete-heavy shards remain conservatively capped.

```bash
invoke forward_netbox.smoke-sync --max-changes-per-branch 10000
```

For local performance proof runs, add `--enable-bulk-orm`. This only changes
the apply engine for the parity-tested model set: `dcim.site`,
`dcim.manufacturer`, `dcim.devicerole`, `dcim.platform`, `dcim.devicetype`,
`ipam.vrf`, and `ipam.vlan`.
Branching review semantics, Fast bootstrap semantics, and the default adapter
path are unchanged.

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
  - `latestCollected` resolves at runtime to the most recent processed snapshot that still has a freshly-collected in-scope device. It scans the most recent processed snapshots newest-first (up to 10) and skips any whose in-scope devices were all backfilled because collection was canceled. Use this when the network's newest snapshot can be a collection-canceled backfill and you do not want to sync stale backfilled data. If the sync has a device tag scope on the source, the scan only considers devices in that scope. If none of the scanned snapshots has a collected in-scope device, the run fails with a clear error instead of silently syncing nothing.
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
- `Use safe bulk ORM models`
  - Enabled by default when the sync has no explicit `enable_bulk_orm` override.
  - Uses the plugin's current parity-tested bulk ORM safe set for eligible models.
  - `Branching` and `Fast bootstrap` both auto-enable the same safe set when unset; set this explicitly to `false` to force adapter-only behavior.
  - Models with dependency, relationship, IPAM hierarchy, or plugin-specific contracts remain on the adapter path even when this is enabled.
  - Current safe set: `dcim.site`, `dcim.manufacturer`, `dcim.devicerole`, `dcim.platform`, `dcim.devicetype`, `dcim.macaddress`, `dcim.virtualchassis`, `ipam.vlan`, `ipam.vrf`, and the two highest-volume models `dcim.interface` and `ipam.ipaddress` (promoted to the safe set with adapter-vs-bulk parity tests and compare-before-write).
- `bulk_orm_models` (advanced parameter)
  - Optional sync parameter list that narrows the safe bulk-ORM model set for this sync.
  - Leave this unset to use the plugin's current parity-tested safe set.
  - Use this only when you explicitly want to restrict bulk ORM to a subset of safe models for troubleshooting or staged rollout.
- `Auto merge`
  - Enabled by default.
  - When enabled, each native Branching shard is merged automatically before the next shard runs.
  - When disabled, the sync stages one shard, pauses for review, and continues only after the user merges that shard and clicks `Continue Ingestion`.
  - Only the final successful shard is marked as the incremental diff baseline.
  - Applies to the `Branching` backend only.
- `Stage next shard during merge`
  - Optional experimental Branching speedup.
  - Requires `Auto merge`.
  - If unset, Branching + Auto merge defaults this on to reduce stage-queue idle time; set it explicitly off to disable overlap.
  - When enabled, the execution ledger may pre-stage one eligible next shard while the current shard is merging.
  - Merges remain serialized. The plugin does not run multiple branch merges at the same time.
  - Leave disabled unless support evidence shows merge/queue wait is the bottleneck and the NetBox workers/database have capacity headroom.
- `Skip scheduled runs on an unchanged snapshot`
  - Optional Forward API load reduction. Off by default.
  - When a scheduled run would target the same snapshot as the last successful baseline ingestion, the plugin skips query execution entirely (a no-op completion) instead of re-fetching unchanged data for every model.
  - Manual/adhoc runs always execute, so you can force a re-sync (for example after editing objects directly in NetBox on the same snapshot).
  - Best for scheduled syncs on a stable snapshot; pairs well with `latestProcessed`/`latestCollected`, which advance the snapshot when new data is collected.
- `Diff fallback mode`
  - `Allow full fallback` (default) keeps runs moving when a diff-eligible map cannot run as a diff and must temporarily fall back to full query execution.
  - `Require diff` enforces diff-only execution once a baseline exists.
  - Use `Require diff` for steady-state speed and strictness when all enabled maps are correctly bound to diff-capable query IDs/paths and you want fast failure instead of full-query fallback.
- `Schedule at` / `Recurs every`
  - Optional scheduled execution controls.

### Sync Execution Behavior

- Syncs always use the `Network` selected on the source.
- Syncs run NQE against the selected `Snapshot`.
- The default snapshot selector is `latestProcessed`, which resolves to the latest processed snapshot in the source network at runtime.
- `latestCollected` is an alternative selector that skips snapshots whose in-scope devices were all backfilled (collection canceled) and resolves to the most recent snapshot that actually collected an in-scope device. Because the resolved snapshot can change between runs, `latestCollected` always runs a full fetch rather than a Forward `nqe-diff`.
- Both dynamic selectors get end-of-run catch-up: if a newer snapshot (newest processed for `latestProcessed`, or newest with a collected in-scope device for `latestCollected`) appears while a run is in progress, the plugin queues a follow-up sync automatically instead of waiting for the next scheduled interval.
- All built-in queries only ingest devices whose snapshot collection `result` is `completed`; backfilled (collection-canceled) devices are intentionally excluded. When a `latestProcessed` run finds that every in-scope device in the snapshot is backfilled, the run logs a warning and applies zero changes — switch the sync to `latestCollected`, pin a known-good snapshot, or re-run collection in Forward.
- `Branching` syncs use native multi-branch execution.
- Each Branching shard is a native NetBox Branching branch.
- `Auto merge` controls whether Branching shards advance automatically or pause for review after each shard.
- `Stage next shard during merge` can reduce scheduler idle time for auto-merged Branching runs by pre-staging one next shard, but it does not reduce native Branching merge cost.
- `Diff fallback mode` controls what happens when a model would otherwise fall back from diff to full query execution:
  - `Allow full fallback` continues with full query execution.
  - `Require diff` fails that model/run path instead of broadening query scope.
- `Fast bootstrap` syncs create a branchless ingestion and write rows directly through the same NetBox adapters after validation.
- Each ingestion records both the selected snapshot mode and the resolved snapshot ID used for the run.
- Snapshot metrics returned by Forward are stored on the ingestion for later review.
- `NQE Maps` are managed globally under `Plugins > Forward > NQE Maps`.
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
