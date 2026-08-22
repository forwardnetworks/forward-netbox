# Device config backup from Forward to a NetBox git data source

## Goal

An opt-in post-sync job that writes every in-scope device's running
configuration - as Forward collected it, snapshot-consistent - into the git
repository behind a NetBox data source, so Validity (and anything else that
reads config backups from a data source) has configs without touching a single
device.

## Why

The operator is standing up Validity for golden-config checks. Validity's
native answer for backups is pollers - per-device SSH with stored credentials.
Forward already holds every config, collected per snapshot, for the exact
device set this plugin syncs. Reusing it means no device credentials in NetBox,
no polling load, and configs that are consistent with a NAMED snapshot - the
same one the sync ran against - so a golden-config failure is traceable to the
exact network state that produced it.

Probed live before design against the validation deployment's latest
processed snapshot: the verbatim
`show run` text is `device.outputs.commands` where
`commandType == CommandType.CONFIG` - 4,305 devices carry one, 2.4 GB total,
average 557 KB, largest 20 MB. Forward strips collected credentials but the
text retains hashed secrets (`enable secret 9 …`), so the repo is
access-controlled material like any config backup repo - the operator chooses
where it lives.

## Constraints

- **NQE, not REST.** One paginated query serves the fleet; REST would be 4,305
  per-device calls - the call-volume pattern Forward engineering asked us to
  cut - with no snapshot pinning.
- **Not a registered query map.** The map machinery is keyed to NetBox model
  rows and drags in variants, reducers and diff contracts; configs are not
  model rows. Direct client fetch, like the scope probes. Device mapping
  happens NetBox-side via `ForwardDeviceIdentity`, which is also why no alias
  variant is needed: the query never renders a NetBox name.
- **Scoped at the FETCH, by the same tag scope as the rest of the sync.** The
  query takes `forward_netbox_shard_keys`, exactly as every device-scoped
  bundled query does, and the job passes the device names it holds identities
  for. An unscoped fetch transfers the whole collected estate and discards
  whatever has nowhere to go. Note the NQE convention that an EMPTY key list
  means unscoped - so a sync with no identities must not fetch at all.
- **Never `fetch_all`.** The client has row-count ceilings but no byte
  ceiling; 2.4 GB through `fetch_all` sits in worker RAM. Manual pagination
  with a small page size, each page written into the repo tree and discarded.
- **No new credential storage.** The repo URL, username, password and branch
  come from the operator's existing `core.DataSource` (type git) - the same
  object Validity reads and NetBox already encrypts. The plugin holds a
  pointer, never a secret.
- **dulwich, object-level.** The runtime has dulwich 1.2.11 and NO git binary.
  No working-tree checkout: fetch the remote head, rewrite tree entries for
  changed devices only, commit, push.
- **Config text never reaches logs, ingestion issues, or support bundles.**
  Counts and device names only.
- Skip entirely when the snapshot already backed up equals the current one.

## Touched Surfaces

- `forward_netbox/queries/forward_config_backup.nqe` (new; not registered in
  the map registry - loaded by the job)
- `forward_netbox/utilities/config_backup.py` (new): fetch, map, tree rewrite,
  commit, push, data-source sync trigger
- `forward_netbox/jobs.py`: `_maybe_enqueue_config_backup` +
  `ConfigBackupJob` + work function, wired into
  `_enqueue_post_sync_overlays`
- `forward_netbox/forms.py`: `config_backup_data_source` field, BOTH fieldsets
  (SaaS and on-prem), initial, `clean()`, `save()`
- `forward_netbox/utilities/model_validation.py`: parameter allowlist
- `forward_netbox/models.py`: `get_masked_parameters` allowlist
- `forward_netbox/tests/test_config_backup.py` (new)
- `docs/01_User_Guide/configuration.md`

## Approach

Opt-in via one source parameter, `config_backup_data_source` (a
`core.DataSource` pk; unset = feature off). Enabling is choosing a repo.

The post-sync overlay job (the `_maybe_enqueue_device_analysis_refresh`
pattern, guarded by `current_post_sync_snapshot`) then:

1. Reads the data source's url/username/password/branch.
2. Fetches the remote head with dulwich into a temp bare repo; loads its tree.
3. Pages the NQE query (name + config text, 100 rows/page), scoped to the
   identity table's device names; for each row, computes the blob SHA and
   rewrites the tree entry under `configs/<netbox-device-name>.cfg` only when
   it differs.
4. If anything changed, commits with the snapshot ID and pushes; then calls
   `DataSource.sync()` so DataFiles - and Validity - are current immediately.
5. Records counts (written, unchanged, unmapped, pages) on the job.

## Validation - proven against Validity end to end

The whole chain was run in the development runtime with `netbox-validity`
installed: our backup job -> git push -> `DataSource.sync()` -> `DataFile` ->
`VDataSource.get_config_path(device)` -> matching data file -> content
identical to what Forward returned. Validity binds a device to its
configuration through a `device_config_path` custom field on the data source,
rendered as Jinja2 with `device` in context, so `configs/{{device.name}}.cfg`
is exactly what our layout produces.

**That test failed on its first run, and the failure was the point.** The data
source synced ZERO files while the backup reported `pushed=True`. A bare
repository's `HEAD` points at `refs/heads/master`; this code assumed `main`
when the data source names no branch, so the push landed on a branch nobody
reads, NetBox cloned `HEAD`, and found nothing. Every counter said success and
nothing was delivered - the worst failure shape available. No unit test here
could have caught it, because they all inspect the repository we push TO;
only cloning it the way NetBox does exposes the mismatch.

The branch is now resolved from the remote's own `HEAD`, with an explicit
`branch` parameter still winning, and two regression tests pin both paths.

## Validation

Unit tests against a local bare repository (dulwich end-to-end, no network):
first run writes all mapped devices; second run with identical content
produces no commit; a changed config produces a one-file commit; unmapped
Forward devices are counted and skipped; config text never appears in job log
data. Form-render test covers BOTH fieldset branches (the 2026-07-06
endpoint-toggle lesson). Live verification against the validation source
before release.

## Rollback

Unset the parameter; the overlay stops enqueueing. No migration. The git
repository is the operator's and is untouched by a rollback.

## Decision Log

- **NQE over REST** - measured, not argued: the REST device endpoints 404 on
  this instance while the NQE path returned the config verbatim; and only NQE
  pins to a snapshot.
- **Git-layer change detection over NQE snapshot diffs** for v1. `run_nqe_diff`
  requires a published query id, and the diff pipeline is welded to model
  workloads. Blob-hash comparison gives the same "commit only changes" result
  today; per-snapshot transfer (~2.4 GB) is the cost, call count (~1 query +
  ~90 pages) is not. NQE diff is the recorded optimization once the query has
  an org identity.
- **DataSource as the single config surface** - the repo Validity reads is by
  definition already configured in NetBox; duplicating url+secret in our
  parameters would be a second place for the same credential to rot.
- **Never assume the default branch.** See the validation section: assuming
  `main` produced a silent no-op backup. The remote's `HEAD` is the only
  authority on where its default lives.
- **The query's trailing `;` is load-bearing** and was missing until a live
  call rejected it. No unit test executes NQE against Forward, so the fake
  client accepted a query the real one would not have - the reason this
  feature was probed live before it was written and verified live after.
- **Files keyed by NetBox device name**, because Validity's path templates
  render NetBox names; the identity table is the authoritative mapping and
  already handles aliasing.

## Open

- NQE diff optimization (above) once bundled queries carry org query ids in
  the customer's deployment.
- Devices in Forward with a CONFIG output but no NetBox identity are now
  excluded at the fetch rather than counted and discarded, so `unmapped`
  becomes a real signal: nonzero means an identity-mapping fault, not routine
  out-of-scope inventory. Whether an operator wants unmanaged devices archived
  under a separate prefix remains a product question, deliberately unanswered.
  (An earlier draft of this plan estimated that population at ~900 by
  subtracting a figure from one deployment from a count taken on another; the
  number was not measured and has been removed rather than repeated.)
- The 20 MB outlier config is one NQE row; page size 100 keeps worst-case page
  memory ~2 GB only if 100 such outliers cluster, which the probe says they do
  not (one device >20 MB). A per-page byte budget is noted as hardening.
