# Troubleshooting

## Source Authentication Fails

Symptoms:

- The source form cannot populate `Network`.
- Saving the source succeeds, but later syncs fail immediately.

Checks:

- Confirm the `URL`, `Username`, and `Password` are correct.
- For SaaS, confirm the source type is `Forward SaaS`.
- For custom deployments, confirm `Verify` matches the deployment certificate state.

## Network Selection Does Not Populate

Symptoms:

- The `Network` field remains empty after entering credentials.

Checks:

- Confirm the Forward account has access to at least one network.
- Confirm the selected source URL is correct.
- Confirm the Forward account password has been set (new accounts must log in and set a password before network discovery works).
- Re-enter the password if the source was edited with a blank password field previously.

## NQE Map Validation Fails

Symptoms:

- Saving an NQE map returns a validation error.

Checks:

- Define exactly one of `Query ID` or `Query`.
- If using `Query ID`, ensure the published query exists in Forward.
- If using raw `Query`, validate the NQE text directly against the Forward API if needed.
- Keep the `NetBox Model` aligned with the output shape of the query.
- Ensure `Coalesce Fields` are valid for the selected model and use non-empty field sets.

## Sync Fails With Identity/Coalesce Errors

Symptoms:

- Sync fails with messages about missing required fields, coalesce mismatch, or ambiguous coalesce lookups.

Checks:

- Confirm the active map query returns all required fields for that model.
- Confirm each row satisfies at least one configured coalesce field set.
- If multiple NetBox rows can match the same coalesce keys, fix the duplicate data in NetBox or tighten coalesce fields.
- Prefer keeping NetBox-ready shaping in NQE rather than adding Python-side normalization.
- If `ipam.ipaddress` fails with a global-table duplicate IP error, inspect the selected Forward snapshot for the same host IP reported on multiple interfaces with different masks. The built-in query now collapses global-table host collisions to one deterministic row, so a failure here usually means the snapshot or a custom query is still emitting an exact duplicate global IP.
- If an ingestion warning reports IP addresses without an imported parent prefix,
  compare the enabled `ipam.prefix` maps with the affected IP rows. The plugin
  logs capped examples on full baseline runs where both models are enabled, but
  does not synthesize missing parent prefixes.

Expected row-scoped apply/delete failures are recorded as `Forward Ingestion Issues` and counted as failed or skipped rows without stopping the rest of the shard. Preflight, query execution, branch, and merge failures can still stop the current phase because they are not tied to a single row.

## Sync Fails Before Staging

Symptoms:

- The sync status moves to failed quickly.
- A new ingestion is created with early issues.
- The latest validation run is `Blocked` or `Failed`.

Checks:

- Confirm the source has a valid `Network` selected.
- Confirm the selected `Snapshot` is valid for that source network, or leave it at `latestProcessed`.
- Confirm the required NQE maps are enabled.
- Open the sync `Health` tab before rerunning. It summarizes source status,
  query binding mode, diff eligibility, data-file-dependent maps, worker/source
  timeout settings, latest validation, latest ingestion, latest execution run,
  and the current recovery recommendation without making live Forward API calls.
  The `Export ZIP` control on the sync page packages the same support-bundle
  evidence into a compressed archive, and accepts an optional password when the
  bundle needs to be shared externally.
  The bundle also carries live source health, live query-drift, and live
  data-file health checks so support can work from a single artifact when the
  issue is reported later.
  Use `Export Live Source Check` when you want the plugin to call Forward and
  export reachability diagnostics for the configured source/network/snapshot
  without including the network ID or snapshot ID in the export.
  It also includes `Local Query Drift` diagnostics that compare enabled maps to
  bundled query metadata where that can be done locally. Direct query IDs remain
  org-specific and require a live Forward repository lookup for full source/commit
  drift verification; use `Export Live Query Drift Check` from the Health tab
  when you want the plugin to call Forward and export repository/query-ID drift
  diagnostics. Use `Export Live Data File Check` when optional data-file-backed
  maps are enabled and you need to confirm the selected snapshot actually sees
  rows for the required Forward NQE data files. When execution-step timing is
  available, `Capacity Projection` estimates remaining shard time from completed
  ledger steps and warns when observed shard duration approaches the configured
  worker timeout. `Compatibility Cache` also reports whether the sync still has
  legacy `_branch_run` payload state after execution-ledger history exists.
  If it reports stale payloads, prune them with:

  ```bash
  invoke prune-compat-cache --sync-name "<sync_name>" --dry-run=True
  invoke prune-compat-cache --sync-name "<sync_name>" --dry-run=False
  ```
- If the selected model is `dcim.module`, confirm the optional beta module path
  is still enabled in the plugin and that the `Forward Modules` NQE Map remains
  turned on.
- Open the latest `Forward Validation Run` from the sync detail page and review `Blocking Reasons`, `Drift Summary`, and `Model Results`.
- If a drift policy blocked the run, adjust the policy or fix the query/source data before rerunning the sync.
- If the block is intentionally accepted, use `Force allow` on the validation run to record the override reason and reviewer, then rerun the sync.
- Check `Forward Ingestion Issues` for the failing model and error text.

## Validation Blocks The Sync

Symptoms:

- No Branching branch is created.
- The validation run status is `Blocked`.
- The ingestion issue mentions `Forward validation blocked sync`.

Checks:

- For snapshot checks, confirm the selected snapshot is processed.
- For zero-row checks, confirm the enabled model should return rows and that the active NQE map targets the selected snapshot.
- For deletion thresholds, review whether the run used a diff baseline and whether the destructive-change count is expected.
- If the run was force-allowed, verify the validation run shows the override reason and actor before rerunning.
- Use `Validate` on the sync page to rerun policy checks before staging changes.

## Sync Or Merge Times Out

Symptoms:

- Sync or merge ends with timeout status.
- Ingestion issues include `JobTimeoutException`.
- Job logs mention `RQ_DEFAULT_TIMEOUT`.

Checks:

- Compare NetBox `RQ_DEFAULT_TIMEOUT` to the Forward source `Timeout`; the
  worker timeout should be higher than the longest expected Forward API/NQE
  request plus NetBox staging/merge time. The default Forward source timeout is
  20 minutes because current Forward public NQE API execution uses a 20-minute
  default query-compute timeout.
- Increase the RQ worker timeout in your NetBox deployment before rerunning a
  large baseline.
- For a very large trusted first baseline, consider `Fast bootstrap`; switch
  back to `Branching` for normal diff-based runs after the baseline succeeds.
- If using Branching, keep `Max changes per branch` aligned to your Branching
  guidance and let the plugin shard the run instead of raising the branch
  budget to force everything into one branch.
- Use `Continue Ingestion` to resume a resumable Branching baseline from the
  recorded plan index after a timeout. Do not start a new baseline unless the
  plan or source data needs to change.
- If the timeout happened after a shard was staged, open that ingestion and
  requeue the merge instead of rerunning the shard.
- Use `Export Support Bundle` from the sync page or `Export Logs` from the
  ingestion page. For multi-shard Branching
  runs, the export includes the execution run/step bundle with shard statuses,
  linked stage/merge jobs, retry counts, branch names, health summary, and last
  errors. Execution-run support bundles now also include compatibility-cache
  retirement evidence (legacy payload presence, active-run linkage, stale
  payload detection, and prune recommendation). The `Export ZIP` control on the
  sync and execution-run pages compresses the same payload and can be password
  protected when the archive needs to be shared outside the current trust
  boundary.
- Review `core/jobs` plus ingestion issues for the matching timestamp window
  when the exported bundle points to a specific failed job.

Example NetBox configuration:

```python
RQ_DEFAULT_TIMEOUT = 7200
```

Set the final value according to your environment, worker supervision policy,
and how long a trusted large baseline is allowed to run.

## Sync Is Slow But Still Running

Symptoms:

- Sync stays in `Syncing` for a long time.
- No timeout/failure is recorded, but shard progress is slow.
- Host CPU and memory look underutilized.

Checks:

- Confirm the run is actually progressing:
  `Sync` detail `Execution` summary, latest ingestion `Logs`, and execution-step heartbeat.
- Open the sync `Health` tab and review `Large Run Tuning`. It ranks the first
  action the plugin can infer from ledger metrics: restore diff utilization,
  reduce fallback fetches, tune timeout/capacity, inspect worker/database
  headroom, adjust query fetch concurrency, or choose the right execution
  backend for the current run.
- In `Large Run Tuning`, check `Backend advice` before changing worker or query
  settings. If a Branching baseline is projected near worker timeout, use Fast
  bootstrap only for a trusted first baseline. If Fast bootstrap is active,
  complete the baseline and switch back to Branching for steady-state diff
  review.
- In `Query Runtime & Pushdown`, check `Baseline to diff`. This explains
  whether the run is using API diffs, is still creating a Fast bootstrap
  baseline, is missing a compatible prior baseline, is using non-diff-capable
  raw query maps, or requested diffs but fell back to full execution.
- For steady-state diff runs where speed is the priority, set sync `Diff fallback mode` to
  `Require diff` so maps fail fast instead of silently broadening to full-query execution.
- Export the sync support bundle when asking for help. The execution-run
  metrics include `operator_tuning_summary`, `throughput_smoothing`,
  `fallback_reason_summary`, `diff_baseline_transition`, and pushdown/diff
  signals needed to identify whether the run is query-bound, apply-bound,
  merge-bound, waiting on scheduling/merge handoff, or missing the expected API
  diff path.
- Verify source runtime knobs:
  `query_fetch_concurrency` and `nqe_page_size` on the active `Forward Source`.
- Verify NetBox worker replica count and Postgres capacity for the same window.
- Keep `max_changes_per_branch` near guidance; increasing it can make each shard
  take longer even when total shard count drops.

Large-ingestion triage order:

1. Confirm execution mode and intent:
   - First baseline: `Fast bootstrap` for trusted high-volume imports.
   - Steady state: `Branching` with repository `query_path`/`query_id` maps for diff eligibility.
   - Use the Sync Health `Backend advice` field as the first local signal when
     timeout or baseline size makes the backend choice unclear.
2. Confirm workers/database are not under-sized relative to host capacity.
3. Confirm `query_fetch_concurrency` is not too low for preflight volume.
4. Confirm shard sizing is conservative (`max_changes_per_branch` near guidance) instead of oversized branches.
5. Confirm long-running jobs have sufficient worker timeout (`RQ_DEFAULT_TIMEOUT` > expected shard runtime).

Local Docker tuning path:

```bash
invoke forward_netbox.optimize-runtime --worker-replicas 0 --query-fetch-concurrency 16 --nqe-page-size 10000 --apply-postgres
```

Operational notes:

- This improves query fetch/preflight and DB throughput, but one Branching
  shard/merge remains mostly serialized by native NetBox Branching semantics.
- For trusted very large first baselines, use `Fast bootstrap`, then switch
  back to `Branching` once baseline exists for diff-based steady state.
- Use `invoke forward_netbox.ingestion-delete-regression` to validate ingest and
  diff-delete behavior in synthetic regression before live reruns.

What to collect when opening a tuning issue:

- `Export Support Bundle` from the Sync page.
- Source runtime parameters (`timeout`, `nqe_page_size`, `query_fetch_concurrency`, `nqe_fetch_all_max_pages`, `nqe_identical_full_page_streak_limit`).
- Sync mode (`Fast bootstrap` or `Branching`) and `max_changes_per_branch`.
- NetBox worker count and worker timeout (`RQ_DEFAULT_TIMEOUT`).

## Fast Bootstrap Looks Like A Dry Run

Symptoms:

- A fast-bootstrap ingestion is running or completed.
- The ingestion counters stay empty.
- The ingestion `Changes` tab says no changes were found.
- The global NetBox change log does not show expected object changes.

Checks:

- Confirm the plugin version includes fast-bootstrap direct change tracking.
  Fast bootstrap should store a request id on the branchless ingestion and use
  native NetBox `ObjectChange` rows for created/updated/deleted counters.
- Open the branchless ingestion `Changes` tab; for fast bootstrap it should list
  direct NetBox object changes rather than Branching diffs.
- If the counters remain empty after rows are applied, check `Forward Ingestion
  Issues` for row-level failures and confirm the enabled maps are returning
  non-empty rows for the selected snapshot.
- If the global change log is empty but objects exist, verify NetBox change
  logging is enabled and the sync is not running under a custom path that clears
  the NetBox request context.

## Merge Records Skipped Changes

Symptoms:

- The merge completes, but the merge log reports skipped changes.
- `Forward Ingestion Issues` contains merge-phase failures.

Checks:

- Review the merge-phase ingestion issues first; they identify the exact model and validation error.
- Confirm the affected built-in or custom NQE map is emitting NetBox-valid values directly.
- Do not patch the value in Python if the intended contract is NetBox-ready NQE output; fix the query instead.

## Virtual Chassis Shard Fails With `vc_position`

Symptoms:

- The sync appears to progress, but an ingestion part for `dcim.virtualchassis`
  records a failure similar to `A device assigned to a virtual chassis must
  have its position defined` or a duplicate `vc_position` validation error.
- Routing, cabling, IP, or other downstream models appear empty or incomplete
  because the base device/virtual-chassis shard did not finish cleanly.

Checks:

- Confirm whether the `Forward Virtual Chassis` map is customized or bound to an
  older Forward repository query. The bundled map is conservative and emits no
  rows by default because Forward HA peer relationships such as vPC, MLAG, and
  active/standby clusters are not native NetBox virtual chassis membership.
- If you intentionally import `dcim.virtualchassis`, confirm the map emits
  `device`, `vc_name`, `name`, `vc_domain`, and a unique `vc_position` per
  member in each virtual chassis.
- If the map is bound to a Forward Org Repository `query_path` or a pinned
  `query_id`, upgrading the NetBox plugin does not rewrite the already-published
  Forward query. Use the native NQE map bulk edit workflow and select `Publish
  bundled queries to Org Repository and bind selected maps` with `Overwrite
  existing repository queries` enabled, or restore the affected map to bundled
  raw query text.
- Re-run validation or a sync after the map has been refreshed. Current plugin
  versions fail stale or invalid virtual-chassis query output during preflight
  instead of allowing an invalid VC assignment to surface later as a device save
  failure.

## Snapshot Metrics Are Missing

Symptoms:

- The ingestion completes, but the `Forward Snapshot Metrics` card is empty.

Checks:

- Confirm the selected snapshot still exists in Forward.
- Confirm the Forward API user can read snapshot metrics for that snapshot.
- Review the job log for a warning about fetching snapshot metrics.

## Sync Stages But Merge Fails

Symptoms:

- The ingestion finishes, but merge does not complete.

Checks:

- Review the merge job record from the ingestion detail page.
- Review the branch diff for unexpected object conflicts.
- Re-run the sync after correcting the underlying data or query issue.

## Built-In Maps Are Missing

Symptoms:

- `NQE Maps` opens, but the built-in maps are not present.

Checks:

- Confirm `python manage.py migrate` completed successfully.
- Confirm the plugin is enabled in `PLUGINS`.
- Run migrations again and re-open the plugin menu.

## NetBox Has More Devices Than Expected

Symptoms:

- A tag-scoped sync (e.g. scoped to `Prod_Core`) leaves more devices in NetBox
  than the tag should match.

Cause:

- The device scope is enforced as an allowlist of devices that are **tagged and
  collected (`completed`)** in the resolved snapshot. Devices imported by an
  earlier, broader sync are not deleted automatically — out-of-scope pruning is
  opt-in. Tagged devices that were backfilled (collection canceled) are also
  excluded from the current allowlist, so they linger from a prior run.

Checks:

- Run the read-only reconciliation audit to break the count down:

  ```
  python manage.py forward_device_scope_reconciliation_audit --sync-name "<sync_name>"
  ```

  It reports `forward_in_scope_completed` (expected), `netbox_out_of_scope`
  (stale leftovers), and `netbox_present_backfilled` (tagged but not collected
  this snapshot), with capped name samples. Add `--fail-on-drift` for monitoring.

Remediation:

- To delete out-of-scope devices on each run, enable
  `device_tag_prune_out_of_scope` on the sync. Review `out_of_scope_sample`
  first — pruning issues deletes.

## APIC CIMC Inventory Is Empty

Symptoms:

- The `Forward ACI APIC CIMC Inventory` map is enabled but produces zero
  `dcim.inventoryitem` rows, even though APIC devices are present.

Cause:

- That map parses the `moquery -c eqptCh -a all` APIC custom command. It yields
  nothing unless that command was collected on a **completed** (non-backfilled)
  APIC device in the resolved snapshot.

Checks:

- Run the read-only readiness audit:

  ```
  python manage.py forward_apic_cimc_readiness_audit --sync-name "<sync_name>"
  ```

  It reports the APIC device count, how many carry the controller-detail and
  `eqptCh` commands, and `cimc_inventory_ready`. If `with_eqptch_command` is
  greater than zero but `completed_with_eqptch` is zero, the command exists only
  on a backfilled snapshot. Add `--fail-on-missing` for monitoring.

Remediation:

- Add `moquery -c eqptCh -a all` as a recurring custom command on the APICs in
  Forward so it is collected into a completed snapshot, then re-run the sync.

## Collect Logs And Issue Evidence

Use these commands when a customer reports a failed or hanging sync.

### 1) Pull plugin API evidence

Replace `${NETBOX_URL}` and `${NETBOX_TOKEN}` with your values.

```bash
# Latest sync objects
curl -sS -H "Authorization: Token ${NETBOX_TOKEN}" \
  "${NETBOX_URL}/api/plugins/forward/sync/?limit=50"

# Latest ingestions (includes snapshot metadata)
curl -sS -H "Authorization: Token ${NETBOX_TOKEN}" \
  "${NETBOX_URL}/api/plugins/forward/ingestion/?limit=50"

# Latest validation runs (includes drift summary and blocking reasons)
curl -sS -H "Authorization: Token ${NETBOX_TOKEN}" \
  "${NETBOX_URL}/api/plugins/forward/validation-run/?limit=50"

# All ingestion issues (filter locally by ingestion/model/message as needed)
curl -sS -H "Authorization: Token ${NETBOX_TOKEN}" \
  "${NETBOX_URL}/api/plugins/forward/ingestion-issues/?limit=0"
```

In 0.1.4.1 and newer, each issue includes structured fields to speed root-cause analysis:

- `coalesce_fields`: identity keys used for matching
- `defaults`: payload values attempted for create/update
- `raw_data`: original row emitted by the NQE query

### 2) Pull NetBox job records

```bash
# All recent jobs (look for the Forward sync and merge jobs)
curl -sS -H "Authorization: Token ${NETBOX_TOKEN}" \
  "${NETBOX_URL}/api/core/jobs/?limit=100"

# Optional: details for one specific job id
curl -sS -H "Authorization: Token ${NETBOX_TOKEN}" \
  "${NETBOX_URL}/api/core/jobs/<job-id>/"
```

### 3) Pull plugin runtime logs from the host/container

For a full sync/run handoff, use `Export Support Bundle` from the sync detail
page first. For a single failed ingestion, use the native `Export Logs` action
on the ingestion detail page. Both downloads include linked execution-run and
job context so the failure can be investigated without scraping the UI.

Also check the sync `Health` tab before collecting logs. It is read-only and is
intended to answer the common first-pass questions: whether the source is marked
ready, whether enabled maps are diff-capable, whether optional data-file maps
are active, whether a baseline-ready ingestion exists, and whether the latest
execution run recommends waiting, retrying, requeueing merge, or exporting a
support bundle. Use `Export Live Source Check` from that tab for a live
Forward reachability export. Its local query-drift check can identify raw bundled-query
modifications and repository paths that no longer match the bundled query name;
it does not contact Forward to compare repository source or commit contents
unless you use the explicit `Export Live Query Drift Check` action. For
optional data-file-backed maps, use `Export Live Data File Check` to verify that
the selected snapshot has captured the uploaded data file value.

If file logging is enabled, collect:

```bash
/var/log/netbox/forward_netbox.log
```

For containerized deployments where file logging is not enabled, collect container stdout/stderr:

```bash
docker compose logs netbox --since=2h
docker compose logs netbox-worker --since=2h
```

### 4) Filter to one ingestion or one model quickly

```bash
# Only issues for one ingestion id
curl -sS -H "Authorization: Token ${NETBOX_TOKEN}" \
  "${NETBOX_URL}/api/plugins/forward/ingestion-issues/?ingestion_id=<ingestion-id>&limit=0"

# Only issues for one model string (example: dcim.devicerole)
curl -sS -H "Authorization: Token ${NETBOX_TOKEN}" \
  "${NETBOX_URL}/api/plugins/forward/ingestion-issues/?model=dcim.devicerole&limit=0"
```

### 4b) Classify blocking vs non-blocking issues quickly

Run this in the NetBox runtime container/host:

```bash
# Audit one ingestion directly
python manage.py forward_blocker_audit --ingestion-id <ingestion-id>

# Audit latest ingestion for one sync name
python manage.py forward_blocker_audit --sync-name "<sync-name>"

# Exit non-zero if blocking issues are present
python manage.py forward_blocker_audit --ingestion-id <ingestion-id> --fail-on-blocking
```

This command classifies issues into:

- **blocking**: rows that should prevent baseline readiness
- **non-blocking**: optional-model and dependency-skip rows (`ForwardDependencySkipError`)

### 4c) Watch a sync until terminal state

```bash
# Poll by sync name until completed/failed/ready_to_merge
python manage.py forward_watch_sync --sync-name "<sync-name>"

# Poll by id and fail the command if blocking issues are present at completion
python manage.py forward_watch_sync --sync-id <sync-id> --fail-on-blocking

# Bound polling for automation
python manage.py forward_watch_sync --sync-id <sync-id> --max-polls 120 --interval-seconds 30

# Bound polling for long-running jobs without treating non-terminal status as failure
python manage.py forward_watch_sync --sync-id <sync-id> --max-polls 120 --interval-seconds 30 --allow-nonterminal
```

Each poll now includes `execution_run` state (active shard, shard job id,
run/step heartbeat age) in addition to the initial sync job log entry. Use this
to distinguish:

- a queued planning job that already finished, versus
- an active shard stage still running under the execution ledger.

### 4d) Audit warning/error volume for regression checks

```bash
# Audit latest ingestion job logs for one sync
python manage.py forward_warning_audit --sync-name "<sync-name>"

# Aggregate across all ingestions for one sync
python manage.py forward_warning_audit --sync-id <sync-id> --all-ingestions --top 20

# Exit non-zero if any errors are present in sync job logs
python manage.py forward_warning_audit --sync-id <sync-id> --all-ingestions --fail-on-error
```

Use this command after upgrades to confirm warning noise is stable and to surface
new high-volume warnings that may indicate a regression. The audit includes:

- ingestion and merge jobs, and
- active execution-run shard/merge jobs (including live `job.log_entries` before
  final `job.data["logs"]` serialization).

### 5) Validate Forward connectivity from the NetBox runtime

Run this from inside the NetBox container or host where NetBox executes:

```bash
curl -sS -u "<forward-username>:<forward-password>" \
  "https://fwd.app/api/networks"
```

If this fails with timeout/DNS/TLS errors, the issue is environmental connectivity rather than ingestion mapping logic.

### 6) Recommended community troubleshooting bundle

When sharing evidence in community channels (for example GitHub issues/discussions), include:

- Output of `ingestion-issues` API for the failing ingestion
- Output of `core/jobs` for the same timestamp window
- `netbox` and `netbox-worker` container logs for the same window
- NetBox plugin version and NetBox version
- Failing sync name, ingestion id, and model being ingested when it failed

### 7) Redact before sharing

Before uploading logs externally, remove or mask:

- `Authorization` headers and API tokens
- Forward usernames/passwords
- Any internal hostnames/IPs that are not required for troubleshooting context
