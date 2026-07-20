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

Expected row-scoped apply/delete failures are recorded as `Forward Ingestion
Issues` and counted as failed or skipped rows without stopping the rest of the
current workload unit. Preflight, query execution, branch, and merge failures
can still stop the current phase because they are not tied to a single row.

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
  timeout settings, latest validation, latest ingestion, branch and job state,
  ownership reconciliation, and the current recovery recommendation without
  making live Forward API calls.
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
  available, `Capacity Projection` uses ingestion and job timing to warn when
  the complete one-branch run approaches the configured worker timeout.
- If the selected model is `dcim.module`, confirm that the `Forward Modules`
  NQE Map remains enabled and that the exact NetBox 4.6.5/Branching 1.1.1
  runtime is installed.
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
- Keep `Max changes per staging item` aligned to local capacity guidance so planning
  warnings remain meaningful; version 2.6 still stages exactly one branch.
- If staging timed out, inspect the branch and ingestion issues before
  rerunning. Do not start a second sync while the prior branch is nonterminal.
- If the timeout happened during merge, open that ingestion and requeue the
  same branch merge. A partial merge remains retryable and cannot become a
  baseline.
- Use `Export Support Bundle` from the sync page or `Export Logs` from the
  ingestion page. The export includes branch status, linked stage/merge jobs,
  ingestion counters, issues, ownership reconciliation, health summary, and
  last errors. The `Export ZIP` control compresses the same sanitized payload
  and can be password protected when the archive needs to be shared outside
  the current trust boundary.
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
- No timeout/failure is recorded, but staging or merge progress is slow.
- Host CPU and memory look underutilized.

Checks:

- Confirm the run is actually progressing:
  sync status, latest ingestion logs, branch status, and the linked NetBox jobs.
- Open the sync `Health` tab and review `Large Run Tuning`. It ranks the first
  action the plugin can infer from persisted run evidence: restore diff utilization,
  reduce fallback fetches, tune timeout/capacity, inspect worker/database
  headroom, or adjust query fetch concurrency.
- In `Query Runtime & Pushdown`, check `Baseline to diff`. This explains
  whether the run is using API diffs, is still creating its first baseline, is
  missing a compatible prior baseline, is using non-diff-capable
  raw query maps, or requested diffs but fell back to full execution.
- For steady-state diff runs where speed is the priority, set sync `Diff fallback mode` to
  `Require diff` so maps fail fast instead of silently broadening to full-query execution.
- Export the sync support bundle when asking for help. Its aggregate metrics
  include `operator_tuning_summary`, `throughput_smoothing`,
  `fallback_reason_summary`, `diff_baseline_transition`, and pushdown/diff
  signals needed to identify whether the run is query-bound, apply-bound,
  merge-bound, waiting on scheduling/merge handoff, or missing the expected API
  diff path.
- Verify source runtime knobs:
  `query_fetch_concurrency` and `nqe_page_size` on the active `Forward Source`.
- Verify NetBox worker replica count and Postgres capacity for the same window.
- Keep `max_changes_per_staging_item` near guidance so oversized workload warnings
  remain actionable. It does not split the sync into multiple branches.

Large-ingestion triage order:

1. Confirm snapshot and query intent: the first run establishes a reviewable
   Branching baseline; steady-state repository `query_path`/`query_id` maps can
   become diff eligible.
2. Confirm workers/database are not under-sized relative to host capacity.
3. Confirm `query_fetch_concurrency` is not too low for preflight volume.
4. Confirm `max_changes_per_staging_item` remains near local guidance so warnings are useful.
5. Confirm long-running jobs have sufficient worker timeout (`RQ_DEFAULT_TIMEOUT` > expected stage or merge runtime).

Local Docker tuning path:

```bash
invoke forward_netbox.optimize-runtime --worker-replicas 0 --query-fetch-concurrency 16 --nqe-page-size 10000 --apply-postgres
```

Operational notes:

- This improves query fetch/preflight and DB throughput, but the one Branching
  branch and merge remain mostly serialized by native NetBox semantics.
- Use `invoke ingestion-delete-regression` to validate ingest and
  diff-delete behavior in synthetic regression before live reruns.

What to collect when opening a tuning issue:

- `Export Support Bundle` from the Sync page.
- Source runtime parameters (`timeout`, `nqe_page_size`, `query_fetch_concurrency`, `nqe_fetch_all_max_pages`, `nqe_identical_full_page_streak_limit`).
- Single-branch workload estimate and `max_changes_per_staging_item`.
- NetBox worker count and worker timeout (`RQ_DEFAULT_TIMEOUT`).

## Single-Execution Progress Looks Like A Dry Run

Symptoms:

- A single-branch ingestion is running or completed.
- The ingestion counters stay empty.
- The ingestion `Changes` tab says no changes were found.
- The global NetBox change log does not show expected object changes.

Checks:

- Confirm the ingestion has one provisioned Branching branch and that the branch
  contains native `ObjectChange` rows.
- Open the ingestion `Changes` tab; it should list the staged branch diff.
- If the counters remain empty after rows are applied, check `Forward Ingestion
  Issues` for row-level failures and confirm the enabled maps are returning
  non-empty rows for the selected snapshot.
- If staging counters remain empty, verify NetBox change logging is enabled and
  inspect the sync job before attempting a merge.

## Merge Records Skipped Changes

Symptoms:

- The merge job errors and reports an incomplete merge.
- `Forward Ingestion Issues` contains merge-phase failures.
- The branch remains ready for inspection and the sync returns to **Ready to merge**.

Checks:

- Review the merge-phase ingestion issues first; they identify the exact model and validation error.
- Confirm the affected built-in or custom NQE map is emitting NetBox-valid values directly.
- Do not patch the value in Python if the intended contract is NetBox-ready NQE output; fix the query instead.
- Retry the same branch after correcting the cause. Already-applied rows are
  idempotent; only a clean retry marks the baseline complete and starts
  ownership reconciliation.

## Virtual Chassis Workload Fails With `vc_position`

Symptoms:

- The sync appears to progress, but an ingestion part for `dcim.virtualchassis`
  records a failure similar to `A device assigned to a virtual chassis must
  have its position defined` or a duplicate `vc_position` validation error.
- Routing, cabling, IP, or other downstream models appear empty or incomplete
  because the base device/virtual-chassis workload did not finish cleanly.

Checks:

- Confirm the optional `dcim.virtualchassis` model is enabled only with a custom
  `Forward Virtual Chassis` map. The bundled contract template is disabled and
  emits no rows because Forward HA peer relationships such as vPC, MLAG, and
  active/standby clusters are not native NetBox virtual chassis membership.
- If you intentionally import `dcim.virtualchassis`, confirm the map emits
  `device`, `vc_name`, `name`, `vc_domain`, and a unique `vc_position` per
  member in each virtual chassis.
- If the map is bound to a Forward Org Repository `query_path` or a pinned
  `query_id`, upgrading the NetBox plugin does not rewrite the already-published
  Forward query. Use the native NQE map bulk edit workflow and select `Publish
  bundled queries to Org Repository and bind selected maps` with `Overwrite
  existing repository queries` enabled, use **Publish Bundled Queries** from the
  sync Health page for its enabled built-in maps, or restore the affected map to
  bundled raw query text.
- Re-run validation or a sync after the map has been republished. Current plugin
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
- Correct the underlying data or query issue and retry the same branch. Do not
  start a replacement sync while the incomplete branch is open.
- After a clean merge and completed overlays, run:

  ```bash
  python manage.py forward_ownership_audit \
    --fail-on-inconsistent --require-no-open-branches
  ```

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
- A device carries both `forward-out-of-scope` and one of the source's Forward
  include tags.
- Imported console servers have no matching NetBox scope tags.
- Opengear or Avocent software/build strings appear as DeviceType models.

Cause:

- The device scope is enforced as an allowlist of devices that are **tagged and
  collected (`completed`)** in the resolved snapshot. Devices imported by an
  earlier, broader sync are **orphans**: they are no longer in the scoped Forward
  result at all, so the sync never sees them. `device_tag_prune_out_of_scope`
  does **not** remove them — it only deletes rows the sync query *returns* that
  fall outside scope, and orphans are absent from the result. Tagged devices that
  were backfilled (collection canceled) are also excluded from the current
  allowlist, so they linger too (but are real, not orphans).
- SNMP endpoints use the source's explicit **Scope SNMP Endpoints by Include
  Tags** policy. Version 2.6 converts an include-scoped source with no recorded
  decision to enabled (fail closed); only a previously explicit opt-out remains
  disabled. Before 2.5.10, endpoint tag intersections were not added to the
  device scope-tag map, and console-server `sysDescr` software/build suffixes
  could become DeviceType identity.
- Endpoint import now defaults to recognized Avocent/Opengear console servers.
  **Import Generic SNMP Endpoints as Devices** is a separate broad opt-in. CIMC
  endpoints are excluded by endpoint name, profile, or controller `sysDescr`.
- A device that leaves Forward scope is absent from the in-scope upsert rows.
  Older releases therefore could add `forward-out-of-scope` without revisiting
  and removing the device's previous managed include-tag assignment.

Checks (UI):

- On the sync detail page, click **Scope Reconciliation**. It runs the same
  check live and shows the counts — in-scope (collected), tagged-but-backfilled,
  imported SNMP endpoints, and out-of-scope (orphans) — with a sample of the
  orphan names. Endpoint-imported devices are protected using the source's
  endpoint scope settings; exclude tags always apply, while include tags apply
  only when **Scope SNMP Endpoints by Include Tags** is enabled. The **Prune
  orphans** button on that page queues a background job to delete them
  (confirmation first); watch the sync's **Jobs** tab for the result. It runs as
  a job because deleting many devices cascades to their interfaces and IPs and
  would otherwise exceed an HTTP gateway timeout on large fabrics.

Checks (CLI):

- Or run the read-only reconciliation audit:

  ```
  python manage.py forward_device_scope_reconciliation_audit --sync-name "<sync_name>"
  ```

  It reports `forward_in_scope_completed`, `forward_in_scope_endpoints`,
  `netbox_out_of_scope` (stale leftovers), and `netbox_present_backfilled`
  (tagged but not collected this snapshot), with capped name samples. Add
  `--fail-on-drift` for monitoring.

Remediation:

- Upgrade to 2.6.0. If upgrading from 2.5.10 or earlier, run **Publish Bundled
  Queries** with overwrite enabled; 2.5.11 already has the required query set.
  On the Forward source, enable **Scope SNMP Endpoints by Include Tags** and keep
  **Apply Device Scope Tags** enabled when matching Forward tags should be
  visible in NetBox. Run **Preview Dependencies** before the next sync; resolve
  any endpoint-scope bypass warning before applying.
- Leave **Import Generic SNMP Endpoints as Devices** off unless generic MIB-2
  endpoints are a required inventory source. Re-run Scope Reconciliation after
  the corrected sync; old generic endpoints and CIMCs are existing NetBox rows
  and require reviewed orphan pruning.
- Run **Reconcile device scope tags**. With **Apply Device Scope Tags** enabled,
  it removes stale configured include-tag assignments from the current
  out-of-scope set while preserving unrelated tags; it does not delete devices.
  A zero-row or failed Forward scope changes no assignments. Shared managed tags
  are supported through per-sync claims; the assignment remains until the last
  current claim is released.
- Use the **Prune orphans** button on the Scope Reconciliation page, or the CLI:

  In 2.6 this is intentionally manual-only. Upgrade and runtime normalization
  remove old automatic-prune state.

  ```
  python manage.py forward_device_scope_reconciliation_audit \
    --sync-name "<sync_name>" --prune-orphans          # dry run: reports count
  python manage.py forward_device_scope_reconciliation_audit \
    --sync-name "<sync_name>" --prune-orphans --apply  # deletes the orphans
  ```

  Only devices not tagged in the Forward result are removed; tagged-but-backfilled
  devices are kept. Deletion cascades to each device's interfaces and IPs, so
  review the dry run first.
- After the corrected sync, filter Device Types to rows with zero devices and
  manually remove only the obsolete volatile console-server types. Scope prune
  deliberately does not delete shared DeviceType metadata.

## Large DLM Vulnerability Ingestion-Issue Count

Symptoms:

- Most ingestion issues say a device could not be found for DLM device
  software or vulnerability association.
- Vulnerability query row counts are much larger than the scoped device count.
- Hardware notices show duplicate apply attempts or raw-model lookup skips.

Cause and remediation:

- Publish the bundled DLM queries with overwrite enabled. Device-derived DLM
  maps now receive the resolved device-name scope, and Vulnerability has a
  second local device-name filter.
- Enable only the alias hardware-notice map when alias-aware DeviceType maps are
  active. Health warns when both base and alias notice maps are enabled, and the
  registry now collapses the base map when the alias map is present.
- Missing DLM parent devices are dependency skips. Detail is capped and followed
  by one summary issue, so one parent mismatch cannot create thousands of issue
  records.

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
on the ingestion detail page. Both downloads include linked ingestion and job
context so the failure can be investigated without scraping the UI.

Also check the sync `Health` tab before collecting logs. It is read-only and is
intended to answer the common first-pass questions: whether the source is marked
ready, whether enabled maps are diff-capable, whether optional data-file maps
are active, whether a baseline-ready ingestion exists, and whether the latest
sync run recommends waiting, retrying, requeueing merge, or exporting a
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

Each poll includes the sync and ingestion status, one native branch, stage and
merge jobs, change and blocker counts, and ownership reconciliation state. The
command is read-only: it does not requeue staging or merge while it watches.

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
new high-volume warnings that may indicate a regression. The audit includes the
selected ingestion stage and merge jobs. It reads live `job.log_entries` before
final serialization and the stored `job.data["logs"]` representation after
completion.

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
