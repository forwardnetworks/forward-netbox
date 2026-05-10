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
- If the selected model is `dcim.module`, enable the `Forward Modules` NQE Map
  or disable the `dcim.module` model on the sync. The native module path is beta
  and its built-in map is intentionally disabled by default.
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
- Re-run the sync (and merge if needed) after increasing timeout.
- Review `core/jobs` plus ingestion issues for the matching timestamp window.

Example NetBox configuration:

```python
RQ_DEFAULT_TIMEOUT = 7200
```

Set the final value according to your environment, worker supervision policy,
and how long a trusted large baseline is allowed to run.

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

## Device Shard Fails With `vc_position`

Symptoms:

- The sync appears to progress, but an ingestion part for `dcim.device` or
  `dcim.virtualchassis` records a failure similar to `A device assigned to a
  virtual chassis must have its position defined`.
- Routing, cabling, IP, or other downstream models appear empty or incomplete
  because the base device/virtual-chassis shard did not finish cleanly.

Checks:

- Confirm the `Forward Virtual Chassis` map emits `vc_position`.
- If the map is bound to a Forward Org Repository `query_path`, upgrading the
  NetBox plugin does not rewrite the already-published Forward query. Use the
  native NQE map bulk edit workflow and select `Publish bundled queries to Org
  Repository and bind selected maps` with `Overwrite existing repository
  queries` enabled, or restore the affected map to bundled raw query text.
- Re-run validation or a sync after the map has been refreshed. Current plugin
  versions fail stale virtual-chassis query output during preflight instead of
  allowing a positionless VC assignment to surface later as a device save
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
