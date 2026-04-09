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

## Sync Fails Before Staging

Symptoms:

- The sync status moves to failed quickly.
- A new ingestion is created with early issues.

Checks:

- Confirm the source has a valid `Network` selected.
- Confirm the selected `Snapshot` is valid for that source network, or leave it at `latestProcessed`.
- Confirm the required NQE maps are enabled.
- Check `Forward Ingestion Issues` for the failing model and error text.

## Merge Records Skipped Changes

Symptoms:

- The merge completes, but the merge log reports skipped changes.
- `Forward Ingestion Issues` contains merge-phase failures.

Checks:

- Review the merge-phase ingestion issues first; they identify the exact model and validation error.
- Confirm the affected built-in or custom NQE map is emitting NetBox-valid values directly.
- Do not patch the value in Python if the intended contract is NetBox-ready NQE output; fix the query instead.

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

# All ingestion issues (filter locally by ingestion/model/message as needed)
curl -sS -H "Authorization: Token ${NETBOX_TOKEN}" \
  "${NETBOX_URL}/api/plugins/forward/ingestion-issues/?limit=0"
```

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
