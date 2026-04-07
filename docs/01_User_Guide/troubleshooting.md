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
