## Goal

Make plugin job logs visible on the core NetBox job log tab by persisting Forward sync log entries into `core.Job.log_entries` instead of only storing them in plugin job data.

## Constraints

- Keep the existing Forward sync logging flow intact.
- Do not add new dependencies.
- Preserve the plugin's existing `job.data` payload for ingestion detail views and summaries.
- Normalize log levels to the core NetBox job log table's supported set.

## Touched Surfaces

- `forward_netbox/jobs.py`
- `forward_netbox/tests/test_jobs.py`

## Approach

Convert Forward sync logger tuples into the core job log entry shape during job-data persistence, then save both `job.data` and `job.log_entries` together. Map plugin `success`/`failure` levels onto core NetBox job levels so the `/core/jobs/<id>/log/` tab can render the entries without raising a badge-color error.

## Validation

- `invoke lint`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox python manage.py test --keepdb forward_netbox.tests.test_jobs`
- Playwright verification of `/core/jobs/52/log/` showing `Log Entries` and the synthetic UI harness message

## Rollback

Remove the `job.log_entries` backfill in `safe_save_job_data()` and restore the prior `job.data`-only persistence if the core job log tab regresses.

## Decision Log

- Chosen: persist into `core.Job.log_entries` so the native NetBox job log page renders the logs customers already expect.
- Rejected: leaving logs only in plugin JSON data, because that keeps the core job log tab empty and shifts troubleshooting back into plugin-specific views.
