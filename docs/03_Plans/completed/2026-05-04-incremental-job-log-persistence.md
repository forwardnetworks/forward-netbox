## Goal

Make Forward sync log entries appear incrementally on the native NetBox job log page while the job is still running.

## Constraints

- Keep plugin log-data collection intact.
- Preserve the existing core job `log_entries` render path.
- Normalize plugin log levels into NetBox's native job log levels.

## Touched Surfaces

- `forward_netbox/utilities/logging.py`
- `forward_netbox/tests/test_logging.py`
- `forward_netbox/tests/test_jobs.py`

## Approach

Write each plugin log entry into the core job's `log_entries` field at log time, using a normalized level mapping that NetBox's native job log table understands. Keep the final job-data persistence path in place so the plugin's own ingestion UI still has the complete payload at completion.

## Validation

- `invoke lint`
- `docker compose --project-name forward-netbox --project-directory development exec -T netbox python manage.py test --keepdb forward_netbox.tests.test_logging forward_netbox.tests.test_jobs`
- Playwright verification of a running job showing log entries on `/core/jobs/<id>/log/`

## Rollback

Remove the incremental `core.Job.log_entries` persistence from `SyncLogging._log()` if it causes performance or schema issues, leaving the end-of-job save path intact.

## Decision Log

- Chosen: incremental writes to the core job log so operators can troubleshoot during long-running runs.
- Rejected: waiting until job completion, because that leaves the native NetBox log page empty during the period when troubleshooting is most likely needed.
