# Forward Log Exporter

## Goal

Add a native, read-only log export surface for Forward ingestion runs so
operators can download the structured sync and merge logs they need for
troubleshooting long-running or failed jobs.

## Outcome

Completed as part of the 0.9.0 release.

## Validation Evidence

- `invoke lint`
- `python manage.py test --keepdb --noinput forward_netbox.tests.test_log_export`
- `invoke ci`

## Constraints

- Keep the export read-only.
- Reuse existing job data and log-entry structures.
- Keep the export attached to the existing ingestion/sync workflow.
- Do not introduce a separate logging model or a new data store.
- Do not commit customer identifiers, network IDs, snapshot IDs, screenshots,
  or credentials in examples, tests, or docs.

## Approach

Exposed a download action on the existing ingestion log surface that returns
structured JSON with:

- ingestion metadata
- sync metadata
- sync-stage job data
- merge-stage job data when present
- normalized log entries and statistics

Also surfaced a compact export action from the sync page that routes operators
to the latest ingestion log export.

## Touched Surfaces

- `forward_netbox/views.py`
- `forward_netbox/templates/forward_netbox/partials/job_logs.html`
- `forward_netbox/templates/forward_netbox/forwardsync.html`
- `forward_netbox/tests/test_log_export.py`
- `forward_netbox/tests/test_sync.py`
- `scripts/playwright_forward_ui.mjs`

## Rollback

Remove the export view, export button, and tests. Existing job logs and
ingestion detail views remain unchanged.

## Decision Log

- Chosen: expose export as a download from the existing ingestion log card so
  operators have a single obvious action.
- Chosen: export both sync-stage and merge-stage data in one JSON bundle so
  troubleshooting does not require scraping the page.
- Rejected: inventing a separate logging model or external log store because
  the job log data already exists in NetBox-native structures.
