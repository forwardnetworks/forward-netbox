# Bundle: pages and ingestions the index did not reach, and a check that page and bundle agree

## Goal

The label and card index proved every page row had an entry, not that the entry led to the value the page shows. It also skipped the dependency preview and audit pages and any ingestion but the latest. Cover those, and check the values.

## Constraints

- Counts and labels only; no device, bay or tag names. No migration. No Forward call.

## Touched Surfaces

- `forward_netbox/views.py`: `operator_action_jobs.audit_*` stored payloads are reduced to their numbers (an audit's example lists name devices, interfaces and tags, and the sanitizer did not know those keys; this was already leaking in 3.0.9); `latest_dependency_preview.plan_items` and `plan_items_count` (the "Planned Shards" table: model labels and counts), `audit_reports` (each Forward-backed audit's summary figures and table sizes, never rows), `ingestion_pages` (the ingestion page's figures for the newest ten ingestions).
- `forward_netbox/utilities/bundle_label_index.py`: rows and cards for the dependency preview and audits pages.
- `forward_netbox/tests/test_bundle_label_index.py` (`BundleMatchesPagesTest`), `test_log_export.py`.

## Approach

`BundleMatchesPagesTest` populates one sync, renders the Scope Reconciliation, Module Bay Plan, dependency preview, ingestion, Health and Drift report pages, reads each row's cell, and requires the bundle value at the indexed path to appear in it. Numbers must match a whole token (a 4 is not found in a 40) and text must appear in the cell; a path that leads nowhere while the page shows a number fails. Falsy values (rendered as a placeholder), booleans and timestamps (formatted by the page) are skipped, and each page must compare at least three rows so a page that compares nothing fails. The Types page lists type names, which stay out by design; the audits that run locally on each page view are not run in the bundle.

## Validation

- `invoke test-isolated` on the label-index, log-export and redaction suites.
- The value check was shown to fail by pointing two paths at a neighbouring field.
- Not covered: rows whose value is falsy or formatted in the fixture; Health beyond what a bare sync populates; the full Django suite, at the cut.

## Rollback

Revert; the bundle loses the added keys.

## Decision Log

- Audit rows are not exported: they name devices. The summary figures and table sizes answer "how many" and the audit page answers "which".
- `ingestion_pages` holds ten: a customer quotes a recent run, and a longer list grows the file for little gain.
