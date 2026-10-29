# Bundle: an index from every page row's label to where the bundle holds it

## Goal

A customer quotes a figure by the label on a sync page; the bundle holds it under the code's field name, and a search by the label finds nothing. Ship an index of page label to bundle path inside the bundle, and fail the build when a page gains a row the index does not cover.

## Constraints

- Counts, paths and labels only; no customer data. No migration. No Forward call.

## Touched Surfaces

- `forward_netbox/utilities/bundle_label_index.py` (new): `PAGE_LABEL_PATHS` for the Health, Scope Reconciliation and Drift report pages, and `ui_label_index()`.
- `forward_netbox/views.py`: the bundle carries `ui_label_index`.
- `forward_netbox/tests/test_bundle_label_index.py` (new), `test_log_export.py`.

## Approach

Each row's path was read from its template expression and checked against a real 3.0.9 bundle, then corrected for the keys the export filter renames since (`include_tag_count`, `*_names` (dropped)) and for 3.0.10 additions. Health's `Query parameters` reads a source no code fills in, so that page row always says none were recorded. The Drift report's summary `In sync` row is `drift_report.in_sync`; the per-model column is in `drift_table`. A label repeated on a page maps to a list of paths. Rows that repeat per integration use `<integration>`.

## Validation

- `invoke test-isolated` on the label-index, log-export and redaction suites.
- The guard fails when a page row has no entry and when an entry has no row (checked by removing an entry).
- Not covered: whether a rendered value equals the bundle's value; that needs a populated sync per page.

## Rollback

Revert; the bundle loses `ui_label_index`.

## Decision Log

- An index shipped in the bundle, not only a test: the reader of the file is who needs the lookup.
- Rows inside `{% for %}` loops with labels taken from data are not indexed; their labels are not fixed text.
