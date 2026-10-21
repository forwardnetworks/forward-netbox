# Say what each drift row means and what to do

## Goal

A customer read the drift table (Forward rows 3,862, pending removals 29 with "+12 already removed", In sync "No") and could not tell what any figure counted, why a model was out of sync, or what to do about it. The columns have no help, the only explanations are tooltips on two of them, and a "No" or "Not measured" badge gives no reason. Add a plain description of every column and, on every row that needs one, what it means and what to do.

## Constraints

- No change to any figure, to how drift is computed, or to what a sync does. Text only, derived from the row's own fields.
- Only statements the row's fields support. The row says how many rows are new or differ and how many removals are pending; it does not say whether a removal will happen, because that follows the sync's removal rules, so the text says so and points to where those are shown.
- Counts only; no names. The explanation travels with the drift report, so wherever that is exported it is counts-only text too.

## Touched Surfaces

- `forward_netbox/utilities/drift_explanations.py` (new): the column descriptions and `explain_drift_row`.
- `forward_netbox/utilities/drift_report.py`: each row gains `meaning` and `action`.
- `forward_netbox/templates/forward_netbox/forwardsync_drift_report.html`: a "How to read this table" note and a "What it means" column.
- `forward_netbox/tests/test_drift_explanations.py`, `docs/01_User_Guide/operations.md`.

## Approach

`explain_drift_row` turns a row's fields into a meaning and an action, and says only what the figures are. A row in sync says nothing. A row with no row-by-row comparison says so and that it is how the map is built (it has no difference path, or its comparison is shown on another row for the model). A row whose comparison failed says so and to run Preview Dependencies again. A row with no exact comparison says its figures are an estimate. A row out of sync says how many rows are counted as new or different and how many removals are counted, not checked row by row against NetBox, and that whether a removal happens follows the sync's removal rules; the already-removed line is "more removals than this run stages, for reasons such as". The device row carries one extra sentence about what Forward rows counts. The text promises nothing about what a sync will do, gives no cause for a removal, and does not call a count the most or least a sync will stage. Two audits rejected earlier drafts for sentences the code contradicts (that the next sync creates or updates every counted row, that a removal is a row NetBox holds that Forward dropped, that "already removed" means applied earlier, that device removals are "mostly" out-of-scope devices, that a figure is "the most the next sync would stage"); the figures are clamped between what Forward declares and what the durable state stages, and a sync can stage removals they do not include.

## Validation

- `invoke test-isolated --test-label "forward_netbox.tests.test_drift_explanations forward_netbox.tests.test_drift_coverage_is_explained forward_netbox.tests.test_drift_rows_are_distinguishable forward_netbox.tests.test_health"`, then `pre-commit run --all-files`, `invoke harness-check`, `scripts/check_operator_text.py` and the script tests.
- Tests: an in-sync row has no text; each not-compared reason is explained; counts and singular and plural are right; the already-removed sentence appears only when there are some; the device row always carries the denominator sentence; every row of a computed report carries the fields; the page renders the note and the column; no figure changes.

## Rollback

Revert. The table goes back to its tooltips.

## Decision Log

- The text states what the figures say and where to look, not what will happen to a removal, because that depends on settings the row does not carry.
- One module holds the wording, so the Health page, the bundle and the drift table can share the same phrases for the same idea.
