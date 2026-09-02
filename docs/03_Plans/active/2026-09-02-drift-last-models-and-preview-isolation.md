# The last two drift models, and a preview that survives one model failing

## Goal

Measure `dcim.virtualchassis`, `netbox_dlm.inventoryitemsoftware` and
`netbox_dlm.inventoryitemroleplatform`, so every fetched model has a
comparison; and stop one model's comparison failure from taking the whole
dependency preview down.

## Why

After slice nine, three fetched models still reported the workload upper
bound: `virtualchassis` because the bulk path declined to answer, and the two
DLM inventory-item models because slice six left their chains unaudited. And
`2026-08-20-preview-runner-priming-contract.md` recorded that one missing
attribute killed the whole preview - every model "Not measured", the report
empty - and that per-model isolation would have turned that outage into a
single cell.

## Constraints

- `virtualchassis` is compared through its ADAPTER, because that is the
  production path: syncs run in a branch, and the bulk path defers to
  `apply_dcim_virtualchassis` whenever one is active.
- The leaf rule for the DLM pair: a software version the row would create is
  `softwareversion`'s drift; the inventory-item row is an update if it exists
  and a create if not.
- A job timeout is never caught by the isolation. The worker is being torn
  down; swallowing it would let the job look finished.
- The recorded error is an exception NAME, never a message.

## Touched Surfaces

- `sync_device.py` - `preview` on `apply_dcim_virtualchassis`.
- `sync_dlm.py` - `preview` on the two inventory-item applies; the
  role-platform ensure returns `(None, None)` for an absent platform instead of
  matching the role's mapping to another platform; per-key outcomes beside the
  cache.
- `drift_comparison.py` - `_compare_dcim_virtualchassis`; the DLM registry.
- `views.py` - `_compare_rows_by_model`, `comparison_error` on the summary.
- `drift_report.py` and the drift report template - the cell says why.
- The dependency preview page header: "Dependency Preview", the one place it
  was still called a dry run.

## Approach

The virtualchassis adapter classifies row by row: an absent chassis is a
create outright (the membership cannot exist yet), a member whose chassis or
position differs is an update, a member in place takes the chassis's own
upsert verdict, and the one direct ORM write - the membership update - is
answered before rather than shimmed. The DLM chain needed one guard, in the
ensure: an absent platform under preview must not let the role-keyed mapping
lookup match the role's mapping to some other platform - the same
absent-parent trap the routing and ACI slices hit.

The preview loop is factored into `_compare_rows_by_model`, which catches per
model, records the exception name, logs with traceback, and continues.

## Validation

`test_virtualchassis_drift_comparison.py`, `test_dlm_inventoryitem_drift_
comparison.py`, `test_preview_isolation.py`; `test_empty_comparison_is_not_a_
measurement` flips its virtualchassis assertion to measured. Adjacent:
`test_dlm_drift_comparison`, `test_dlm_integration`, `test_drift_report`,
`test_sync_device`. Full Django suite.

## Rollback

Revert. The three models return to the upper bound; one failing comparison
again fails the preview.

## Decision Log

- **The adapter is the comparison for virtualchassis**, not a preview mode on
  the two-phase bulk path. The bulk path's answer would only ever describe a
  branch-less run, which production never is.
- **Outcomes beside the cache, not in it.** Two callers unpack the cached
  `(platform, mapping)` pair; changing its shape for one preview flag is the
  kind of edit that breaks the apply to help the preview.

## Open

- Nothing.
