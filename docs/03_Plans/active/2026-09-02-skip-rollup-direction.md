# The dependency-skip rollup says which way the dependency points

## Goal

Stop the rolled-up dependency-skip issue from describing a protected delete
as a missing parent, and make ten rows distinguishable from ten-of-many.

## Why

`emit_dependency_skip_issue_summary` hard-coded "their NetBox parent is not
synced yet" and recommended enabling the parent sync for every skip in a
model. A customer's `netbox_dlm.softwareversion` skips were protected
deletes - a surviving `inventoryitemsoftware` row refusing the prune - and the
rollup told him to sync a parent that was already there. The per-row path had
learned the difference in 2.8.x (`dependency_phrase`); the rollup had not.
Carried as open through `2026-08-21-ownership-sweep-quarantine.md`,
`-release-2.8.9.md` and `2026-08-22-release-2.9.0.md`.

## Constraints

- `DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT` and its `> limit` trigger are
  unchanged: at exactly the cap nothing is suppressed, so the cap IS the
  count. The survey's suggestion to emit at `>=` was wrong and is not taken.
- Nothing customer-valued is persisted. The reason token is derived from the
  dependency MODEL name, a schema identifier.

## Touched Surfaces

- `forward_netbox/utilities/sync_reporting.py` - `dependency_skip_direction`,
  `dependency_skip_reason`, per-direction buckets recorded alongside the
  per-model count, a direction-aware rollup, the cap sentence on the last
  per-row issue, `skip_reason` / `skip_direction` in the persisted context.
- `forward_netbox/tests/test_skip_rollup_direction.py`.

## Approach

Bucket each skip by `dependency_is_protecting` as it is recorded, keeping up
to five distinct dependency models per direction. The rollup renders one
sentence per non-empty bucket, each with its own remedy, and persists both
counts. The catalogued reason (`missing-device`, `still-referenced-by-
inventoryitemsoftware`, ...) is derived from the same two attributes rather
than taught to twenty-seven raisers separately; the three that name nothing
persist `dependency-unnamed`.

## Validation

`test_skip_rollup_direction.py`: a protected-delete rollup names the child
and the right remedy and never the wrong one; a missing-parent rollup keeps
its remedy; a mixed model gets one sentence per direction with both counts;
exactly the cap emits no rollup and the last per-row issue says the cap is
here; the reason and direction reach `coalesce_fields`; an unnamed raiser is
recorded honestly. `test_dlm_integration`, `test_issue_diagnosis`,
`test_skip_names_netbox_row`, `test_health`. Full Django suite.

## Rollback

Revert. The rollup returns to one sentence, wrong for half the cases.

## Decision Log

- **Derived, not taught.** The reason vocabulary lived in the aggregated-
  warning callers and never reached the database. Twenty-four raisers already
  name their dependency model on the exception; deriving the reason from that
  closes the gap in one place and makes the three that name nothing visible
  as `dependency-unnamed` rather than silently absent.
- **The cap trigger stays at `>`.** Ten rows at a cap of ten means ten. The
  ambiguity was that the panel could not say so; the last per-row issue now
  does.

## Open

- Nothing.
