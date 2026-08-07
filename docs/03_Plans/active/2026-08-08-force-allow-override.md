# Make the force-allow override reachable on the sync path

## Goal

An operator who force-allows a blocked run should not be blocked by the same
reason on the next run.

## Contract

- The override is still scoped: same policy, same snapshot selector, same
  snapshot. Widening it is not the fix.
- A run that was not force-allowed carries nothing forward.

## Constraints

- `record_plan_validation` CREATES the new `ForwardValidationRun` before
  blocking reasons are evaluated, so anything reading
  `sync.latest_validation_run` at that point sees the run being recorded, whose
  `override_applied` is always False.
- The old coverage called `_blocking_reasons` directly with no current run.
  That path still worked, so the suite passed straight over the dead branch -
  which is why the test here must go through `record_plan_validation`.

## Touched Surfaces

- `forward_netbox/utilities/validation.py`
- `forward_netbox/tests/test_force_allow_override.py`

## Approach

Resolve the override against the most recent force-allowed run EXCLUDING the one
being recorded, via `_previous_override_run`. This is the same exclusion
`_row_shrink_already_accepted` already had to write for itself, applied where it
belonged.

## Validation

- `invoke test-isolated` - full plugin suite, 2007 tests, OK (4 skipped)
- The carry-forward test was confirmed to FAIL against the previous behaviour
  before being accepted; the four negative cases pass either way, as they should

## Rollback

Revert. The override returns to being unreachable on the sync path.

## Decision Log

- **Fixed the helper rather than adding a second workaround.** The row-count
  floor already worked around this with its own previous-run lookup and said so
  in a docstring. A second copy would have been the third place this logic
  lived.
- **Tested through `record_plan_validation`.** The defect only exists because a
  run is created first; a test that does not create one cannot see it. That is
  precisely how it hid.

## Open

- `_row_shrink_already_accepted` still keeps its own lookup. It is scoped to the
  baseline rather than the snapshot, deliberately, so it is not simply the same
  query - collapsing them needs its own change.
