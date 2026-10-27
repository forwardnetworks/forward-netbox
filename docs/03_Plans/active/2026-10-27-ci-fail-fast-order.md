# Run the cheap gate legs first so a failure does not cost the whole run

## Goal

`invoke ci` takes about four hours and runs three times per release (the push, the cut and the tag). 3.0.9 lost one of those runs because the artifact upgrade leg, which takes minutes, sat last and failed after the long suites had passed. Put every leg that needs no running stack first and the long suites last, shortest first, so a defect surfaces in minutes.

## Constraints

- No leg is removed or weakened; only the order changes.
- The legs keep their own isolated stacks and projects.

## Touched Surfaces

- `tasks.py` (the `ci` task's `pre` list).

## Approach

1. Static checks, then `docs`, `package` and `artifact_upgrade_test` (no stack needed; the upgrade leg installs the artifact `package` builds).
2. Stack bring-up, `check`, the validation-org query audit and the UI test.
3. The long suites last, shortest first: scenario (about 50 min), the retry-scale run (about 20 min), the Django suite (about 2 h 10 min).

## Validation

- `invoke harness-check`, `pre-commit run --all-files`, the script tests, and the pre-push `invoke ci` on this change (the first run with the new order).
- Measured from the 3.0.8 gate log: harness 5 s, scenario 3,107 s, Django 7,852 s, retry-scale 1,232 s.

## Rollback

Revert; the order returns to the previous list.

## Decision Log

- The Django suite goes last because it is the longest and, with the scenario suite, is the least likely to surface a packaging or upgrade defect.
