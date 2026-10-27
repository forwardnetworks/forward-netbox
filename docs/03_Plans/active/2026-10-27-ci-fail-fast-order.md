# Run the cheap gate legs first, and land the lock fixes the push was blocked on

## Goal

`invoke ci` takes about four hours and runs three times per release (the push, the cut and the tag). 3.0.9 lost one of those runs because the artifact upgrade leg, which takes minutes, sat last and failed after the long suites had passed. Put every leg that needs no running stack first and the long suites last, shortest first, so a defect surfaces in minutes.

## Constraints

- Two high Dependabot alerts (virtualenv GHSA-cx6f-vjhg-g3cc, filelock GHSA-v4x8-3wr5-3wq8) block every push to `main` through the release preflight until the fixed locks are on `main`, so this change carries them: `virtualenv` 21.14.13 and `filelock` 4.2.4 in both locks, and the Python floor from 3.10 to 3.12 (NetBox 4.7 and Branching 1.2 already need 3.12, and every filelock fix needs 3.11, so the dev lock cannot otherwise drop its vulnerable entry). Both alert ids are waived in `scripts/check_release_preflight.py`, dated and temporary; remove them once the alerts read closed.

- No leg is removed or weakened; only the order changes.
- The legs keep their own isolated stacks and projects.

## Touched Surfaces

- `tasks.py` (the `ci` task's `pre` list).
- `pyproject.toml` (Python floor and classifiers), `poetry.lock`, `requirements-release.txt`, `scripts/check_release_preflight.py` (the waiver list).

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
