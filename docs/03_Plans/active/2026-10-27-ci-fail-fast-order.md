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

## Follow-up: tiered push gate (branch `chore/tiered-pre-push`)

The same four-hour `invoke ci` ran on every push, in the cut and again on the tag push, so one release cost about twelve hours of gate. With the operator's approval of the relaxations:

- `invoke ci-quick` is the stack-free prefix of `ci`; a branch push runs it. `ci` is that prefix plus the stack legs, so it cannot drift from it.
- The release cut still runs the full `ci`.
- A tag push runs the quick tier only when `scripts/gated_tree.py` proves the tagged tree equals the tree the cut gated. The cut records a digest of every tracked file except the release plan (`gated_tree_digest` in the evidence record, rendered into the plan's Release Authorization); the tag push recomputes it from the tagged commit. A mismatch, a missing digest or a missing plan falls back to the full gate.
- `scripts/pre_push_gate.py` chooses the tier and reads the hook's ref lines; `FORWARD_NETBOX_FULL_GATE=1` forces the full gate.
- The hook itself is not tracked, so it becomes a thin wrapper that calls `scripts/pre_push_gate.py` when present and `invoke ci` when not.

What this loses: a regression that only the long suites catch is no longer found at push time but at the cut, which fails fast and costs one cut. Validation: `scripts.tests.test_gated_tree` (19 tests, including real git repos for the digest), `test_tasks` tier tests, `test_release` render tests; not yet exercised end to end on a real tag push.
