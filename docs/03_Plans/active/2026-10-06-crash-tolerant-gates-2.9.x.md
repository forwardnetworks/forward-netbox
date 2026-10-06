# Make the 2.9.x dev/test gates survive interpreter crashes

## Goal

Stop an interpreter crash in the NetBox container from failing a gate that takes
over an hour on the 4.6 lane, as it already did twice for the 2.9.14 release.

## Constraints

- Only a killed process (exit 139 or 134, or a container that reports it exited
  that way) is rerun. A failing test or assertion exits 1 and fails at once, and a
  crash on the final attempt still fails the gate.
- Every rerun starts from a clean database, because `--keepdb` and a populated
  volume would otherwise carry a crashed migration into the next attempt.
- Build tooling only; nothing in the shipped plugin changes.

## Touched Surfaces

- `tasks.py`: `_retry_after_interpreter_crash`, applied to the isolated test
  stages, the UI stack boot, `artifact-test` and `artifact-upgrade-test`.
- `scripts/tests/test_tasks.py`.

## Approach

The NetBox image runs Python 3.14.4, which crashes at random during
`manage.py migrate` and test database setup (container exit 139, "Fatal Python
error: Segmentation fault", or a corrupted migration). It cost the 2.9.14 release
gate two attempts on this lane. The same helper that landed on `main` is applied
here; `up --wait` only exits 1 when a container dies, so the 139 is read from its
captured output.

## Validation

- Eight script tests pin the behaviour: a killed process is rerun, a failing test
  is not, a crash on every attempt still fails, and a container that exited 139 is
  recognised from compose output.
- Full `invoke ci` pre-push gate on NetBox 4.6.10.

## Rollback

Revert this branch. No migration and no persisted state.

## Decision Log

- **Retry only; no collector tweaks.** A hook that disabled the garbage collector
  was measured on `main` and crashed more (5 of 7 fresh migrations against 0 of 4
  stock), so this lane takes only the rerun.
- **Same helper as `main`.** The two lanes' tooling should not drift.
