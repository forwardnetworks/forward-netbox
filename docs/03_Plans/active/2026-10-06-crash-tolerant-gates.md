# Make the dev/test gates survive interpreter crashes

## Goal

Stop an interpreter crash in the NetBox container from failing a gate that takes
over an hour, and undo a mitigation that made the crashes worse.

## Constraints

- Only a killed process (exit 139 or 134, or a container that reports it exited
  that way) is rerun. A failing test or assertion exits 1 and fails at once, and a
  crash on the final attempt still fails the gate.
- Every rerun starts from a clean database. `--keepdb` and a populated volume would
  otherwise carry a crashed migration into the next attempt.
- Dev and test tooling only; nothing in the shipped plugin changes.

## Touched Surfaces

- `tasks.py`: `_retry_after_interpreter_crash` and the stages that boot NetBox
  (isolated tests, the UI stack, `artifact-test`, `artifact-upgrade-test`).
- `scripts/tests/test_tasks.py`.
- `development/Dockerfile` and `development/gc_off_for_migrate.pth` (removed).

## Approach

The NetBox image runs Python 3.14.4. During `manage.py migrate` and test database
setup it crashes at random: container exit 139, `Fatal Python error: Segmentation
fault` often reporting "Garbage-collecting", or a corrupted migration
(`'int' object has no attribute 'state_forwards'`). It failed gates on both release
lines, and the host's kernel log shows a general-protection fault inside the
container's `python3.14`. The host runs a release-candidate kernel
(7.3.0-rc5-1-cachyos-rc).

A shared helper reruns a stage after a crash, resetting the database first, and is
applied to every stage that starts NetBox. `up --wait` only exits 1 when a
container dies, so the 139 is read from its captured output.

## Validation

Fresh-schema `manage.py migrate` runs in the same image, GC mitigation of #529
against stock:

- With the hook (`gc.disable` plus a no-op `gc.collect`): 5 of 7 crashed.
- Stock collector: 0 of 4 crashed.

The hook is therefore removed. The stock interpreter still crashed three times
before the hook existed, which is what the rerun is for. Eight new and existing
script tests pin the behaviour, and the full `invoke ci` gate passes.

## Rollback

Revert this branch. No migration and no persisted state.

## Decision Log

- **Reverted #529 rather than tuning it.** Its validation was four runs; the
  measurement that mattered needed more and showed the opposite.
- **Did not claim a root cause.** The crash is intermittent, appears with and
  without the collector, and coincides with a kernel fault report, so the rerun is
  the fix that can be demonstrated. Booting a stable kernel is the likely cure and
  is the host owner's call.
