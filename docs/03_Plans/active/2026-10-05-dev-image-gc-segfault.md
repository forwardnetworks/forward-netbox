# Keep the cyclic GC off during migrate in the dev image

## Goal

Stop the dev and test image crashing at random while Django migrates. Release
and pre-push gates have been dying, and the only remedy was to run them again.

## Constraints

- Dev and test image only. Nothing in the shipped plugin changes, and the web
  server and workers keep the collector on.
- No change to what the tests assert.
- The image already carries a system `sitecustomize`, which shadows one placed
  in site-packages, so the hook has to be a `.pth` file.

## Touched Surfaces

- `development/Dockerfile`
- `development/gc_off_for_migrate.pth` (new)

## Approach

The NetBox image runs Python 3.14.4. While Django renders migration state for
`manage.py migrate`, and for the test runner's database setup, the cyclic
collector either segfaults (`Fatal Python error: Segmentation fault` with
"Garbage-collecting" at the top of the traceback, container exit 139) or
corrupts a migration (`'int' object has no attribute 'state_forwards'`). Five
such crashes were seen in two days, across three different gates, always during
migration state rendering.

A one-line `.pth` file in the venv's site-packages turns the collector off when
the process is `manage.py migrate` or `manage.py test`. Reference counting still
frees almost everything; only cyclic garbage waits until the process exits.

## Validation

- In the image: the hook disables the collector for `manage.py migrate` and
  leaves it enabled for `manage.py runserver`.
- Four consecutive fresh-database runs of a database-using test all passed with
  no crash lines. Before the change roughly half of the fresh-database
  migrations in the previous two days crashed.
- Full `invoke ci` pre-push gate.

## Rollback

Revert this branch. No migration and no persisted state.

## Decision Log

- **Off for `test` as well as `migrate`.** The test runner migrates inside its
  own process, so keying on `migrate` alone would not cover it. Cyclic garbage
  accumulating over one test run is bounded and the host has ample memory.
- **Fixed in the image, not by retrying.** A retry hides the crash and costs an
  hour per attempt; every remaining release gate would have had the same odds.
