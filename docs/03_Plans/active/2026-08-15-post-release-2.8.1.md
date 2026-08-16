# Post-release note for 2.8.1

## Goal

Occupy the post-release documentation bridge for `v2.8.1`.

## Constraints

- This commit must be the first on `main` after the tag and must touch only
  documentation. The slot cannot be reclaimed.
- It must not change the anchor; that is the next change.

## Touched Surfaces

- This file only.

## Approach

Documentation only, by construction.

## Validation

`scripts/check_harness.py`, which enforces the documentation-only shape of the
post-release bridge.

## Rollback

None available or needed; the bridge is inert.

## Decision Log

- **The release found a defect in the release before it.** 2.8.0's migration
  `0052` depends on a NetBox 4.6.6 migration while the plugin declares 4.6.5, so
  2.8.0 cannot be installed on 4.6.5 at all. 2.8.0's own gate could not have
  caught it - its upgrade leg seeded a version with no `0052`. The lesson is
  about gate topology rather than about the migration: a defect only reachable
  by installing the NEW migration on the OLD runtime is invisible until the
  following release, so the from side is the part of the gate that finds these.
- **Guarded at author time, not just fixed.** `makemigrations` writes whichever
  core migration is newest on the author's machine, so this reproduces by
  default. A test now fails on any version-specific `dcim` dependency.
- **A per-release override, not an environment variable.** The first attempt at
  seeding the from side elsewhere was global and silently dropped the scenario
  suite's 4.6.5 upgrade coverage.

## Open

- Anyone on NetBox 4.6.5 could not install 2.8.0 and was held at 2.7.13. 2.8.1
  is the first 2.8.x installable there. Worth stating plainly to anyone who
  reported an upgrade failure on that runtime.
- The adapter-only models still have no drift comparison; #206 stays open.
- The anchor advance follows this commit.
