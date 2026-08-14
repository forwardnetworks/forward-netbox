# Post-release note for 2.8.0

## Goal

Occupy the post-release documentation bridge for `v2.8.0`.

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

- **The quarantine shipped with the fix that makes it work.** As merged, the
  absence quarantine could never release a device: an absent device loses its
  scope claim on the first run that observes the absence, orphan status is
  derived from live claims, so the streak reset every second run and nothing
  ever reached the threshold. Shipping the feature alone would have delivered a
  permanent no-op that reported nothing held - indistinguishable, from the
  panel, from a prune with nothing to do.
- **Found by a gate, not by review.** The defect survived a careful read of the
  diff and its own 362-line test file, because every streak test called
  `record_device_absence` directly with an explicit pk list. The first test to
  run two consecutive real reconciliations failed immediately. Where a feature
  depends on state carrying across runs, at least one test has to actually run
  it twice.
- **2.8.0, not 2.7.14.** The quarantine changes when a destructive operation
  fires and adds operator-settable source parameters.

## Open

- The first attempt at this release was refused at publish by the
  sensitive-content guard: a redacted identifier appeared in release prose - a
  plan file, a commit message body and a pull-request description - and the
  local pattern feed does not carry it, so no local gate could fail. Nothing was
  published; `main` was returned to its last clean commit and the tranche was
  re-landed through fresh pull requests, which kept pull-request provenance
  intact and let the version number be reused. The original commits survive in
  `refs/pull/199/head` and `refs/pull/200/head`, which only GitHub Support can
  purge.
- The 3-run / 72-hour quarantine defaults are a guess at this deployment's sync
  cadence. They are source parameters, so they are tunable without a release.
- netbox-dlm now declares `max_version = "4.6.99"`, identical to ours. Whichever
  side raises its NetBox ceiling first blocks the other; worth raising upstream
  before either moves to 4.7.
- The anchor advance follows this commit.
