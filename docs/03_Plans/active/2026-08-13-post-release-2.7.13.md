# Post-release note for 2.7.13

## Goal

Occupy the post-release documentation bridge for `v2.7.13`.

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

- **This release removes a regression this repository shipped itself.** The
  removal reconciliation introduced in 2.7.11 applied to every model with no
  exclusions, so a full sync deleted devices unattended - bypassing the
  operator-gated prune flow that exists precisely for that. It survived two
  release gates because every test asserted what the feature should remove and
  none asserted what it must not. The fix is a fail-closed allowlist, and the
  tests now pin the negative space, so adding a gated model to the list fails
  a test.
- **Released same-day, alone.** Every day 2.7.12 remained the installed
  version, each full sync could delete devices. Batching this with other work
  would have traded customer exposure for tidiness.

## Open

- Nothing for 2.7.13. The anchor advance follows this commit.
