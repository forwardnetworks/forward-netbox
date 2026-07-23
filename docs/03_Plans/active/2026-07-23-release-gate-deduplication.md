# Release Gate Deduplication

## Goal

Keep tag publication bounded by using the protected exact-commit CI result
instead of running the full CI fan-out a second time inside the release job.

## Contract

- Main and tag protection continue to require the exact NetBox matrix, CodeQL,
  and trusted sensitive-content checks.
- The release job retains provenance, authorization, sensitive-content, and
  artifact validation before any upload.
- The release job does not repeat the long scale and Playwright suites after
  those protected checks have passed.

## Constraints

- Keep main and tag rulesets enabled.
- Do not remove required status checks or trusted publishing controls.

## Touched Surfaces

- `.github/workflows/release.yml`
- This release-gate plan and its validation evidence.

## Approach

1. Retain the exact-commit protected CI matrix as the test authority.
2. Limit the tag workflow to release-specific checks and artifact publication.

## Validation

- Protected CI, CodeQL, and trusted sensitive-content checks pass on the exact
  merged tree before tag publication.
- The release workflow performs provenance, authorization, dependency, build,
  wheel-install, and publication checks.

## Rollback

Revert the workflow commit through the normal protected pull-request path.

## Decision Log

- 2026-07-23: Removed the duplicate `invoke ci` fan-out after repeated release
  attempts timed out in isolated UI startup after the protected CI passed.

## Evidence

- The prior release attempts passed the full plugin and scale suites, then
  timed out during isolated UI-container startup before artifact publication.
- This change removes only the duplicate `invoke ci` call; it does not weaken
  branch or tag protection or skip the required CI status checks.
