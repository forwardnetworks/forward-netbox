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

## Evidence

- The prior release attempts passed the full plugin and scale suites, then
  timed out during isolated UI-container startup before artifact publication.
- This change removes only the duplicate `invoke ci` call; it does not weaken
  branch or tag protection or skip the required CI status checks.
