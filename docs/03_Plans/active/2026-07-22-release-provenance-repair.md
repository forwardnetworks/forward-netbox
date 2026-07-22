# Release Provenance Repair

## Scope

Correct the prior-release documentation bridge used by tagged-release
provenance validation. The previous commit identifier was not present in the
repository history, so the 2.6.0 publication workflow rejected an otherwise
valid protected-main release.

## Evidence

- The actual post-2.5.11 documentation bridge is `f9a8420a8bcc2d3afe338d0435a17df9e2bc01d0`.
- The change is limited to the provenance validator constant.
- Required CI, CodeQL, sensitive-content scanning, and tagged release
  validation must pass before retrying publication.

## Completion

After the protected PR merges, recreate the failed `v2.6.0` tag from the
validated main commit and verify identical GitHub and PyPI artifacts.
