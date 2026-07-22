# Scrub internal name from test comments; fix release SBOM flag

**Date:** 2026-07-06

## Goal
Post-2.3.1 hygiene: (1) remove an internal name that was reintroduced in two
test comments (`# Regression (<name> 2.3.0)`); (2) fix the release workflow's
CycloneDX SBOM step, which broke the tag-triggered publish.

## Constraints
- No functional/behaviour change; comment + workflow only.
- No version bump (2.3.1 accepted as-is; this only cleans main going forward).

## Touched Surfaces
- `forward_netbox/tests/test_sync.py`, `forward_netbox/tests/test_health.py` —
  reword the two regression comments to drop the name.
- `.github/workflows/release.yml` — SBOM step: `cyclonedx-py … --outfile` →
  `--output-file` (the CLI flag was renamed; the old flag failed the build job,
  which skipped the publish job on the v2.3.1 tag) + `continue-on-error: true`
  so a supplementary SBOM can never gate a publish again.

## Approach
Mechanical edits verified by the full gate; the release workflow is validated on
the next real tag (its publish job uses Trusted Publishing).

## Validation
`invoke harness-check`; `yamllint` / pre-commit on the workflow; the two
regression tests still pass (`test_sync` / `test_health`).

## Rollback
Revert the three edits. No release artifact depends on this.

## Decision Log
- SBOM step made non-blocking: it is supplementary supply-chain metadata, not a
  release gate — a `cyclonedx-py` CLI change should never block publishing.
- No 2.3.2 for this: the reintroduced name is an internal first name and the
  functional 2.3.1 fixes are already published; main is cleaned forward instead.
- The recurrence guard is the `FORWARD_SENSITIVE_PATTERNS` CI secret (still to be
  set by the maintainer) — the content-scan is toothless until it is populated.
