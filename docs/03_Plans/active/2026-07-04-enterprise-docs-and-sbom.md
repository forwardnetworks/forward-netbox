# Enterprise docs + SBOM

**Date:** 2026-07-04

## Goal
Close the remaining documentation/supply-chain GA items from the assessment: a
REST API reference, a versioning + deprecation policy, and an SBOM published with
each release.

## Constraints
- Documentation + release-pipeline only; no plugin behavior/schema change.
- The SBOM must NOT land in `dist/` (that directory is uploaded to PyPI).
- No release this pass.

## Touched Surfaces
- `docs/02_Reference/rest-api.md` (+ nav) — the plugin's REST endpoints and custom
  actions.
- `docs/01_User_Guide/versioning.md` (+ nav) — SemVer + a concrete deprecation
  process (announce → keep one minor → remove no earlier than next major).
- `.github/workflows/release.yml` — a CycloneDX SBOM step over the declared runtime
  deps, written to `sbom/` and uploaded as its own artifact (not `dist/`).

## Approach
Docs are derived from the actual `api/urls.py` router + `@action` endpoints. The
SBOM uses `cyclonedx-bom` against a small runtime-requirements list, kept separate
from the PyPI upload path.

## Validation
`mkdocs build --strict` (new pages in nav, no broken links); lint incl. yamllint;
harness.

## Rollback
Docs and a workflow step are additive; revert the commit.

## Decision Log
- Deprecation policy describes the mechanism only; the support/warranty commitment
  stays in the README disclaimer (a maintainer/legal decision, unchanged here).
- SBOM covers declared runtime deps (httpx, netbox-branching, pyzipper); a full
  transitive lockfile-based SBOM can follow once a lockfile exists.

## Bundled changes
- REST API reference + versioning/deprecation policy docs.
- Per-release CycloneDX SBOM artifact.
