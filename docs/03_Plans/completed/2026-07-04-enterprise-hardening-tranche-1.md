# Enterprise-hardening tranche 1 (GA-readiness quick wins)

**Date:** 2026-07-04

## Goal
A 7-agent enterprise-readiness assessment rated the plugin **near-GA** with a
strong engineering core but flagged GA-blockers and field-project tells. This
tranche lands the safe, high-value subset: stop a confidentiality leak, remove a
committed customer name, add the standard security/supply-chain hygiene, and
professionalize packaging. Larger items (Trusted Publishing, upgrade testing,
credential-at-rest, support-posture/GA framing) are tracked separately.

## Constraints
- No behavior change to the sync/apply/merge engine. No schema/migration change.
- Do not assert an official support posture or warranty (business/legal decision) —
  leave the README Support Disclaimer wording to the maintainer.
- No release this pass.

## Touched Surfaces
- `mkdocs.yml` + `docs/.pages.yml` + `docs/README.md` — exclude `03_Plans/` and
  `00_Project_Knowledge/` from the built site (internal/field material) and drop
  their landing-page links, so a doc publish cannot leak customer field notes.
- `forward_netbox/tests/test_interface_naming.py` — rename the customer-named
  live-abbreviation test to `test_live_mgmt_abbreviations` and scrub the
  customer-named comment.
- `forward_netbox/utilities/sensitive_content.py` + `tests/test_sensitive_content.py`
  — the scanner now also reads patterns from the `FORWARD_SENSITIVE_PATTERNS` env
  var so CI can block customer identifiers via a secret without committing them
  (the gitignored local file was invisible to CI — how the name slipped through).
- `.github/workflows/ci.yml` — inject the sensitive-pattern secret; add a
  non-blocking `pip-audit` step; add a `makemigrations --check` model-drift guard.
- `.github/dependabot.yml` — new: weekly pip + github-actions updates (the
  enforcing continuous dependency scanner).
- `SECURITY.md` — new: private vulnerability-disclosure policy + supported versions.
- `.github/ISSUE_TEMPLATE/*` + `.github/PULL_REQUEST_TEMPLATE.md` — new.
- `pyproject.toml` — add project URLs + Development-Status/Framework/Python
  classifiers.
- `forward_netbox/jobs.py` — `_resolve_request_user` no longer SILENTLY attributes
  inventory writes to an arbitrary superuser; it logs a warning naming the sync so
  the audit trail is explainable (fallback retained so the run still works).
- Removed the stale `SHA256SUMS` (referenced 0.1.3) and the personal
  `local colima` paths in `scripts/tests/test_tasks.py`; fixed the
  stale wheel-version install examples (0.9.4.1 / 1.7.2 → 2.2.5 + PyPI form).

## Approach
Mechanical, low-risk edits verified with the full gate. The doc exclusion uses
mkdocs `exclude_docs` (build-level, not just nav) and is proven by a `--strict`
build showing zero customer names in `site/`. The scanner env feed is proven by a
unit test. The `pip-audit` step is `continue-on-error` so unfixable upstream
advisories do not gate the build (Dependabot is the enforcing scanner).

## Validation
Full Django suite on 4.6.4; lint (black/flake8/imports/changelog/sensitive);
harness; `mkdocs build --strict` (no excluded dirs in `site/`, no customer names);
`makemigrations --check` clean; env-fed scanner unit test.

## Rollback
Every change is `git revert`-able; docs and metadata are additive; the jobs.py
change only adds a log line around an unchanged fallback.

## Decision Log
- Exclude internal docs from the build (not just nav): nav-hiding still renders and
  ships the pages; `exclude_docs` keeps them out of `site/` entirely.
- Scanner reads a CI secret rather than committing customer names: committing the
  names to block them would itself be the leak.
- Warn (not fail) on the missing-user fallback: failing could break legitimate
  automation with no sync owner; a loud, attributed warning fixes the "silent"
  audit gap without a functional regression.
- Leave the "unsupported / no warranty" disclaimer and `Development Status :: 4 -
  Beta` as-is — bumping to Production/Stable and stating a support SLA is a
  product/legal decision, tracked in the known-architectural-backlog.

## Bundled changes
- Stopped the doc-publish confidentiality leak; scrubbed a committed customer name
  and hardened the scanner to catch recurrences via CI secret.
- Added SECURITY.md, Dependabot, pip-audit, migration-drift guard, issue/PR
  templates, and richer package metadata.
- Made the missing-user sync attribution auditable instead of silent.
- Removed stale provenance/version/personal-path artifacts.
