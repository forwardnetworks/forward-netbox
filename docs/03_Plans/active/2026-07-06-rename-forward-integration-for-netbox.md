# Rename to "Forward Integration for NetBox"; drop stale Version History

**Date:** 2026-07-06

## Goal
Match the naming convention used across the Forward integration family
("Forward Integration for Dynatrace", "Forward Integration for Kentik"): rename
the display name from the generic "Forward Field Integration" to **Forward
Integration for NetBox**. Also drop the Version History table, which was stale
(stopped at 1.5.0.1) and duplicated the Release Compatibility table.

## Constraints
- Display-only: the plugin machine name (`forward_netbox`), `forward` URL prefix,
  package name, and APIs are unchanged.
- Do not touch the Release Compatibility table rows — `gen_changelog.py` parses
  them and the CHANGELOG-matches-README gate depends on them byte-for-byte. The
  historical `v2.0.5` row that mentions the old name stays as-is (it records
  what that release did).

## Touched Surfaces
- `forward_netbox/__init__.py` — `verbose_name`.
- `forward_netbox/navigation.py` — plugin menu label.
- `forward_netbox/templates/forward_netbox/inc/brand_bar.html` — tooltip + visible
  wordmark span.
- `forward_netbox/tests/test_brand_bar.py` — assertion for the new label.
- `mkdocs.yml` — `site_name`.
- `README.md`, `docs/README.md`, `docs/01_User_Guide/README.md` — title/intro/support
  name usages (not the compat rows); Version History section removed.

## Approach
Targeted rename of the name usages only; a script renamed the READMEs on every
line except compat-table rows (`| \`v…`) and removed the `## Version History`
section. `gen_changelog.py --check` confirms the compat rows are unchanged.

## Validation
`test_brand_bar` (new label renders); `gen_changelog.py --check`; sensitive +
harness + mkdocs.

## Rollback
Revert the rename edits; restore the Version History section from git history.

## Decision Log
- "Forward Integration for NetBox" (matches Dynatrace/Kentik), not "…Field
  Integration" — "Forward field integration" remains the generic category term.
- Dropped Version History rather than refreshing it: it duplicated the Release
  Compatibility table and had drifted out of date; the compat table is the single
  maintained source (and the one the release tooling updates).

## Bundled changes
Renamed the plugin display/brand/site name to **Forward Integration for NetBox**
and removed the stale, duplicate Version History table from the READMEs.
