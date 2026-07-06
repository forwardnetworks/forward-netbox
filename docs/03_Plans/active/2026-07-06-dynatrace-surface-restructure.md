# Match the README/UI surface to the sibling integrations

**Date:** 2026-07-06

## Goal
Align this plugin's public surface with the Forward integration family
(cf. Forward Integration for Dynatrace): a short in-NetBox name, a
dynatrace-structured README, and real UI screenshots.

## Constraints
- In-app display is just **Forward** (menu, plugin name, brand bar); the
  product/README/PyPI name stays **Forward Integration for NetBox**.
- Do not touch the Release Compatibility table rows (gen_changelog parses them).
- Screenshots use only synthetic UI-harness data (no customer data).

## Touched Surfaces
- `forward_netbox/__init__.py` — `verbose_name = "Forward"`.
- `forward_netbox/navigation.py` — plugin menu label `"Forward"`.
- `forward_netbox/templates/forward_netbox/inc/brand_bar.html` — wordmark `Forward`.
- `forward_netbox/tests/test_brand_bar.py` — assertion for the `Forward` wordmark.
- `README.md` — dynatrace-style sections (Status, What It Does, What It Does Not
  Do, Screenshots, Architecture) above the existing content.
- `docs/assets/screenshots/{sync-detail,ingestion-diff,drift-report,sources}.png`
  — new, captured from the live plugin (synthetic harness data).

## Approach
In-app name shortened to "Forward" per maintainer note; README top restructured
to mirror the Dynatrace repo; screenshots captured with the existing Playwright
login harness against the seeded UI fixture and cropped to the viewport.

## Validation
`test_brand_bar` (new wordmark); `gen_changelog.py --check` (compat rows
unchanged); sensitive + harness; screenshot links resolve.

## Rollback
Revert the edits; delete `docs/assets/screenshots/`.

## Decision Log
- In-app "Forward" but product name "Forward Integration for NetBox": the menu
  reads cleanly next to NetBox's own nav, while the repo/PyPI keep the
  descriptive name for discoverability.
- Wrote the README restructure by hand (a delegated Codex draft returned empty);
  kept the compat/Support/Quickstart/Features content, only reframing the top.

## Bundled changes
Shortened the in-NetBox name to "Forward", restructured the README to the
Forward-integration house style with a Screenshots section, and added live UI
screenshots.
