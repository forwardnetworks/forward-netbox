# Reframe as a field integration (drop "officially maintained")

**Date:** 2026-07-06

## Goal
Correct the support posture: this is a **field integration** built by a Forward
Networks SE, not an officially supported Forward product. Remove the
"officially maintained" / "Production/Stable" claims, matching the sibling
Dynatrace repo's honest "field integration reference" framing.

## Constraints
- Do not touch the Release Compatibility table rows.
- Keep the descriptive product/PyPI name "Forward Integration for NetBox".

## Touched Surfaces
- `README.md` — Status + Support sections reworded to "field integration …
  not an officially supported Forward Networks product, provided as-is, no SLA".
- `docs/README.md` — Support paragraph, same reword.
- `pyproject.toml` — `Development Status :: 5 - Production/Stable` →
  `4 - Beta` (a field integration should not advertise Production/Stable).

## Approach
Straight wording edits + one classifier change; mirrors the Dynatrace repo's
`Support model: field integration reference, not an officially supported
Forward product integration`.

## Validation
`gen_changelog.py --check` (compat rows untouched); sensitive + harness;
`grep -i official` shows only the unrelated `forward-backfilled` tag rows.

## Rollback
Revert the wording + classifier edits.

## Decision Log
- Maintainer (Forward SE) explicitly rejected the "official" framing: it is a
  field/reference integration, provided as-is. Prior GA/"supported product"
  posture (2.3.0) is superseded by this.
- Beta not Production/Stable: aligns the PyPI maturity signal with the
  as-is/no-SLA support model.

## Bundled changes
Reframed the plugin as a field integration (not an officially supported Forward
product); dropped the "officially maintained" wording and the Production/Stable
classifier.
