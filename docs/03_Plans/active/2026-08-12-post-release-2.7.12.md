# Post-release note for 2.7.12

## Goal

Occupy the post-release documentation bridge for `v2.7.12` and record what this
release cycle cost, while the detail is still recoverable.

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

- **The gate refused this tree three times before it published**, and each
  refusal is worth keeping:
  1. Customer snapshot identifiers written into a test comment as if they were
     documentation. Caught by the preflight sensitive scan before any tag
     existed and before the branch was pushed. Identical in shape to the
     employee name in a fixture that cost `v2.7.7`; the difference is only that
     the scan now runs at preflight rather than at publish.
  2. `Cannot upgrade 2.7.11 from itself` - the gate was run before the version
     surfaces moved. The upgrade leg cannot exercise anything until the tree
     carries the version being released.
  3. `SBOM required-component mismatch: expected 0.7.0, actual 0.8.0` -
     `scripts/validate_sbom.py` carries its own optional-plugin pin, and it
     fails only at the artifact stage, after `invoke ci` has already gone
     green. Two version sites, this and the `pyproject.toml` install cap, were
     absent from the list this repository kept.
- **The lineage was proved before the tag, not after.**
  `scripts/check_release_lineage.py` reported four commits and the correct
  production/release pairing while the tag could still be changed. That check
  exists because `v2.7.10` was refused for a three-commit lineage, and a tag is
  immutable.

## Open

- Nothing for 2.7.12. The anchor advance follows this commit.
