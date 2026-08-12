# Post-release note for 2.7.11

## Goal

Occupy the post-release documentation bridge for `v2.7.11` and record what this
release cycle cost, while the detail is still recoverable.

## Constraints

- This commit must be the first on `main` after the tag and must touch only
  documentation. The slot cannot be reclaimed, and getting it wrong makes every
  later release unverifiable.
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

- **Two tags spent on one tranche.** `v2.7.10` was refused for a three-commit
  lineage where four are required, because the whole tranche was squashed into
  one commit and that removed the control position the rule needs. The tree was
  never at fault - it is the tree that shipped as 2.7.11.
- **The lesson was made mechanical rather than remembered.**
  `scripts/check_release_lineage.py` now runs the structural provenance rules
  that are decidable locally before a tag exists. Every rule the publish
  workflow enforces is otherwise a rule that costs a version number to
  discover, because it only runs once a tag is immutable.
- **The publish run also hit a runner-side TLS failure** -
  `CERTIFICATE_VERIFY_FAILED: self-signed certificate` while reaching the GitHub
  API. That is infrastructure, not repository state, so the same tag was re-run
  and published. Worth distinguishing on sight: a provenance refusal costs a
  version number, a transport failure costs a re-run.

## Open

- Nothing for 2.7.11. The anchor advance follows this commit.
