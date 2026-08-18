# Post-release bridge for 2.8.3

## Goal

Occupy the first commit on `main` after `v2.8.3` with a documentation-only
change, so the provenance anchor has a bridge commit to point at.

## Why

`PRIOR_POST_RELEASE_DOC_COMMIT` must name a commit that is documentation-only
and that follows the release tag. The anchor cannot advance until such a commit
exists, and the anchor commit itself is not documentation-only - it edits the
verifier, the workflow and the harness - so it cannot be its own bridge.

The slot is claimed by whatever lands first. After `v2.7.0` it was taken by the
promotion commit and could not be reclaimed, which is why this is a deliberate
commit rather than a side effect of the next piece of work.

## Constraints

- Documentation only. A single new file; no code, no configuration, no
  workflow, or the commit cannot serve as the bridge.
- It must be the first commit on `main` after the tag.

## Approach

Write this file and merge it before anything else lands on `main`. Then advance
`PRIOR_RELEASE_TAG` and `PRIOR_POST_RELEASE_DOC_COMMIT` in a separate commit
that names this one.

## Touched Surfaces

This file only. Nothing else may enter this commit.

## What shipped

`v2.8.3` published to PyPI and GitHub on 2026-08-18, tagged at
`a095c5c3ab49b9d39cf8cbe739558d8ba971d10a`.

It carries the three fixes a deployment's 2.8.2 sync produced, the NetBox 4.6.8
uplift, two fixes to this release's own machinery, and one more fix that
arrived mid-release from a fresh field report - a drift report showing `In
sync: Yes` for a model nothing had compared.

## The version number was recovered, not burned

`v2.8.3` was refused twice by the publish workflow's sensitive-content gate and
published on the third attempt under the same number. Nothing was published on
either refusal: `Validate tagged release` failed and the build, PyPI and
GitHub-release jobs were skipped, so no partial artifact ever existed.

Recovering a number rather than moving to the next one is the stated intent for
this repository. It costs a verified ruleset window per attempt -
`version-tag-integrity` blocks tag deletion and has no bypass actors - and each
window was opened against a captured snapshot and closed by diffing every field
back against it.

Both refusals had one cause: the release gate applies a superset pattern feed
that no developer checkout has, so the local gate could not fail on what the
release gate refuses. The second refusal was worse than the first, because the
recovery documentation for the first quoted the offending value verbatim while
explaining it.

What ended it was not more care. `.sensitive-patterns.local.txt` - gitignored,
and named in every message that scanner emits - had never been created. It now
reproduces the refusal locally in under a second, and creating it immediately
surfaced a scanned surface neither refusal had reached: a stale
remote-tracking ref. Contents, paths, ref names and tag names are four
surfaces; the review that produced the first tag covered one and reported the
tree clean.

## Validation

None. A documentation-only commit whose entire purpose is to exist at this
position in the history.

## Rollback

Revert, and the anchor has no bridge to point at until another
documentation-only commit lands.

## Decision Log

- **A dedicated commit rather than the next convenient one.** The slot is
  claimed by whatever lands first and cannot be reclaimed afterwards.
- **The promotion goes in the anchor commit, not here.** It is documentation,
  so it would be a legal bridge, but it belongs with the anchor advance and the
  bridge must stay minimal to be obviously documentation-only.
