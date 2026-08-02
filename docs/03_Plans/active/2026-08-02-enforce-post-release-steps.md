# Enforce The Post-Release Steps

## Goal

Make skipping a post-release step fail immediately, instead of months later at
a tag.

## Contract

- The provenance anchor names the release the compatibility table calls current.
- Skipping either the anchor advance or the table promotion fails the harness.
- The anchor is not copied by hand into a second file.

## Constraints

- No release control is weakened; this only adds a check.
- The check reads the canonical `README.md` table, which
  `scripts/gen_changelog.py` already treats as the source of truth.

## Touched Surfaces

- `scripts/check_harness.py` - new `_check_release_anchor_tracks_current_release`,
  and the literal anchor copy in `_check_standard_release_tag_flow` reduced to a
  presence check
- `scripts/tests/test_check_harness.py` - coverage
- This plan.

## Approach

Two post-release steps are easy to skip because nothing fails when they are
skipped:

1. advancing `PRIOR_RELEASE_TAG`, and
2. promoting the shipped release in the compatibility table.

Both were skipped repeatedly. The anchor had sat at `v2.5.11` until the reviewed
chain reached 42 commits and an expired run burned `v2.6.10`. The table told
operators that `2.6.9` was current while `2.6.12` and `2.7.0` were both published
and still marked release candidates. `release.py --auto-finish` performs both,
and hand-tagging performs neither.

The two facts are related: after a release, the anchor and the current release
are the same version. Checking them against each other turns two independently
forgettable steps into one enforced invariant, and it fails at the point of
omission rather than at the next tag.

This also deletes a hand-maintained copy. `_check_standard_release_tag_flow`
asserted the literal `PRIOR_RELEASE_TAG = "v2.6.10"`, so the guard against a
stale anchor itself had to be edited by hand whenever the anchor moved - it
could never have caught staleness. It now asserts the constant is present and
lets the derived check verify the value.

**What this does not do.** It does not make releases run through
`--auto-finish`. It makes the omission loud. Automating the release path is the
real fix and remains open.

## Validation

- With the anchor set back to `v2.6.10` against a table naming `v2.7.0` current,
  the check reports exactly that mismatch and names both remedies. Verified by
  temporarily reverting the anchor.
- Passes on the current tree.
- A table with zero or several current releases is reported rather than silently
  taking the first, because the promotion step edits that column and a botched
  edit is the likely way it breaks.

## Rollback

Revert. Both post-release steps go back to being unenforced.

## Decision Log

- 2026-08-02: Derived the anchor check from the release table rather than adding
  a second pinned constant. A pinned guard against staleness is itself stale.

## Open

- Releases still do not run through `release.py --auto-finish`, which is the
  underlying reason these steps get skipped.
