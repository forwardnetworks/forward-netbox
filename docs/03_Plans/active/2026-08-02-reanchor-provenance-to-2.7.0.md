# Re-anchor Release Provenance To 2.7.0

## Goal

Advance the provenance anchor to the release that just shipped, so the reviewed
commit range starts short again instead of growing until GitHub expires a run.

## Contract

- `verify_release_provenance.py` walks from `v2.7.0`, not `v2.6.10`.
- The recorded post-release doc commit is the actual first commit after the tag.
- No control is removed, relaxed, or made advisory.

## Constraints

- The anchor is pinned in more places than the constant: both the harness check
  and the harness tests must pass, because they disagree about which they read.
- `release.yml` stays tag-triggered; Trusted Publishing scoping is untouched.

## Touched Surfaces

- `scripts/verify_release_provenance.py` - `PRIOR_RELEASE_TAG`,
  `PRIOR_POST_RELEASE_DOC_COMMIT`
- `scripts/check_harness.py` - three assertions
- `.github/workflows/release.yml` - the tag fetch
- `scripts/tests/test_check_harness.py` - two fixtures
- `scripts/tests/test_tasks.py` - one assertion
- This plan.

## Approach

`_require_merged_main_pr` requires every first-parent commit from the anchor to
still have successful main-push `ci.yml` and `codeql.yml` runs. GitHub expires
runs, so a stale anchor is a slow failure: the range only grows, and one
expiry burns a release at the tag. That is what happened to `v2.6.10`, whose
anchor had been left at `v2.5.11` until the chain reached 42 commits.

Advancing is a post-release step. `release.py --auto-finish` performs it;
hand-tagging skips it, which is why it had not been done since `2.5.11`.

`PRIOR_POST_RELEASE_DOC_COMMIT` must equal the first first-parent commit after
the tag, and it is checked as `lineage[0]`. That commit is `bbebcaa`, the
promotion of `v2.7.0` - which is why the promotion had to land first. Verified
directly: `git rev-list --first-parent v2.7.0..HEAD | tail -1` returns exactly
that commit.

A blanket substitution also rewrote a docstring recording which release each
historical failure mode burned, turning a true statement about `v2.6.10` into a
false one about `v2.7.0`. Reverted; the diff is nine lines, all of them the
intended pins.

## Validation

- `invoke harness-check` and `invoke harness-test` both pass. Both are needed:
  a previous re-anchor passed the check while three test fixtures still carried
  the old value.
- The reviewed range drops from 8 commits to 0 at the anchor, so the next
  release starts from a chain whose runs cannot already have expired.

## Rollback

Revert. The anchor returns to `v2.6.10` and the range resumes growing.

## Decision Log

- 2026-08-02: Anchored to `v2.7.0` rather than skipping a cycle. The failure
  mode is progressive, so deferring it makes the next release more likely to
  burn, not less.

## Open

- This is still a manual step after a hand-tagged release. Until releases go
  through `--auto-finish`, the anchor will keep going stale.
