# Enforce a documentation-only post-release bridge

## Goal

Fail the harness while the pinned `PRIOR_RELEASE_TAG` /
`PRIOR_POST_RELEASE_DOC_COMMIT` pairing has a bridge that is not
documentation-only, so a bad bridge is caught while it can still be fixed
rather than at a tag that cannot be moved.

## Contract

- The bridge rule has exactly one owner. The harness loads
  `_is_documentation_path` from `scripts/verify_release_provenance.py` rather
  than restating it; a second copy would drift, and a harness that passes while
  the verifier fails is worse than no harness check.
- The check fails closed. An unreadable diff, a missing anchor constant, and an
  empty bridge are failures, not silence — `all()` is vacuously true on an empty
  sequence and `_git_names` returns nothing when git fails.
- The failure names the disqualifying paths and states that the slot cannot be
  reclaimed, so the message rules out the wrong remedy as well as naming the
  right one.

## Constraints

- This adds a check. No existing release control is relaxed, and the widened
  bridge rule from `#134` is not touched.
- The anchor constants are not moved by this change.
- No customer identifiers.

## Touched Surfaces

- `scripts/check_harness.py`
- `scripts/tests/test_check_harness.py`

## Approach

`_check_post_release_bridge_is_documentation_only` reads both anchor constants
out of `scripts/verify_release_provenance.py`, runs the same
`git diff --name-only <tag> <bridge>` the verifier runs, and applies the
verifier's own path rule to the result. The failure names the exact paths that
disqualified the bridge and states that the slot cannot be reclaimed, because
that is the part operators get wrong: the instinct on seeing this failure is to
re-point the anchor, and the bridge is *defined* as the first first-parent
commit after the tag, so there is nothing to re-point it to.

The gap this closes: `_check_release_anchor_tracks_current_release` asserts only
that the anchor names the release the README calls current. It says nothing
about what the bridge commit contains, which is why `#121` could re-anchor onto
a commit that could never satisfy the verifier, with nothing re-running the
verifier until the next tag.

## Validation

`python -m unittest discover -s scripts/tests -p 'test_check_harness.py'` — 60
tests, OK. Five new cases: a documentation-only bridge passes (and the git
invocation is asserted), a bridge carrying plugin code fails, a bridge carrying
a workflow fails, an empty or unreadable diff fails closed, and a missing
`PRIOR_POST_RELEASE_DOC_COMMIT` is reported.

`python scripts/check_harness.py` — the new check passes against the current
pairing (`v2.7.0` / `bbebcaac`), whose bridge changes `CHANGELOG.md`,
`README.md`, `docs/01_User_Guide/README.md`, and `docs/README.md`.

## Rollback

Revert this commit. Nothing else depends on the check; reverting restores the
prior state in which a non-documentation bridge is discovered only at the next
release tag.

## Decision Log

- The rule is imported by file path rather than by `import scripts....`. The
  harness runs both as `python scripts/check_harness.py` (where the repository
  root is not on `sys.path`) and as `scripts.check_harness` under unittest, and
  only a file-path load works in both.
- The check diffs the tag against the pinned commit rather than re-deriving the
  bridge from `rev-list --first-parent`. Using the verifier's exact comparison
  is what guarantees the harness cannot pass while the verifier fails; a
  mis-pinned anchor still fails here, because the diff then carries everything
  between the two commits.
- Ordering is still not enforced at the source. `scripts/release.py` never
  archives plans, so archive-before-promote remains a human step; this check
  makes a wrong bridge loud, not impossible. See Open.

## Open

- `stage_finish` promotes the candidate *before* the tag and never archives, so
  which commit lands in the bridge slot is still decided by hand after the tag.
  Making the close-out own the archival commit — one branch, archive committed
  first, promotion second — would make the bridge correct by construction rather
  than merely checked. That is a change to `stage_post_release`, not to the
  two-PR flow, and it is not made here.
