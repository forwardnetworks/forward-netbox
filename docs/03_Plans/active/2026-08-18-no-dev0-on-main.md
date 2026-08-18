# Take the .dev0 marker back off main

## Goal

Return every version surface on `main` to `2.8.3`, the version that shipped.

## Why

`stage_post_release` ran far enough to write a `2.8.4.dev0` bump across the
four version surfaces before it failed on an unrelated step, and the edits were
left in the working tree. They were then swept into the post-release bridge
commit by a `git add -A` that nobody had checked `git status` before running.

`main` must carry the released version between releases. Customers install this
plugin from source, so a `main` that says `2.8.4.dev0` is a version that was
never gated, never tagged and never published, presented to anyone who clones.

This is a standing contradiction in the tooling rather than a one-off slip:
`scripts/release.py` has a `--open-next` stage whose entire purpose is to put
`.dev0` on `main`, and the operating rule is that it must not be there. The
contradiction is recorded and unresolved; this change follows the rule and does
not resolve the contradiction.

## Constraints

- All four surfaces move together. `forward_netbox/utilities/fast_baseline.py`
  carries a runtime pin that is easy to miss and reverts the first baseline
  from about six minutes to about fifteen hours when it disagrees.
- Documentation that describes historical `.dev0` behaviour is not touched.
  Those sentences are accurate statements about what earlier releases did.

## Touched Surfaces

- `pyproject.toml`
- `forward_netbox/__init__.py`
- `forward_netbox/utilities/fast_baseline.py`
- `forward_netbox/tests/test_runtime_dependency_check.py`

## Approach

Replace the literal in each of the four, then confirm no tracked source or
packaging file mentions `dev0` any more.

## Validation

`check_harness.py`, `pre-commit run --all-files`, and the runtime dependency
test, which asserts the declared version directly.

## Rollback

Revert. `main` returns to advertising a version that does not exist.

## Decision Log

- **Follow the recorded rule, not the script.** The script's `--open-next`
  stage and the no-`.dev0` rule cannot both be right. The rule has a stated
  reason that names a real consequence for people who install from source; the
  stage has automation convenience. Resolving the contradiction properly is
  separate work.
- **Its own commit.** It is a correction to a mistake in the previous commit
  and should be reviewable as exactly that, not folded into the promotion.

## Open

- The `.dev0` contradiction itself. Either `--open-next` goes, or the rule
  does, and neither should be decided as a side effect of a release.
- The bridge commit that carried these edits is no longer documentation-only,
  which makes it unusable as `PRIOR_POST_RELEASE_DOC_COMMIT`. Reverting the
  content here does not fix that: the verifier diffs the release tag against
  the bridge commit itself, and that diff is immutable. Handled separately in
  the anchor change.
