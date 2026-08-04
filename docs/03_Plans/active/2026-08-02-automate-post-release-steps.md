# Automate The Post-Release Steps

## Goal

Make a successful release open its own close-out work, instead of relying on
somebody remembering three steps that fail silently when skipped.

## Contract

- A successful `--finish` opens the next `.dev0` as a pull request.
- The anchor follow-up is printed with the exact commands and the released tag.
- Nothing is guessed: no hash is written before it exists.

## Constraints

- The tag-only publish trigger is untouched.
- `main` is protected, so post-release work lands as a pull request, never a
  direct push.
- The four version surfaces move together.

## Touched Surfaces

- `scripts/release.py` - `stage_post_release`, `_next_patch_version`, and the
  call at the end of `stage_finish`
- `scripts/tests/test_release_flow_post_release.py` - new
- This plan.

## Approach

Three steps follow a release, and each has been skipped at least once because
nothing failed at the time:

1. promote the shipped release in the compatibility table - already automated
   in `stage_finish`, before the tag;
2. move `main` onto the next `.dev0`, so an install from `main` is not
   indistinguishable from the published release. A customer hit exactly this;
3. advance `PRIOR_RELEASE_TAG` and `PRIOR_POST_RELEASE_DOC_COMMIT` - which no
   part of `release.py` has ever done. A stale anchor grows the reviewed commit
   range until GitHub expires a run and burns a release at the tag, which is
   what happened to `v2.6.10`.

Step 2 is now automatic: after the release workflow publishes, `stage_finish`
branches from `origin/main`, applies `stage_open_next`, runs the harness gate
and pushes the branch.

**Step 3 deliberately is not.** `PRIOR_POST_RELEASE_DOC_COMMIT` must equal the
first first-parent commit *after* the tag, and squash-merging mints that hash
only when the pull request lands. Writing it earlier would mean writing a hash
that does not exist. So the exact follow-up is printed, including the
`git rev-list --first-parent <tag>..origin/main | tail -1` that resolves it.

That leaves one manual step, but it is no longer a silent one:
`_check_release_anchor_tracks_current_release` fails the harness while the
anchor disagrees with the release the table calls current. The combination is
what closes the loop - automation for what can be derived, enforcement for what
cannot.

## Validation

- `stage_post_release` is tested against a mocked runner: it opens the next
  `.dev0` through the existing stage rather than editing files, branches from
  `origin/main`, runs the harness gate *before* pushing, and prints a follow-up
  naming both constants and the released tag.
- `_next_patch_version` is tested at a double-digit rollover, so `2.7.9` becomes
  `2.7.10` rather than sorting lexicographically.
- `invoke harness-test`: 264 tests OK.

## Rollback

Revert. A release stops opening its own close-out branch; the harness check
still catches the anchor.

## Decision Log

- 2026-08-02: Printed the anchor step rather than guessing its hash. An
  instruction that is right is worth more than automation that is wrong.
- 2026-08-02: Used `stage_open_next` rather than re-implementing the bump. The
  hand edit is how the fast-baseline pin gets left behind.

## Open

- The bridge constant could be *derived* from the tag rather than pinned, which
  would remove the last manual step. It is pinned as a control asserting that a
  specific reviewed commit follows the tag; the retention walk already requires
  every commit in range to carry successful main-push runs, so the two may
  overlap. Changing it is a deliberate decision about a release control, not a
  cleanup.
