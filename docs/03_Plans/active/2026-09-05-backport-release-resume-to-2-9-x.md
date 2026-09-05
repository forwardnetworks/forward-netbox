# Backport the release resume fixes to the 2.9.x lane

## Goal

Give `maint/2.9.x` the two release-tooling fixes that landed on `main` after
3.0.0 - PR liveness (#358) and stage idempotency (#359) - so 2.9.3, the first
release cut through the new maintenance lane, does not re-pay a 45-minute gate
for every failure in a fast bookkeeping step afterwards.

## Why

v3.0.0 took fourteen attempts. Exactly one was a product defect; the rest
failed in bookkeeping stages AFTER the gate, and the stages the driver told
the operator to resume from raised on a second run. The 2.9.2 release plan
carries the same complaint under `## Open`:

> `stage_prepare` is not idempotent and there is no way to resume. [...] any
> failure after prepare can only be retried by resetting the tree and starting
> over.

That is written about this lane, and this lane is about to cut a release.

## Constraints

- **Both fixes were written where `origin/main` and "the branch this release
  comes from" are the same string.** On this lane they are not. Every
  hardcoded `main` in the incoming diff has to become the declared lane, or
  the backport is worse than not doing it - see the Approach.
- `scripts/release_lane.py` is the single declaration and already exists here
  (#360). Nothing new is invented; the incoming code is bent onto it.
- The two commits are taken with `git cherry-pick -x`, so the provenance line
  names the commit each came from.

## Touched Surfaces

- `scripts/release.py` - the two fixes, with `origin/main` replaced by
  `REMOTE_RELEASE_REF` / `RELEASE_BRANCH` at every site
- `scripts/tests/test_release.py` - the incoming tests, the `SimpleNamespace`
  and `unittest.mock` imports they need, and the new lane-pinning class
- `scripts/tests/test_tasks.py`, `tasks.py` - the PyPI fetch moving to
  `http.client`
- `forward_netbox/management/commands/forward_profile_merge.py` - the merge
  profile's `ChangeDiff.save()` instrumentation
- `docs/03_Plans/active/` - the two incoming plans, the 2.9.0/2.9.1 stale
  `## Open` corrections, and this file

## Approach

Cherry-pick both, then fix what a cherry-pick cannot know.

**The silent failure this plan is really about.** `_text_on_main` arrived
hardcoded to `origin/main`. Its whole job is answering "has this bookkeeping
already landed" so a retry is a no-op. Asked of `origin/main` from the 2.9.x
lane it answers NO *forever* - main carries a different series - so every
stage rebuilds and re-pushes, and the idempotency this backport exists to
provide is simply absent. Nothing in the inherited suite would have noticed:
the incoming tests mock the helper out by name.

So it is renamed `_text_on_lane` and reads `REMOTE_RELEASE_REF`. Same for
`_merge_is_live` and `_head_is_merged`, which measure ancestry, and for the
`git fetch` and `starting_branch` fallbacks in the two bookkeeping stages.

## Validation

- `ResumeChecksAskTheLaneNotMainTest`, new here, pins all three helpers to the
  lane ref. **Non-vacuity checked by reverting**: with the three sites put back
  to `origin/main`, 3 of its 4 tests fail. The 4th is the deliberate
  opposite-branch guard (`REMOTE_RELEASE_REF != "origin/main"`) and correctly
  still passes.
- `scripts/tests` in full.
- The full Django suite, because `forward_profile_merge.py` is product code.
- `pre-commit run --all-files`, `check_harness.py --base origin/maint/2.9.x`.

## Rollback

Revert. The lane returns to release tooling that cannot resume, which is the
state 2.9.2 shipped in and complained about.

## Decision Log

- **Backport rather than re-derive.** The two fixes are three days old and
  their tests came with them; rewriting them here would produce a second
  implementation of the same behaviour on a branch that has to stay mergeable.
- **Rename `_text_on_main`, do not just re-point it.** A helper whose name
  says `main` on a branch that releases from `maint/2.9.x` is the next
  person's trap, and this backport is the moment the name became wrong.
- **Pin the lane in tests, not just in code.** The parameterisation is
  invisible to every inherited test, so without this class a later merge from
  `main` could quietly restore `origin/main` and pass.

## Open

- `main` and this lane now carry two copies of these helpers that differ only
  in how they name the branch. That is the same shape as `release_lane.py`
  itself - one file per lane, differing by design - but it is a real merge
  hazard, and a future change to the resume logic has to be made twice.
