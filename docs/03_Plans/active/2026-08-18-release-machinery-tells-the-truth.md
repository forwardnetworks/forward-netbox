# Two places the release machinery misreported what it did

## Goal

Stop the preflight calling a skipped check a pass, and stop the post-release
stage abandoning the operator on a branch it created.

## Why

Both surfaced during the 2.8.3 release. Both are the defect this repository
keeps finding in its product, sitting in the tooling that ships it.

### A skipped check printed as passed

The preflight prints five lines, all prefixed `release preflight passed:`.
Several of its checks legitimately decline to run and return a
`skipped (reason)` string, so the release log read

    release preflight passed: evidence base commit skipped (v2.8.3 is already
    tagged)

An operator scanning for `passed` counts five. Four ran. The evidence-base
binding - the check that exists to catch an authorization bound to the wrong
commit - was not evaluated at all, because a tag already existed.

Skipping is correct there and must not block a release. Claiming to have passed
is what has to stop.

The sensitive-pattern parity line is the same shape and worse consequence: it
reports `UNVERIFIED` under a `passed:` prefix, and that specific gap has now
refused two tags.

### A failed stage left the operator on its own branch

`stage_post_release` checks out `release/<next>-post-release`, commits a
`.dev0` bump onto it, then runs `check_harness.py`. That check is unsatisfiable
at that moment by construction - the anchor cannot advance until the pull
request it is about to open has merged - so it fails, every time.

It left the operator standing on that branch with the bump committed. The
working tree was CLEAN, so `git status` showed nothing wrong. The next
`git checkout -b` branched from there, inherited the `.dev0` commit, and the
squash merge folded four version surfaces into the post-release bridge - which
pinned it permanently as unusable for `PRIOR_POST_RELEASE_DOC_COMMIT` and had
to be excused by hash.

A clean `git status` concealing the problem is exactly why this belongs in the
tool rather than in an operator habit.

## Constraints

- A skipped check must not become a failure. Every reason it skips for is
  legitimate and none should stop a release.
- The recovery checkout must not mask the original exception, and must not fail
  the run itself if it cannot switch back.
- The partial branch is not deleted. It holds real work and the operator may
  want it.

## Touched Surfaces

- `scripts/check_release_preflight.py` - outcome word per check
- `scripts/release.py` - `stage_post_release` restores the starting branch
- `scripts/tests/test_release_flow_post_release.py` - four new tests

## Approach

The preflight derives the word from the detail it already returns: `skipped`
when the check declined, `unverified` for the parity gap, `passed` otherwise.
The duplicated word is stripped from the detail so the line reads once.

`stage_post_release` records `git branch --show-current` before it moves, wraps
the checkout through the push, and on any exception checks the operator back
out - `check=False`, so a failed recovery cannot replace the real error - then
re-raises.

## Validation

`scripts/tests/test_release_flow_post_release.py` asserts the branch is
recorded, that the checkout, commit, harness call and push are all inside the
guard, and that the recovery neither masks nor swallows the failure. Preflight
output confirmed by running it.

## Rollback

Revert. The preflight resumes reporting five passes for four checks, and a
failed post-release stage resumes leaving the operator somewhere they did not
choose.

## Decision Log

- **Derive the outcome from the detail, not a new return type.** The checks
  already say what happened in the string they return; a parallel status enum
  would be a second source of truth that can disagree with the message printed
  beside it.
- **`unverified` as its own word.** Folding the parity gap into `skipped`
  understates it: skipped means "did not apply", unverified means "applies and
  was not checked", and that distinction is the one that cost two tags.
- **Restore the branch, keep the work.** Deleting the partial branch on failure
  would destroy a commit the operator may want; leaving them standing on it is
  what caused the harm.

## Follow-up, resolved separately

The claim first written here - that `check_harness.py --base origin/main` is
unsatisfiable by construction inside `stage_post_release` - was wrong, and the
release log says so plainly:

    commit ba4fb396ad changes high-risk paths without a plan file in the same
    commit: forward_netbox/utilities/fast_baseline.py, pyproject.toml

The gate was working. It refused a commit that changed high-risk version
surfaces with no plan file, which is exactly what it is for, and the `.dev0`
bump `stage_open_next` writes never had one. Satisfiable, and never satisfied.

Fixed in the change that makes the stage open the documentation-only bridge
instead, which is the commit a release actually needs next.
