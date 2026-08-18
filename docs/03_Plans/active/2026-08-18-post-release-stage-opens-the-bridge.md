# The post-release stage should open the bridge, not a .dev0

## Goal

Make `stage_post_release` produce the commit a release actually needs next, and
stop it producing one that fails, strands the operator, and poisons the bridge
slot.

## Why

The stage checked out `release/<next>-post-release`, committed a `.dev0` bump
onto it, and ran `check_harness.py --base origin/main`. It was wrong three ways
at once.

**It failed, every time.** The release log gives the reason exactly:

    commit ba4fb396ad changes high-risk paths without a plan file in the same
    commit: forward_netbox/utilities/fast_baseline.py, pyproject.toml

The gate was doing its job. `stage_open_next` writes four version surfaces, two
of them high-risk, and no plan file. An earlier note in this repository guessed
the check was unsatisfiable by construction; it was not. It was satisfiable and
never satisfied.

**It stranded the operator.** The bump was already committed on a branch this
function created, so the failure left them standing there with a CLEAN working
tree - nothing in `git status`. The next `git checkout -b` inherited the commit.
For `v2.8.3` that reached the post-release bridge, which must be
documentation-only, and disqualified it permanently.

**It was the wrong commit.** What follows a release tag is the bridge:
documentation-only, parented to the release commit. That slot goes to whatever
lands first and cannot be reclaimed. The stage was spending it on a `.dev0`.

## Constraints

- The bridge must contain exactly one documentation file, or it disqualifies
  itself.
- The operator must end where they started, whether the stage succeeds or fails.
- `--open-next` stays. It documents a real incident and is still the right tool
  when someone deliberately opens the next version.

## Touched Surfaces

- `scripts/release.py` - `stage_post_release`, plus `_bridge_plan_text` and
  `_bridge_plan_path`
- `scripts/tests/test_release_flow_post_release.py` - rewritten for the new
  behaviour, six new tests

## Approach

The stage now writes a generated bridge plan, `git add`s that one path, commits,
runs the harness, and pushes - all inside a `try/finally` that checks the
operator back out with `check=False` so a failed restore cannot replace the real
error. The partial branch is kept.

`git add -A` is gone from this path. A commit that must carry one file should
name that file.

### The .dev0 question is narrowed, not settled

`stage_open_next` documents a real incident: a customer installed from `main`
between the release pull request merging and the tag existing, and reported a
version PyPI did not have. The operating rule that `main` carries the RELEASED
version is about a different window - after the tag, when `main` IS that
release.

Both hold. They only appeared to contradict because the stage applied the first
rule at the moment the second one governs. A `.dev0` belongs with the first
substantive change after a release, not as an automatic step that runs while
`main` is still exactly the released tree.

Deciding that properly is a separate change. This one stops the automatic bump.

## Validation

`scripts/tests/test_release_flow_post_release.py`, 15 tests: the stage no longer
calls `stage_open_next`, `--open-next` still exists, the commit carries one plan
file, `git add -A` is absent, the generated plan carries every heading the
harness requires, and its path satisfies the release verifier's OWN
documentation rule - loaded from `verify_release_provenance` rather than
restated, so the two cannot drift.

Full harness suite.

## Rollback

Revert. The stage returns to committing a `.dev0` that its own harness refuses.

## Decision Log

- **Generate the bridge rather than instruct the operator to write one.** Two
  releases needed this commit and one of them got it wrong, at a cost of a
  permanently disqualified bridge and a hash-keyed exception in the verifier.
- **Ask the verifier whether the generated path is documentation.** A restated
  copy of that rule would drift from the check that gates the release, and this
  whole class of defect is two things that were supposed to agree.
- **`try/finally`, not `try/except`.** The operator should be put back on
  success too; only the failure path was ever considered before.

## Open

- Whether `main` should carry a `.dev0` once the first change after a release
  lands. Narrowed above, not decided.
