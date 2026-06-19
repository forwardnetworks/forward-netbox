# Harness Pre-Push Simulation

## Goal

Stop failed-CI rounds (and the failure emails) caused by the push-event harness
gate rejecting a push after it already ran — by validating the same rule locally
before pushing.

## Constraints

- Must match the GitHub push-event behavior, including the new-branch case.
- No change to the existing local/CI harness behavior by default.

## Findings

The local `check_harness` is a no-op on a clean tree: `_local_changed_files`
diffs uncommitted changes, which is empty after committing, so it always passes
and never catches plan-gate violations. The GitHub push check, for a NEW branch
(before SHA is zero), evaluates only the tip commit
(`diff-tree after_sha`), so a high-risk file and its plan file must be in the
SAME commit — which the local check can't see.

## Touched Surfaces

- `scripts/check_harness.py` — add `--base <ref>`:
  `_check_per_commit_plan_lifecycle` walks every commit in `<base>..HEAD` and
  fails any commit that touches a high-risk path without a plan file in that same
  commit.
- `scripts/release.py` — `stage_publish` runs `check_harness --base origin/main`
  before pushing.

## Approach

Per-commit validation guarantees the push passes regardless of how GitHub
computes the diff (tip-only or full range), and surfaces the problem locally
before any CI run is triggered.

## Validation

- `scripts/check_harness.py --base origin/main` passes on a correctly-structured
  branch and fails a synthetic commit that touches a high-risk path without a
  plan.
- Harness tests (`python -m unittest discover -s scripts/tests`).

## Rollback

Remove the `--base` option and the release-script call.

## Decision Log

- Per-commit (not just full-range) check: the new-branch push evaluates the tip
  commit only, so the strict per-commit rule is the safe superset that prevents
  all push-gate surprises.
