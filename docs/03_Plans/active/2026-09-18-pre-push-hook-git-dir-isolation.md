# Pre-push hook GIT_DIR isolation for git-safety test fixtures

## Goal

Stop the pre-push hook's own `invoke ci` run from corrupting the real
repository's branch ref and creating stray objects (commits/tags) with
sensitive-looking content, by isolating the two test fixtures that shell out
to `git` against a throwaway `TemporaryDirectory()`.

## Constraints

- No behavior change to what either test suite actually asserts - this is
  pure test-fixture isolation.
- Must not depend on `--no-verify` or any other hook bypass.

## Touched Surfaces

- `scripts/tests/test_sensitive_content.py`
- `scripts/tests/test_verify_release_provenance.py`

## Approach

Git sets `GIT_DIR`/`GIT_WORK_TREE` in the environment for the duration of
hook execution, so a hook's own child processes reliably operate on the repo
that triggered it. `GIT_DIR`, when present, overrides normal
directory-based discovery - a bare `cwd=repo_root` (or `-C <repo_root>`)
does not clear it. Directly invoked, `invoke ci` has no ambient `GIT_DIR`,
so both fixtures' git helpers worked; inside the pre-push hook, their own
`git init`/`commit`/`tag` calls against a `TemporaryDirectory()` followed
the ambient `GIT_DIR` onto the real repository instead, landing
"Scanner Test"-authored fixture commits (and once, a `v-test` tag whose
message matched the sensitive-content scanner's own network-identifier
pattern) directly on whatever branch was mid-push -
confirmed twice in this session, each time recovered via `git reflog` +
`git branch -f` back onto the real commit, plus deleting the stray tag
object the sensitive-content scanner then flagged on a later push attempt.

Fix: scrub every `GIT_*` environment variable before each fixture's own
`subprocess.run(["git", ...])` call, so it cannot see an ambient `GIT_DIR`
regardless of what process invoked it. Add a regression test that injects a
decoy `GIT_DIR`/`GIT_WORK_TREE` via `mock.patch.dict` and asserts the
fixture's git calls land in its own temp dir, not the decoy.

This ports, byte-for-byte, the fix already reviewed and open as PR #416
against `maint/2.9.x` - `main` forked before that lineage existed for this
file and never received the fix, which is why the corruption reproduced
here on `main`-based work as well.

## Validation

- New regression test in `test_sensitive_content.py` demonstrating the
  ambient-`GIT_DIR` immunity.
- Full `scripts/tests/test_sensitive_content.py` and
  `test_verify_release_provenance.py` suites green.
- `scripts/check_harness.py` passes with this plan file in the same commit.
- Full `invoke ci` green before push.
- The actual proof: this fix must survive being pushed through the real
  pre-push hook without corrupting this branch's ref.

## Rollback

Pure test-fixture change; revert the commit to back out. No production code,
migrations, or schema involved.

## Decision Log

- **2026-09-18** -- Ported PR #416's fix to `main` rather than waiting for
  it to land on `maint/2.9.x` first and forward-porting later: the bug
  reproduced live against `main`-based work twice in one session (two
  separate push attempts, two separate recoveries), actively blocking safe
  continuation of the in-progress forward-sdk migration. The two lineages
  diverged before this file's fix existed, so `main` needed its own copy of
  the same change regardless of PR #416's own merge timing.
