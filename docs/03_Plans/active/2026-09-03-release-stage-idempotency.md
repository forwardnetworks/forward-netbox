# A release stage can be run twice

## Goal

Make the four release stages that cannot be re-entered idempotent, so the
resume advice the driver already prints actually works, and port the last
caller using the HTTP pattern this host stalls on.

## Context

v3.0.0 took fourteen attempts. Exactly one was a product defect; the rest
failed in fast bookkeeping steps *after* the ~45-minute gate. A resume
mechanism already exists - `--finish`, `--authorize`, `--post-release` and
`--anchor` are each independently invocable and `stage_finish_unattended`
prints which one to use - but the stages it names raised on a second run, so
the printed advice did not work and every retry re-paid the whole gate.

## Constraints

- **`stage_post_release`'s clean-slate delete stays.** It deletes its branch on
  failure because inheriting a half-made bridge is how v2.8.3's slot was lost.
  The fix is a pre-check, never resuming a partial branch.
- A candidate row for a **different** version is still a hard error: that is
  what the guard is for.
- No new CLI surface. Once the stages self-detect, the existing per-stage flags
  are the resume mechanism.

## Touched Surfaces

`scripts/release.py` (`insert_release_row`, `version_surface_edits`,
`stage_prepare`, `stage_publish`, `stage_post_release`, `stage_anchor`),
`scripts/tests/test_release.py`, `tasks.py` (`_previous_released_version`),
`scripts/tests/test_tasks.py`,
`forward_netbox/management/commands/forward_profile_merge.py`.

No plugin runtime code, no migrations.

## Approach

**The candidate guard is version-scoped.** It matched the bare
`"| Release candidate;"` marker, so it could not tell this version's row from
another version's - and because the edits are computed before `stage_prepare`'s
`write` check, even a dry run tripped it. A candidate for *this* version now
returns the table unchanged; a different version still raises.

**Version surfaces are matched by shape, not by the outgoing literal.**
`version_surface_edits` required the *old* string exactly once, so a prepare
that died mid-write left the rest unreachable: the retry read the new version
from `pyproject.toml`, looked for it in a file still holding the old one, and
reported "expected exactly one". Patterns mirroring
`check_release_preflight.py::version_surfaces` rewrite whatever is there.
Ambiguity (zero or several literals) is still refused. `stage_prepare` prints
which surfaces it had to move, so the repair is visible.

**`stage_publish` commits only when something is staged.** An unconditional
`git commit` turned "the previous attempt already committed" into a hard
failure, because `git commit` exits non-zero with nothing staged.

**`stage_post_release` and `stage_anchor` ask origin/main first.** Both rebuild
their branch from `origin/main` every run, so re-pushing after a partial
attempt is a non-fast-forward. Each now returns early if its work is already on
`origin/main` (the bridge plan file; `PRIOR_RELEASE_TAG` already naming the
tag) or if its pull request is already open.

**The PyPI fetch speaks `http.client`.** `urllib.request` appends
`Connection: close` inside `do_open` where it cannot be suppressed, and this
host stalls close-mode responses - measured at 1/10 completions against 6/6
with keep-alive. `tasks.py` was the last caller using it.
`scripts/verify_release_provenance.py` already speaks `http.client` for this
reason. PyPI has not been seen stalling; this removes the pattern from the last
place it appears rather than fixing an observed failure. The Forward API client
is unaffected - it uses `httpx`, which does not force `close`.

**The merge profile can see `ChangeDiff.save()`.** Its fixture was CREATE-only
and bulk-safe, so it never entered the per-row upstream fallback - the only
path that reaches `ChangeDiff.save()`. `dcim.region` (a tree model, which
`_is_bulk_safe` always refuses) joins the fixture, and the profiling command
wraps `ChangeDiff.save` in its own `profile_scope` so the cost stops being
hidden inside the opaque `objectchange_apply` bucket. Production is untouched:
the instrumentation lives in the profiling command.

## Validation

- `scripts/tests/test_release.py`: re-inserting the same version is a no-op and
  a different version still raises; a partially bumped tree resumes; a drifted
  surface is repaired and two literals are still refused; publish does not
  re-commit but still commits real changes; post-release and anchor stop when
  their work has landed and still run when it has not.
- **Non-vacuity checked**: 8 of the 10 new cases fail against `scripts/release.py`
  as it shipped in 3.0.0. The 2 that pass are the deliberate opposite-branch
  tests (the ambiguity guard, and publish still committing), which must hold
  both before and after.
- `scripts/tests/test_tasks.py` moved off `urllib.request.urlopen` mocks; with
  the old mocks in place the ported code would have made real network calls and
  passed by accident, so every call site was converted. The two failure-path
  tests no longer sleep through the `(0, 10, 30)` backoff: 80s -> 0.1s.
- The ported fetch was exercised against live PyPI and returned `3.0.0`.
- `python -m pytest scripts/tests -q`, `pre-commit run --all-files`,
  `python3 scripts/check_harness.py --base origin/main`.
- The fixture allocation still sums to the requested volume at 100/1000/5000.

## Rollback

Revert the commit. `scripts/release.py`'s changes are detection-only - every
stage still does the same work when its work is absent - and the `tasks.py`
port swaps one stdlib HTTP client for another behind the same retry loop. The
profiling changes affect a management command that only runs behind
`--i-understand-this-creates-test-data`.

## Decision Log

- **Idempotent stages rather than a `--resume-from` flag.** The flags already
  exist; what failed was the stages behind them. A flag would have left the
  non-idempotent stages able to fail on the retry it was meant to enable.
- **Matched by shape, so drift is repaired rather than refused.** The strict
  literal match is what made a half-written prepare unresumable. Detecting
  drift belongs to `check_version_surfaces`, which runs in the gate; prepare's
  job is to set the version, and it now says which surfaces it moved.
- **A pre-check, not a resumed branch, for the bridge and anchor.** Resuming a
  half-made bridge is the specific failure that cost v2.8.3's slot, so the
  clean-slate delete is left exactly as it is.
- **`ChangeDiff.save` is wrapped in the profiling command, not the merge path.**
  Instrumenting production to answer a profiling question would leave a
  permanent wrapper on a hot path.

## Open

- The measurement itself has not been run: `scripts/run_merge_profile.sh` needs
  exclusive use of the docker stack, and a concurrent run corrupts it and reads
  as a code failure. The fixture and the scope are in place so the question -
  whether branching 1.1.3's `ChangeDiff.save()` saving is measurable on a real
  merge - can be answered in one run. If the fallback turns out to be rare in
  practice, "not measurable on a merge" is the honest answer and should be
  written down rather than pursued.
