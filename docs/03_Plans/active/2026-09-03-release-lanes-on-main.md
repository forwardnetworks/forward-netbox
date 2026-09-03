# The trunk declares its lane too

## Goal

Forward-port the release-lane change from `maint/2.9.x` so both branches carry
the same tooling. Without it the two lanes diverge in the release path itself -
the part hardest to reason about and most expensive to get wrong - and a future
3.x maintenance branch would have to rediscover the whole change.

## Constraints

- **`main`'s release behaviour must not change.** Its lane names `main`, so
  every gate resolves to exactly the ref it resolved to before. This is a
  refactor with one new refusal, not a change to how a release works.
- **The trunk declares no series.** A maintenance lane exists to carry exactly
  one series; the trunk is where the next one is born. Pinning it would refuse
  the next minor bump and would need editing on every one of them.
- **The two copies stay structurally identical.** Only the declared values may
  differ between branches. Anything else is drift, and drift in the release
  path is what this change exists to prevent.

## Touched Surfaces

- `scripts/release_lane.py`, `scripts/tests/test_release_lane.py` (new here,
  ported)
- `scripts/release.py` - including the four `origin/main` sites that only exist
  on this branch (`_merge_is_live`, `_head_is_merged`, `_text_on_main`)
- `scripts/verify_release_provenance.py` - taken wholesale from the maintenance
  branch with this lane's two anchor constants restored, because the files
  differed by nothing else
- `scripts/check_release_preflight.py`, `scripts/check_release_lineage.py`,
  `scripts/check_harness.py`, `.github/workflows/release.yml`
- `scripts/tests/` - four modules
- `docs/00_Project_Knowledge/release-playbook.md`

## Approach

Identical to `2026-09-03-release-lanes.md` on the maintenance branch; that plan
carries the reasoning and is not repeated here. Three things are specific to
this branch.

### 1. The declaration names the trunk, and no series

```python
LANE = ReleaseLane(branch="main", ruleset="main-release-integrity")
```

`series` is `None`, so `require_version_in_lane` returns immediately. That is
not a weakened check; it is the absence of one that never applied here.

### 2. The stale prior-release tag was worse on this branch

`release.yml` fetched `refs/tags/v2.9.1` as a literal. On the maintenance
branch that was one release stale; **here it was two** - this branch has
released 2.9.2 and 3.0.0 since, and `PRIOR_RELEASE_TAG` says `v3.0.0`. The next
3.0.x release would have failed fetching a tag it no longer needs while missing
the one it does. Both the lane and the prior tag are now read from the scripts.

### 3. One test literal legitimately differs between lanes

The provenance fixtures release `3.0.1` here and `2.9.3` on the maintenance
branch. That is required rather than incidental: the maintenance lane's series
guard would refuse a 3.0.1 fixture, which is the guard working. Every other
difference between the two copies of these files is a declared lane value.

## Validation

- `python -m pytest scripts/tests` - 408 tests on this branch.
- **Live**: `python scripts/verify_release_provenance.py --controls-only`
  against the real `main-release-integrity` ruleset, which is the check that
  refuses a release from an unprotected branch.
- `pre-commit run --all-files`, `python3 scripts/check_harness.py --base
  origin/main`.
- The lane values are asserted directly: this branch's lane must name `main`
  and must declare no series, so a merge that carried the maintenance lane's
  declaration here fails a test rather than a release.

## Rollback

Revert the commit. The lane module is additive, nothing outside the release
tooling imports it, and no shipped code changes - `packages = [{include =
"forward_netbox"}]`, so none of it reaches the wheel.

## Decision Log

- **The verifier was taken wholesale rather than re-edited.** The two branches'
  copies differed by exactly two anchor constants, so copying and restoring
  those is provably equivalent to re-applying the change, and it cannot drift.
- **`release.py` was re-applied rather than copied.** It differs by ~300 lines
  of stage-idempotency work that exists only here, so a copy would have
  reverted it.
- **The trunk's series is `None`, not `"3.0"`.** Pinning it would refuse 3.1.0
  the day it is cut, and the failure would look like a bug in the guard rather
  than a stale declaration.

## Open

- **Nothing has been released through this on either lane.** The controls check
  passes live and the rest is proven by tests; the first real proof is the next
  release from either branch.
- **A third lane would want the ruleset created before its first tag.** There is
  no tooling for that - it is a documented step in the playbook, not a command.
