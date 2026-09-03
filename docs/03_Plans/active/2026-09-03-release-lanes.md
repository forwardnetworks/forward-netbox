# A release can come from a branch that is not main

## Goal

Make `maint/2.9.x` releasable. The 4.6 lane exists as a branch but every
release gate asks whether a commit is on `main`, so a `v2.9.3` tag cut there
would be refused - not by one check, but by four independent ones.

## Constraints

- **The gates must keep failing closed.** Parameterising "main" must not turn
  any refusal into a pass. A wrong lane value has to refuse the release, not
  accept a different one.
- **Each lane keeps its own declaration.** The scripts live on the branch they
  release, so the lane is branch-local by construction. That is the property
  that makes a wrong value safe: the tagged commit will not be an ancestor of
  the branch the file names.
- **No new bypass.** The maintenance branch needs the same protection main has,
  or a release from it is a release from an unprotected branch. The verifier
  already refuses that; it must keep refusing it.
- **`main`'s release path must not change behaviour.** Its lane declaration
  names `main`, so every gate resolves exactly as it did before.

## Touched Surfaces

- `scripts/release_lane.py` (new) - the declaration and the series guard
- `scripts/release.py` - checkout, reset, fetch, PR base, harness base
- `scripts/verify_release_provenance.py` - ruleset, PR base ref, workflow head
  branch, ancestry
- `scripts/check_release_preflight.py`, `scripts/check_release_lineage.py`
- `.github/workflows/release.yml`
- `scripts/tests/test_release_lane.py` (new) and the four existing test modules
  that pinned "main"
- `docs/00_Project_Knowledge/release-playbook.md`

## Approach

### 1. Declare the lane, do not derive it

`series` is optional. A maintenance lane exists to carry exactly one series, so
pinning it there is the point; the trunk is where new series are born - it
released 2.9, then 3.0, and will release whatever comes next - so pinning it
would refuse the next minor bump and would have to be edited on every one of
them. `None` means the lane is not confined, and ancestry remains the real gate
either way.


`scripts/release_lane.py` holds one `ReleaseLane` - branch, series, and the
name of the branch ruleset that must protect it - and derives every ref form
from the branch so a slash in `maint/2.9.x` cannot be mishandled by one caller
and not another.

Declared rather than inferred, for the reason `tested_runtime.py` declares
runtime pins: a value read from the current checkout is a value that silently
follows a bad merge, and this one decides which branch's history a release may
come from.

### 2. Substitute, do not abstract

Roughly thirty literal `"main"` and `"origin/main"` uses become the lane's
values. No new indirection beyond the one module: the gates keep their shape,
and a reader who knew where "main" was written still finds the same code doing
the same thing.

Operator-facing messages name the lane rather than saying "main", because a
message that says main while running on `maint/2.9.x` is worse than no message.

### 3. Refuse the wrong series by name

`require_version_in_lane` runs before any stage in the driver and before any
GitHub call in the verifier. The ancestry check would refuse a cross-lane tag
anyway; this turns a merge-base error into a sentence naming both series and
the branch to use instead.

### 4. Derive the prior-release tag in the workflow

The workflow fetched `refs/tags/v2.9.1` as a literal so the bridge check had an
object to resolve. Nothing advanced it when the release anchor moved, so on
this branch it fetched the tag from two releases back while
`PRIOR_RELEASE_TAG` already said `v2.9.2`. It is now read from the verifier,
which is the only place that decides it.

### 5. Protect the branch

`maint-2-9-x-release-integrity` mirrors `main-release-integrity` exactly: no
bypass actors, deletion and non-fast-forward blocked, linear history, and
squash-only through a pull request with conversation resolution.

## Validation

- `python -m pytest scripts/tests` - 382 tests. The four modules that pinned
  "main" now assert against the lane, so they still pin the contract rather
  than being relaxed to accept anything.
- The provenance fixtures move from a 2.6.0 tag to 2.9.3. A fixture releasing
  2.6.0 from the 2.9 maintenance branch was never coherent; now the test says
  what the branch actually does.
- **Live**: `python scripts/verify_release_provenance.py --controls-only` run
  from this branch, against the real ruleset. This is the check that would have
  refused a release from an unprotected branch, and it passes.
- `pre-commit run --all-files`, `python3 scripts/check_harness.py --base
  origin/maint/2.9.x`.

## Rollback

Revert the commit and delete the ruleset. The lane module is additive; nothing
outside the release tooling imports it, and no shipped code changes - `packages
= [{include = "forward_netbox"}]`, so none of this reaches the wheel.

## Decision Log

- **One declaration per branch, not a lane table.** A table mapping series to
  branches would have to be identical on every branch and would be a merge
  conflict every time a lane was added. The scripts are already branch-local;
  the declaration follows them.
- **The series guard is belt and braces, and worth it.** Ancestry already fails
  closed. The guard exists so the failure names the mistake instead of naming
  merge-base.
- **The verifier's fixtures moved series rather than the guard being
  weakened.** Patching the lane out in tests would have left the guard
  untested at exactly the point it matters.
- **No `--lane` flag.** A lane that can be passed on the command line is a lane
  that can be passed wrongly, and the whole point is that the branch decides.

## Open

- **Forward-porting to `main`.** `main` needs the same change so the two lanes
  do not diverge and a future 3.x maintenance branch works. Not done here: this
  branch is the one that needs it, and main's release path works today.
- **Nothing has been released through this yet.** The controls check passes
  live, and the rest is proven by tests; the first real proof is `v2.9.3`.
