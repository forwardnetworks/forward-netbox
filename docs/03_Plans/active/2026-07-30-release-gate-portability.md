# Release Gate Portability

## Goal

Let the release gate run on a host port other than 8000, and stop CI from
timing out on runs that succeeded.

Both defects were found while releasing 2.6.6, not by design review. Neither
changes what the gate verifies; both change whether the gate can be run at all
without disrupting the operator's own environment or being cancelled a minute
past the finish line.

## Contract

- The release environment a cited command ran under is still checked exactly:
  docker project, postgres data path, worker autoreload, and NetBox version
  must equal the canonical release values.
- `ui-validation` still *requires* the host port pair, since Playwright must be
  told where to connect.
- A host port and a `NETBOX_URL` that disagree are still rejected, wherever
  either appears.
- CI still fails a genuinely hung job rather than running unbounded.

## Constraints

- Do not weaken the tag-only publish trigger or Trusted Publishing scoping.
- Do not remove required status checks; do not disable main or tag rulesets.
- Do not relax which evidence ids a release requires, nor the concreteness
  rules that make an evidence entry count.

## Touched Surfaces

- `scripts/check_release_authorization.py`
- `scripts/tests/test_release_authorization.py`
- `.github/workflows/ci.yml`
- This plan.

## Approach

1. **Host port pair.** `_environment_matches` compared the parsed environment
   for equality with the four release variables, so naming
   `FORWARD_NETBOX_HOST_PORT`/`NETBOX_URL` was a rejection for every evidence
   id except `ui-validation`. Treat the pair as *optional* there instead of
   forbidden, and keep exact equality over every other variable.

   The premise the restriction rested on — recorded in the code as "`invoke ci`
   never binds an HTTP port" — is false. `ci` has `start` among its pre-tasks,
   which brings up the shared Compose project, and both
   `development/docker-compose.yml` and `docker-compose.override.yml` map
   `${FORWARD_NETBOX_HOST_PORT:-8000}:8000`. The rule did not describe the
   command; it pinned every gate run to port 8000. Releases 2.6.0 and 2.6.1
   recorded their gate on `FORWARD_NETBOX_HOST_PORT=8080`, so this was a working
   capability that the 2026-07-27 proportionality change removed.

   This is a loosening of a release control, so state plainly what it does and
   does not give up. The port is host-side only: it changes which port the
   stack is published on, not the image, the plugin, the database, or the code
   under test. `_safe_environment_assignment` already bounds the port to
   1..65535 and constrains `NETBOX_URL` to loopback, and `_rtk_parts` already
   rejects a URL whose port disagrees with `FORWARD_NETBOX_HOST_PORT`. Nothing
   an attacker could express with the pair was previously prevented by
   requiring its absence.

2. **CI timeout.** Raise `.github/workflows/ci.yml` `timeout-minutes` from 45
   to 75. Twenty-five recent runs put successful validation at 34.1–43.8
   minutes; the ceiling sat 1.2 minutes above the observed maximum, and the run
   on the 2.6.6 release commit hit 45m43s and was cancelled. GitHub reports a
   timed-out job as `cancelled`, and `invoke release --finish` correctly
   refuses to tag on a non-success conclusion, so the timeout blocked a release
   whose tests had passed.

## Validation

- `python3 -m unittest discover -s scripts/tests -p 'test_release_authorization.py'`
- `python3 -m unittest discover -s scripts/tests -p 'test_release_preflight.py'`
- `python3 scripts/check_harness.py`
- The next release records `final-tree-full-gate` on a non-8000 port, which
  exercises the change on the path it was written for.

## Rollback

Revert the commit through the normal protected pull-request path. The
authorization module is pure and offline, so a revert restores the previous
behaviour with no migration or state to unwind; evidence already accepted
under the looser rule would need re-recording on port 8000.

## Decision Log

- 2026-07-30: Deliberately *not* done during the 2.6.6 release, even though it
  was blocking that release. Loosening a release control so that one's own
  evidence passes is exactly the change that should not be made under time
  pressure by the party it unblocks. The 2.6.6 gate was run on port 8000
  instead, which required stopping the operator's dev stack.
- 2026-07-30: Kept `require_url=True` for `ui-validation`, but **not** for the
  reason on record. The 2026-07-27 change justified keeping it because "Playwright
  does serve over HTTP". It does — on a port it picks itself. `invoke
  playwright-test` takes no port argument;
  `_run_playwright_in_isolated_runtime` reads
  `FORWARD_NETBOX_PLAYWRIGHT_HOST_PORT`, falls back to an auto-picked free
  loopback port, and sets `NETBOX_URL` for the Playwright child itself. So
  `ui-validation` requires the evidence to name two variables that task never
  reads — the same defect, pointing the other way. Left in place because
  `ui-validation` is optional evidence and blocks nothing today; deciding what
  it should require is a change that deserves its own review rather than being
  folded into this one. Recorded so it is not rediscovered mid-release.
- 2026-07-30: Chose 75 minutes over a smaller bump. The spread of successful
  runs is already ~10 minutes, so a ceiling a few minutes above the maximum
  will keep flaking; 75 leaves genuine headroom while still bounding a hang.

## Evidence

- Successful `ci.yml` validate durations over the last 25 runs: 34.1, 34.2,
  34.5, 37.9, 38.1, 38.2, 39.0, 39.2, 40.0, 40.4, 40.5, 40.6, 40.7, 40.9,
  41.0, 41.5, 41.6, 41.9, 43.8 minutes.
- The 2.6.6 release commit's first `main` run was cancelled at 45m43s against
  `timeout-minutes: 45` and passed on rerun without any code change.
