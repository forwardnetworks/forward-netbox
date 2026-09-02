# Release workflow: a check that could never pass

## Goal

The tag-time Dependabot-alert step must stop failing the publish workflow for
a reason no input could change, without giving up the check itself.

## Constraints

- A workflow's `GITHUB_TOKEN` may not read `dependabot/alerts`. The endpoint
  answers `403 Resource not accessible by integration`, and the
  `security-events: read` the workflow already grants does not cover it. No
  permission a workflow can grant itself does.
- The same check must keep failing closed in the local preflight, which runs
  on the operator's own `gh` credential before the tag is pushed.
- Only the inaccessible-credential case may be skipped. A real alert, a 404, a
  5xx, or malformed data still fails.

## Touched Surfaces

- `.github/workflows/release.yml` - the `Check for open Dependabot alerts`
  step.
- `scripts/check_release_preflight.py` - a new `PreflightUnavailable`
  subclass and the narrowed `except` around the alerts read.
- `scripts/tests/test_release_preflight.py` - three cases.

## Approach

`PreflightUnavailable(PreflightError)` means "the check could not run",
distinct from `PreflightError`, which means "the check ran and said no". It is
raised only when the transport error carries BOTH `HTTP 403` and
`not accessible by integration`; everything else keeps the existing behaviour.
Because it subclasses `PreflightError`, every existing caller that does not
name it keeps failing closed.

The workflow catches `PreflightUnavailable` first and prints a `::notice::`
instead of a `::error::`.

## Validation

`scripts/tests/test_release_preflight.py` - 34 tests OK, three of them new:
the inaccessible-credential pair is `PreflightUnavailable`; a plain 403 is
not; a 404 that happens to carry the same phrase is not. `invoke harness-test`
and `invoke harness-check`.

## Rollback

Revert the three files. The step returns to failing on 403, which blocks
publication after the tag is pushed.

## Decision Log

- **Skip, rather than a PAT.** A fine-grained token with `security_events`
  read would keep the check real at tag time, but it is a stored credential to
  mint, scope and rotate for a window the local preflight already covers - the
  minutes between the preflight and the tag push. The release owner chose the
  skip.
- **Narrow on the message pair, not the status.** Matching `403` alone would
  wave through a repository that genuinely refuses the read; matching the
  phrase alone would wave through a 404. Both are pinned by their own test.
- **Found after the tag existed.** v2.9.2 was tagged, the workflow failed at
  its first step, and nothing was built or published. The check ran green in
  the local preflight minutes earlier, so the failure was purely a credential
  difference between the two places it runs.

## Open

- The tag-time window is now unguarded for Dependabot alerts opened between
  the local preflight and the tag. Revisit if a PAT becomes acceptable.
