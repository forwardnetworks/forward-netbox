# Audit Dependencies Before The Gate, Not After The Tag

## Goal

Find a dependency advisory before a release gate runs, rather than after a tag
exists.

## Contract

- The release preflight fails when a pinned dependency carries a known advisory.
- A missing `pip-audit` fails closed, never silently passes.
- No existing control is weakened; this only adds a check.

## Constraints

- The preflight is a fast-fail stage ahead of a forty-minute gate; this check
  costs about ten seconds.
- It audits `constraints.txt`, the same file both workflows audit, so it cannot
  disagree with them.

## Touched Surfaces

- `scripts/check_release_preflight.py` - `check_dependency_advisories`, wired
  into `main` and both output modes
- This plan.

## Approach

`pip-audit` ran only in GitHub CI. An advisory can be published against a
version that was clean when it was pinned - nothing in the repository has to
change for it to start failing. `cryptography` CVE-2026-69247 turned every open
pull request red within a minute, on branches that had passed hours earlier.

The pull-request case is loud and recoverable. **The release case is not.**
`release.yml` audits the same file, so an advisory published between the gate
passing and the tag being pushed fails *after* the tag exists. A tag cannot be
moved or reused under the ruleset, so the version number is spent. `v2.6.10` and
`v2.6.11` were both lost to failures first seen at that point, and this is a new
way to reach the same place - one that no amount of care during the release can
prevent, because the trigger is external and arrives on its own schedule.

Ten seconds in the preflight is the cheapest place to find out, and it is the
stage that already exists for exactly this purpose: `check_ui_harness_dependencies`
is there because a missing `npm install` used to be reported only after the full
Django suite had passed.

**Fails closed when the tool is absent.** A preflight that quietly skips its own
check when `pip-audit` is not installed would be worse than not having it, since
it would read as a pass on precisely the machine that cannot verify.

## Validation

- Against `main`, which still pins the vulnerable version, the check reports the
  real advisory:

      cryptography 49.0.0  CVE-2026-69247  50.0.0

- With the fix applied it returns `no known advisories in constraints.txt`.
- With `pip-audit` absent it raises, naming the install command.

All three were exercised directly rather than asserted.

## Rollback

Revert. An advisory is again first noticed by CI, or by the release workflow
after the tag exists.

## Decision Log

- 2026-08-03: Put it in the preflight rather than the gate. The gate is long and
  runs after the decision to release has been made; the preflight is where a
  cheap external precondition belongs.
- 2026-08-03: Fail closed on a missing tool. A skipped security check that
  prints nothing is indistinguishable from a passing one.

## Open

- A scheduled audit would surface an advisory before anyone is mid-release,
  turning a surprise into a waiting bump. `harness-gardening.yml` is an existing
  scheduled workflow to model it on. Not built here.
- `requirements-release.txt` pins the release toolchain and is audited by
  neither workflow nor this check.
