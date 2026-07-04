# CI: exempt Dependabot from harness gate; make release publish manual

**Date:** 2026-07-04

## Goal
Two post-2.3.0 CI fixes: (1) Dependabot dependency-bump PRs are permanently
blocked by the harness gate because they cannot include a plan doc; exempt them.
(2) The tag-triggered PyPI publish fails and emails a false failure on every
release tag until Trusted Publishing is configured; make it manual until then.

## Constraints
- Keep the harness gate fully enforced for all human-authored changes — exempt
  only the `dependabot[bot]` actor.
- Do not weaken the release publish itself; only change its trigger so it stops
  auto-firing a guaranteed-failing job.

## Touched Surfaces
- `.github/workflows/ci.yml` — add `if: github.actor != 'dependabot[bot]'` to the
  "Check repository harness" step.
- `.github/workflows/release.yml` — change trigger from `push: tags: ["v*"]` to
  `workflow_dispatch` (manual), with a comment to revert once TP is set up.

## Approach
`github.actor` is `dependabot[bot]` for Dependabot-opened PR runs; the `if:`
skips only the gate step (harness unit tests + tests still run). `release.yml`
becomes manual-dispatch; 2.3.0 was published via twine, and future releases can
run it manually or, once the PyPI Trusted Publisher + `pypi` environment exist,
switch the trigger back to tag-based.

## Validation
`invoke harness-check`; `yamllint` both workflows; confirm the 6 open Dependabot
PRs (#27-#32) go green after rebasing onto the updated base.

## Rollback
Revert the two `if:`/trigger edits. No code or data change.

## Decision Log
- Exempt by actor, not by disabling the gate: dependency bumps are low-risk and
  externally reviewed; human changes still need a plan doc.
- Manual release trigger rather than `continue-on-error` on publish: masking the
  publish failure would also hide real failures once TP is configured.

## Bundled changes
CI no longer blocks Dependabot PRs on the plan-doc harness gate. `release.yml`
was briefly switched to manual, then reverted to tag-triggered (plus manual
dispatch) once the PyPI Trusted Publisher + `pypi` environment were configured
in the same session, so release tags auto-publish via OIDC.

## Update — TP configured
The maintainer configured the PyPI Trusted Publisher and `pypi` environment
during this session, so the manual-only stopgap was reverted immediately:
`release.yml` is tag-triggered again (`push: tags: ["v*"]` + `workflow_dispatch`).
The harness-gate Dependabot exemption stands.

## Update — harness gate scoped to PR events
The actor-only exemption covered Dependabot's PR runs but not the
merge-to-main push (actor there is the merger, so the gate re-flagged the dep
bump with no plan doc and redded main after auto-merge). Scoped the "Check
repository harness" step to `github.event_name == 'pull_request' &&
github.actor != 'dependabot[bot]'`: the plan-doc gate enforces on human PRs
only. Direct pushes stay covered by the local `invoke harness-check` (and its
`--base` per-commit simulation) before pushing. The `if:` is written as a
single ≤80-char line (`github.head_ref != '' && github.actor != …`) because
yamlfmt collapses a folded `>-` block back to one line, which then trips the
yamllint 80-char rule.
