# GitHub Actions Node 24 Cleanup

## Goal

Remove the GitHub Actions Node.js 20 deprecation warning from CI and make the updated workflow expectations mechanically checked.

## Constraints

- Keep the workflow behavior equivalent to the current validation lane.
- Do not add new CI dependencies.
- Preserve full git history access for sensitive commit-history scanning.

## Touched Surfaces

- GitHub Actions workflow.
- Harness checker.
- Completed plan record.

## Approach

Move official GitHub JavaScript actions to their Node 24-compatible major versions, opt the workflow into Node 24 explicitly, and set least-privilege read permissions for repository contents. Add required workflow fragments to the harness checker so future edits preserve the Node 24-compatible action versions and named harness/scenario steps.

## Validation

- `invoke harness-check`
- `invoke harness-test`
- `invoke sensitive-check`
- `invoke lint`
- `PATH="$PWD/.venv-release/bin:$PATH" invoke docs`

## Rollback

Revert `.github/workflows/ci.yml`, remove the workflow-specific required fragments from `scripts/check_harness.py`, and remove this completed plan.

## Decision Log

- Chose major-version official actions (`actions/checkout@v6`, `actions/setup-python@v6`) to stay on the supported Node 24 action line without pin churn.
- Kept `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24` in the workflow so the repository remains opted into the new runner behavior before GitHub flips the default.
