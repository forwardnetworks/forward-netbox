# Synthetic Scenario And Plan Enforcement Harness

## Goal

Add deterministic synthetic sync scenarios and enforce plan lifecycle checks for future high-risk changes.

## Constraints

- Do not rely on live/customer data for scenario tests.
- Do not change production sync behavior.
- Keep fixture network and snapshot identifiers synthetic and non-numeric.
- Keep plan enforcement usable in both local uncommitted work and GitHub push/PR CI.

## Touched Surfaces

- Synthetic test scenario fixtures.
- Scenario-focused tests.
- Sensitive-content patterns.
- Harness checker.
- Harness checker unit tests.
- Invoke tasks and project knowledge docs.

## Approach

Add reusable synthetic fixtures for branch planning, preflight failure, NQE diffs, and branch overflow retry. Strengthen sensitive-content scanning for quoted identifier values. Extend the harness checker so high-risk file changes require an active or completed plan in the same local diff or GitHub event diff.

Add stdlib unit tests for the harness checker so local changed-file gating, GitHub event fallback behavior, and required plan headings are mechanically protected.

## Validation

- `invoke harness-check`
- `invoke harness-test`
- `invoke sensitive-check`
- `invoke lint`
- `invoke scenario-test`
- `invoke test`
- `invoke check`
- `mkdocs build --strict`

## Rollback

Remove the synthetic scenario fixture/test files, revert sensitive-content regex changes, remove plan-enforcement logic and tests for `scripts/check_harness.py`, and remove the scenario/harness task docs references.

## Decision Log

- Chose synthetic string identifiers instead of numeric fixtures to make committed tests clearly non-customer-derived.
- Kept scenario tests at the planner/runner/executor level rather than requiring live Forward API or UI state.
- Chose stdlib `unittest` for harness script tests to avoid adding a new dependency.
