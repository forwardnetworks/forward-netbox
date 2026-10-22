# Move the locks past certifi and virtualenv advisories

## Goal

Land certifi 2026.10.19, virtualenv 21.14.12 and packaging 26.3 in `poetry.lock` and `requirements-release.txt` so the open Dependabot alerts close and the 3.0.8 release preflight can pass.

## Constraints

- No runtime pin in `pyproject.toml` changes; these are locked transitive and release-tool packages.
- The push that lands the fix is gated by the Dependabot-alert check, which refuses it until the alerts close, so the three alert ids are waived temporarily, as `ACCEPTED_DEPENDABOT_ALERTS` already allows.

## Touched Surfaces

- `poetry.lock`, `requirements-release.txt`.
- `scripts/check_release_preflight.py` (the temporary waiver list; stale 2026-10-01 entries removed).

## Approach

1. Bump the three packages in both lock files; `pip-audit` on the release lock reports no known vulnerabilities.
2. Waive the three alert ids by GHSA id with a dated TEMPORARY comment.
3. After this lands and the alerts read closed, remove the waiver in the release cut.

## Validation

- `pip-audit -r requirements-release.txt --require-hashes`, `pre-commit run --all-files`, `invoke harness-check`, the script tests (433) and the pre-push `invoke ci`.

## Rollback

Revert; the old pins return, and the alerts reopen.

## Decision Log

- Waiving by GHSA id rather than skipping the check keeps every other high or critical alert blocking.
