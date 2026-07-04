# CI: scan tracked-file content for sensitive identifiers

**Date:** 2026-07-04

## Goal
Close the gap that let a customer identifier ship in a tracked file twice: CI
ran only `check_sensitive_content.py --all-history`, which scans commit
**messages**, never file **content**. Add a `--git-files` content-scan step so a
customer name in a doc, test, or comment fails CI the same way.

## Constraints
- No new sensitive strings in the repo: the block-list stays in the
  `FORWARD_SENSITIVE_PATTERNS` repo secret (and gitignored local file), never
  committed. `BUILTIN_PATTERNS` cannot name customers.
- No change to scanner semantics — the script already supports `--git-files`
  and `scan_paths`/`tracked_files`; this is pure CI wiring.

## Touched Surfaces
`.github/workflows/ci.yml` — one new step ("Check tracked file content for
sensitive content") after the existing history step, fed by the same secret.

## Approach
Invoke `python scripts/check_sensitive_content.py --git-files` with
`FORWARD_SENSITIVE_PATTERNS` in the env. Blocking (exit 1 fails the job) so a
content leak stops the build. Comment documents that short tokens must use
word-boundary regex (`re:\bADP\b`) to avoid substring false positives
(literal patterns match case-insensitively as substrings — e.g. bare `ADP`
matches `ThreadPool`).

## Validation
`invoke harness-check`; `invoke sensitive-check`; run
`check_sensitive_content.py --git-files` locally against the scrubbed tree
(clean); yamllint the changed workflow.

## Rollback
Delete the added step from `ci.yml`. No code or data change.

## Decision Log
- Wiring, not new matching logic: the script already had `--git-files`; the
  only defect was CI never calling it.
- Kept blocking rather than `continue-on-error`: a content leak is a
  release-stopper, and an empty/unset secret yields no findings (no false
  failures), so enabling it is safe even before the secret is populated.
- Did not add name patterns to `BUILTIN_PATTERNS` — that would re-commit the
  very identifiers being blocked; the secret is the only correct home.

## Bundled changes
CI now scans tracked-file content (not just commit messages) for the
customer-identifier block-list, so a name in a doc/test/comment fails CI.
