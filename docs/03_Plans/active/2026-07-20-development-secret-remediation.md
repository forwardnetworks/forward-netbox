# Development Secret Remediation

## Goal

Remove shared NetBox, PostgreSQL, and Redis development credentials from the
repository and prevent them from returning.

## Constraints

- Preserve the supported NetBox 4.6.5 development and CI workflow.
- Never print or commit credential values.
- Keep generated values local, create-only, and inaccessible to other host users.
- Land the current-tree fix through the protected squash-PR flow before rewriting history.

## Touched Surfaces

- Development Compose configuration and ignored local state.
- CI setup, repository harness checks, tests, and security documentation.

## Approach

1. Remove tracked credential assignments and files.
2. Generate four per-clone mode-0600 files atomically with no replacement path.
3. Mount those values through Docker Compose secrets for NetBox, PostgreSQL, and Redis.
4. Exclude local secrets from Git and Docker build contexts.
5. Reject tracked credential files and assignments in the repository harness.
6. Authenticate the trusted PR scanner's private candidate fetch with its
   read-only workflow token while retaining exact-head verification.
7. Resolve the helper relative to `tasks.py` so NetBox tests that import the
   task module by file path use the same secret contract.
8. Rewrite the affected historical paths only after the protected current-tree fix merges.
9. Ask GitHub Support to purge the affected read-only pull-request refs and cached
   views after every writable branch and tag has been verified clean.

## Validation

- Generator unit tests, including parallel first run and unsafe-file rejection.
- Harness tests and clean full-tree pre-commit run.
- Independent redacted Gitleaks scan of the staged tree.
- Fresh isolated Compose migration, NetBox system check, and Redis authentication.
- Exact GitHub CI and CodeQL checks on the protected pull request.

## Evidence

- Pull request 62 was squash-merged after two independent NetBox 4.6.5 CI runs,
  all 1,224 plugin tests, scenario tests, release-artifact validation, and all
  CodeQL checks passed on the exact candidate SHA.
- Pull request 63 then proved the repaired trusted scanner, neutralized every
  current-tree private-pattern finding, and passed the same two independent
  NetBox 4.6.5 CI runs plus all CodeQL checks before squash merge.
- A mode-0600 rollback bundle was verified before the rewrite. The targeted
  `git-filter-repo` rewrite completed with strict Git object validation and no
  orphaned LFS objects.
- Every writable branch and all 145 version tags were force-updated during a
  controlled maintenance window. Actions and both rulesets were restored from
  exact backups immediately afterward; main again requires all five checks with
  no bypass actors, and version tags again prohibit deletion and force updates.
- A fresh normal clone contains 11 branches and 145 tags. Strict Git validation
  passes, the pre-rewrite and post-rewrite current trees are identical, and a
  redacted Gitleaks history scan reports zero findings across 798 reachable
  commits. A separate private-pattern scan reports zero matches across 4,569
  reachable blobs.
- GitHub reports no forks and no open native secret-scanning alerts. Sixty-two
  immutable pull-request head refs remain for GitHub Support to dereference and
  garbage-collect; no client-side push can modify those server-owned refs.
- The repaired trusted scanner exposed pre-existing customer labels in public
  plans and endpoint-test fixtures. All matches were replaced with neutral
  validation labels, and the local private-pattern scan now reports zero
  findings without weakening or baselining the scanner.

## Rollback

Revert the current-tree commit and restore the protected rulesets if history
maintenance fails. Keep a mode-0600 local Git bundle until GitHub verification
is complete.

## Decision Log

- Environment interpolation was rejected because it keeps shared credentials in
  process environments and does not solve first-clone generation.
- Repository-wide squashing was rejected because targeted path rewriting removes
  the exposure without discarding unrelated commit structure.
