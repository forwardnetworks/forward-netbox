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

## Validation

- Generator unit tests, including parallel first run and unsafe-file rejection.
- Harness tests and clean full-tree pre-commit run.
- Independent redacted Gitleaks scan of the staged tree.
- Fresh isolated Compose migration, NetBox system check, and Redis authentication.
- Exact GitHub CI and CodeQL checks on the protected pull request.

## Rollback

Revert the current-tree commit and restore the protected rulesets if history
maintenance fails. Keep a mode-0600 local Git bundle until GitHub verification
is complete.

## Decision Log

- Environment interpolation was rejected because it keeps shared credentials in
  process environments and does not solve first-clone generation.
- Repository-wide squashing was rejected because targeted path rewriting removes
  the exposure without discarding unrelated commit structure.
