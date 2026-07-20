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
- Trust controls must come from the protected base branch or external GitHub
  settings; a candidate cannot choose its own baseline, allowlist, or scanner.

## Touched Surfaces
The scanner and tests, CI/CodeQL workflows, fork-safe trusted PR workflow,
standard release workflow, provenance verifier, and valid CODEOWNERS entries.

## Approach
Scan tracked files plus every post-baseline commit, changed blob, path, ref, and
annotated tag object. Reject binary content unless its current-tree path and
digest are reviewed; accept historical binary exceptions only from an external
secret. For fork PRs, check out only the trusted base, fetch candidate objects
without executing them, scan the candidate tree/history/ref, and publish the
result as an authenticated status on the exact candidate SHA.
Version tags use the standard annotated-tag path from protected `main`. The
release tool verifies live controls before pushing and verifies that the remote
tag peels to the intended commit. The tag-triggered workflow rechecks main
lineage, CI/CodeQL runs, and trusted scanner statuses before PyPI OIDC publish.

## Validation
Scanner and harness regression suites; pre-commit; protected-history scan with
the external baseline and nonempty private-pattern feed; provenance tests for
cross-workflow status forgery and wrong-PR reuse;
fresh hash-locked release-tool installation; byte-identical double builds; and
exact CI and CodeQL workflow runs on the bootstrap commit. Release-tag tests
reject wrong targets, lightweight tags, package-version mismatches, and local
only retry state.
CodeQL then identified two high-severity clear-text logging flows in privileged
release code. The release command runner never logs command arguments or
includes them in errors;
regression tests inject secret-shaped values and prove they remain absent.
Final independent rereview found that the trusted private-pattern status was
not yet required by `main`. The bootstrap permits its own four public statuses;
version authorization requires the authenticated private-pattern status as a
fifth main rule, and the local preflight fail-closes on live repository,
ruleset, PyPI environment, deployment-policy, and Actions SHA-pinning drift.

## Rollback
Revert the bootstrap commit before enabling the dependent main ruleset.

## Decision Log
- Replaced the original message-only history behavior with a tree/blob/ref/tag
  scanner whose trust controls are outside candidate control.
- Kept blocking rather than `continue-on-error`: a content leak is a
  release-stopper. The original implementation tolerated an unset secret; the
  2.6 trust-boundary hardening supersedes that behavior and fails push, trusted
  PR, and release scans unless the private-pattern feed is nonempty.
- Did not add private name patterns to `BUILTIN_PATTERNS` — that would re-commit the
  very identifiers being blocked; the secret is the only correct home.
- During the one-time bootstrap, apply built-in and private patterns to every
  post-baseline changed blob. The inherited main tree contains literals that the
  reviewed 2.6 production PR removes, so the bootstrap does not claim a clean
  full tree. The trusted PR controller scans the complete 2.6 candidate tree
  before it can merge; that candidate then restores the full-tree CI scan.
- Squash-merge the bootstrap through the check-gated PR, then require
  its authenticated trusted-scan status on later main PRs. Release tags use the
  standard annotated-tag path from protected `main`; the release workflow
  validates every first-parent main commit after `v2.5.11`, and the
  version-tag integrity ruleset prevents movement or deletion.

## Bundled changes
CI now scans tracked-file content (not just commit messages) for the
customer-identifier block-list, so a name in a doc/test/comment fails CI.
