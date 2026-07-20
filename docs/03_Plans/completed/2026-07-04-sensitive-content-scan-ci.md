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
The scanner and tests, immutable trust-anchor files, CI/CodeQL workflows,
fork-safe trusted PR workflow, protected-main tag workflow and authorizer,
release controller, and valid CODEOWNERS entries.

## Approach
Scan tracked files plus every post-baseline commit, changed blob, path, ref, and
annotated tag object. Reject binary content unless its current-tree path and
digest are reviewed; accept historical binary exceptions only from an external
secret. For fork PRs, check out only the trusted base, fetch candidate objects
without executing them, scan the candidate tree/history/ref, and publish the
result as an authenticated status on the exact candidate SHA.
Bootstrap and version tags are authorized only by the frozen workflow running
at the exact current protected-main SHA. Its independently reviewed environment
is the only holder of the deploy key recognized by creation-only tag rulesets;
the authorizer requires that SHA to be current before creating the tag. A
concurrent protected-main advance after authorization is valid only when the
tagged commit remains an ancestor; divergence fails closed.

## Validation
Scanner and harness regression suites; pre-commit; protected-history scan with
the external baseline and nonempty private-pattern feed; provenance tests for
cross-workflow status forgery, wrong-PR reuse, and paginated review revocation;
fresh hash-locked release-tool installation; byte-identical double builds; and
exact CI and CodeQL workflow runs on the reviewed bootstrap commit. Trusted-tag
tests reject non-main dispatch, abbreviated SHAs, package-version mismatches,
direct human tag pushes, and candidate-controlled controller changes.
The final bootstrap repair stores the isolated deploy-key and GitHub host-key
settings in the runner SSH config, which keeps the trusted workflow both
formatter-stable and fail-closed under the same CI pre-commit gate.
CodeQL then identified two high-severity clear-text logging flows in privileged
release code. The authorizer now emits only a fixed success message and the
release command runner never logs command arguments or includes them in errors;
regression tests inject secret-shaped values and prove they remain absent.
Final independent rereview found that the trusted private-pattern status was
not yet required by `main` and that a no-op main-ref lease could not prevent a
post-authorization race. The bootstrap permits its own four public statuses;
version authorization requires the authenticated private-pattern status as a
fifth main rule, and the single-ref tag push now has explicit ancestor/race
tests. The authorizer also fail-closes on live repository, ruleset, environment,
deployment-policy, and Actions SHA-pinning drift. Both release environments
disable administrator bypass.

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
- Squash-merge the bootstrap through an independently approved PR, then require
  its authenticated trusted-scan status on later main PRs. Release tags use the
  standard annotated-tag path from reviewed `main`; the release workflow
  validates every reviewed first-parent main commit after `v2.5.11`, and the
  version-tag integrity ruleset prevents movement or deletion.

## Bundled changes
CI now scans tracked-file content (not just commit messages) for the
customer-identifier block-list, so a name in a doc/test/comment fails CI.
