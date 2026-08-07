# Remove the GitHub CI gates and run them locally

## Goal

Delete the GitHub Actions gate workflows and every assertion that depends on
them, so a single maintainer gates releases from the local machine instead of
from CI.

## Contract

- Publishing stays on GitHub. `release.yml` is kept so a tag push still
  publishes through PyPI Trusted Publishing; moving it local would mean a
  long-lived API token on a workstation, which is strictly worse.
- The branch controls that are not status checks stay: no deletion, no
  force-push, linear history, squash-only through a pull request. Only the
  required status checks come off, because the checks they name no longer
  exist and would block every pull request forever.
- No protection may be deleted without moving what it protected. Where a gate
  covered something real, it moves into the local `ci` flow.

## Constraints

- `verify_release_provenance.py` asserted all of it: `REQUIRED_WORKFLOWS`,
  the trusted-scanner commit status and its file digest, and
  `BASE_REQUIRED_STATUS_CHECKS` against the live ruleset. Removing the
  workflows without unpicking those makes the release gate permanently
  unsatisfiable.
- `_rules_by_type` requires an EXACT rule-type set, so dropping the required
  status checks from the ruleset also means dropping `required_status_checks`
  from the expected set - otherwise provenance fails on a ruleset that is
  correct.
- The plan-file gate is per COMMIT, and `.github/workflows/`, `scripts/` and
  `tasks.py` are all high-risk paths.

## Touched Surfaces

- Deleted: `.github/workflows/{ci,codeql,trusted-sensitive-pr,harness-gardening}.yml`
- `scripts/verify_release_provenance.py`, `scripts/check_harness.py`, `tasks.py`
- `scripts/tests/` for each of the above
- The `main-release-integrity` ruleset (required status checks removed)

## Approach

Delete the four gate workflows. Strip the workflow-run, trusted-status and
status-check assertions from the provenance verifier, leaving tag shape,
single-parent commits, merged-PR provenance, ruleset identity, tag
immutability, and the release plan's authorization evidence. Remove the three
harness checks that only described deleted files, and the file/content
requirements naming them.

## Validation

`invoke ci` and `invoke artifact-test` on the pinned environment, plus
`python3 -m unittest discover -s scripts/tests` and `scripts/check_harness.py`.

## Rollback

Revert the commit and re-add the required status checks to the ruleset. The
workflows are recoverable from git history.

## Decision Log

- **The upgrade gate was about to be lost silently.** `artifact-upgrade-test`
  ran ONLY in `ci.yml` - it is not in the local `ci` pre-list - so deleting the
  workflow would have left the upgrade path validated nowhere. It exists
  precisely because an upgrade defect found after the tag costs a version
  number. It is now the last entry in the local `ci` pre-list, after `package`,
  since it needs the built artifact, and `_check_publish_gate_placement` asserts
  that rather than asserting a workflow that no longer exists.
- **The 2.7.4 authorization evidence was corrected, not carried over.** It
  claimed `invoke ci` covered "5 artifact-upgrade paths". Those log lines come
  from `scripts/tests` exercising the upgrade harness with fixture versions
  (2.6.6 -> 2.6.7), not from a real upgrade onto the 2.7.4 wheel. The sentence
  was inherited from the 2.7.2 evidence and was never true of this release.
- **`release.yml` is kept.** It is a publishing pipeline, not a gate, and
  Trusted Publishing's short-lived OIDC credential is the reason there is no
  PyPI token to steal.
- **The sensitive-content scan survives the loss of its workflow.**
  `release.yml` still runs `check_sensitive_content.py --protected-history`,
  and pre-commit runs the same guard locally, so the control remains; only the
  GitHub check run and its commit status are gone.

## Open

- With no PR status checks, nothing external stops a merge. The gate is now
  entirely the maintainer running `invoke ci` before merging, and the release
  plan's authorization evidence is the record that it happened.
- `.github/workflows/ci.yml` was also the only thing running CodeQL. Static
  security analysis now runs nowhere.
