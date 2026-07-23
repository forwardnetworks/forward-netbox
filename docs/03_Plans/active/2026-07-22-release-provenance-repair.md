# Release Provenance Repair

## Goal

Allow the validated 2.6.0 protected-main tag to pass provenance validation and
publish identical GitHub and PyPI artifacts.

## Constraints

Keep the repair limited to release provenance metadata and preserve protected
branch, tag, and trusted-publishing controls.

## Touched Surfaces

- `scripts/verify_release_provenance.py`
- This release repair plan

## Approach

Correct the prior-release documentation bridge used by tagged-release
provenance validation. The previous commit identifier was not present in the
repository history, so the 2.6.0 publication workflow rejected an otherwise
valid protected-main release.

## Validation

- The actual post-2.5.11 documentation bridge is `f9a8420a8bcc2d3afe338d0435a17df9e2bc01d0`.
- The change is limited to the provenance validator constant.
- Required CI, CodeQL, sensitive-content scanning, and tagged release
  validation must pass before retrying publication.

## Rollback

If validation fails, do not publish or recreate the release tag; revert the
repair commit through the protected PR flow.

## Decision Log

- Use the actual first-parent bridge commit rather than weakening provenance
  validation or bypassing the release workflow.
- Treat GitHub-merged pull-request lineage and required checks as the release
  trust source; historical commit signatures are not consistently available.
- Permit only the known pre-PR security-control prefix to use direct commit
  provenance, and reject it if it changes production plugin code.
- Skip unavailable historical workflow-run lookups only for that same
  security-control prefix; all pull-request-backed commits still require the
  complete required-workflow success set.

## Completion

After the protected PR merges, recreate the failed `v2.6.0` tag from the
validated main commit and verify identical GitHub and PyPI artifacts.
