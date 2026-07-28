# Release authorization proportionality

## Goal

Make release authorization enforce what a machine can verify and stop demanding
prose for what it cannot. The previous gate required seven signed evidence
items, five of which asserted work this checker has no way to observe.

## Constraints

- Do not weaken the GitHub rulesets (`main-release-integrity`,
  `version-tag-integrity`). Protected merge and immutable tags stay.
- Do not weaken the controls that caught real defects: sensitive-content
  scanning, version-surface consistency, `artifact-test`, and the full suite in
  CI.
- Authorization must still bind to a specific tree; it must not be possible to
  transplant an approval onto a different commit.

## Touched Surfaces

- `scripts/check_release_authorization.py`

## Approach

Two changes.

**Required evidence reduced to the machine-enforced set.**
`REQUIRED_EVIDENCE_IDS` is now `final-tree-full-gate` and
`exact-runtime-artifact`. Both correspond to gates that actually run against the
tagged tree and whose outcome is recoverable afterwards from the recorded run.

`scale-and-failure`, `ui-validation`, `ownership-audit`,
`customer-equivalent-acceptance` and `independent-review` move to
`OPTIONAL_EVIDENCE_IDS`. They remain recognized and are still fully validated
whenever a release records them — the concreteness, command-binding and
retrospective-outcome rules are unchanged. They are simply no longer mandatory
for every release.

**The evidence-only commit requirement is removed.**
`release_evidence_commit_binding` no longer demands that the tagged commit
change only the release plan. The plan may ship in the same commit as the code
it authorizes.

Still enforced: the tagged commit must have exactly one parent, the working tree
must be clean, and the plan's recorded evidence base commit must equal that
parent.

**Optional does not mean unvalidated.** Only presence is optional. Any recorded
known id is still held to the full concreteness, command-binding and
retrospective-outcome standard, because an id recorded with hollow evidence
reads as a claim the work was done and is worse than an absent one. The harness
rejection cases build plans from every known id so they cover the optional ones
as well.

## Rejected alternatives

- **Leave the gate as-is.** It regex-matches prose for a command string, a
  retrospective-sounding word and a digit. It never verified a command ran, so
  it was expensive for an honest release and trivial for a dishonest one. That
  combination is worse than no gate, because it looks like assurance.
- **Delete authorization entirely.** The tree binding is cheap and real; losing
  it would allow an approval to be reused against a different commit.
- **Keep the second pull request.** Its only content was prose, and the branch
  ruleset requires zero approving reviews, so it never delivered the
  independent sign-off it appeared to represent.

## Validation

- `invoke ci` and `invoke artifact-test` in the release-gate runtime.
- The reduced required set still fails closed when either required id is
  missing, unchecked, or non-concrete.

## Rollback

Revert `scripts/check_release_authorization.py`. The optional ids retain their
original validation logic, so restoring them to `REQUIRED_EVIDENCE_IDS`
reinstates the previous behavior exactly.

## Decision Log

- Release owner decided on 2026-07-27 that CI's checks plus `artifact-test`
  are the appropriate bar for a corrective patch, and that the remaining items
  should not be run as ceremony.
- Prefer machine-verified evidence over attestation. The durable improvement is
  a workflow-emitted, SHA-bound manifest recording which gates ran and their
  conclusions; reinstate any optional id alongside that, not alongside another
  prose field.
- Requirements should scale with release type. A corrective patch does not need
  the same acceptance and UI validation as a feature release.

## Completion Evidence

- `scripts/check_release_authorization.py` requires two ids and binds
  authorization to the tagged commit's parent.
- 2.6.2 authorized under the reduced set.
