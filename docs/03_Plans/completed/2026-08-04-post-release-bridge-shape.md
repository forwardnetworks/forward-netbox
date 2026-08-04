# Accept both real post-release bridge shapes

## Goal

Make release provenance verifiable again. As anchored, it cannot pass for any
release after `v2.7.0`, which blocks `2.7.1` and everything after it.

## Contract

- The post-release bridge still carries no executable content. A bridge
  touching plugin code, scripts, or workflows fails exactly as it did before.
- The anchor constants are not moved. This changes what shape the bridge may
  have, not which commit it is.
- Both shapes that the release flow actually produces are accepted, and each is
  pinned by a test so neither can regress.

## Constraints

- This widens a security control's acceptance criteria. It is deliberately the
  narrowest widening that admits the real flow: an allowlist of documentation
  paths, not a removal of the check.
- The anchor cannot be re-pointed to fix this. The bridge is defined as the
  first first-parent commit after the prior release tag, and that slot is
  already occupied on `main`.

## Touched Surfaces

- `scripts/verify_release_provenance.py`
- `scripts/tests/test_verify_release_provenance.py`

## Approach

`_require_prior_release_bridge` required the bridge to change exactly one file
under `docs/03_Plans/completed/`. Two commit shapes legitimately follow a
release:

1. archiving the release plan into `docs/03_Plans/completed/`, and
2. promoting the release candidate, which rewrites `CHANGELOG.md` and the three
   compatibility tables.

`v2.7.0` was promoted without first being archived, so promotion took the
bridge slot. The check reserved that slot for archival, and no later commit can
reclaim it, so every subsequent release became unverifiable.

The rule becomes a path allowlist — `CHANGELOG.md`, `README.md`, and any
`docs/**.md` — which accepts both shapes and still refuses anything executable.

## Validation

`scripts/tests/test_verify_release_provenance.py` gains five cases: both
accepted shapes, a bridge carrying plugin code, a bridge carrying a workflow,
and an empty bridge. The full release gate re-runs against the final tree,
because the tagged tree now differs from the tree the first gate ran against.

## Rollback

Revert this commit. That restores the previous rule and, with it, the condition
that makes releases unverifiable — so a rollback is only correct alongside a
different fix for the same defect.

## Decision Log

- Widening was chosen over re-anchoring to `v2.6.12`. Re-anchoring leaves the
  check untouched but reintroduces the expired-workflow-run problem that spent
  `v2.6.10` and `v2.6.11`, and it conflicts with the harness check requiring
  the anchor to track the current release.
- The empty-diff case is rejected explicitly. `all()` is vacuously true on an
  empty sequence, so without that guard a bridge changing nothing would pass.

## Open

- The post-release close-out should archive the plan *before* promoting, so the
  bridge is the archival commit. Nothing enforces that ordering yet; this change
  makes the ordering non-fatal rather than correct.
