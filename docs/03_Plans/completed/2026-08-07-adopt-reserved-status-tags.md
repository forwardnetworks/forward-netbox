# Adopt a pre-existing reserved status tag instead of refusing forever

## Goal

Stop an existing `forward-backfilled` tag from breaking ownership
reconciliation permanently.

## Contract

- The operator keeps everything they tagged. Adoption records their existing
  assignments as preserved, and preserved assignments are never removed.
- A genuine conflict still refuses: one tag cannot be two claim types.

## Constraints

- `forward-backfilled` exists in any deployment that has ever had a collection
  failure, so this is not an edge case.
- The refusal is unrecoverable by the operator: the ownership domain never
  completes, convergence stays blocked, and every drift figure reads "Not
  measured" on every subsequent run. Nothing short of deleting the tag clears
  it.

## Touched Surfaces

- `forward_netbox/utilities/ownership.py`
- `forward_netbox/tests/test_reserved_status_tag_adoption.py`,
  `forward_netbox/tests/test_ownership.py`

## Approach

Delete the reserved-slug refusal and the `allow_reserved_adoption` parameter,
and let the adoption path that follows it do its job.

## Validation

- `invoke test-isolated` - full plugin suite, 1998 tests, OK (4 skipped)
- The new tests were confirmed to FAIL without the fix (4 of 5 error) before
  being accepted
- Adoption verified empirically to leave both the reserved tag and the
  operator's own tag on the device

## Rollback

Revert. The refusal returns, and with it the permanent failure for any
deployment whose reserved tag lacks a managed row.

## Decision Log

- **The override could never fire.** The sole caller passed
  `allow_reserved_adoption=tag_created`, true only when the plugin had just
  created the tag - exactly when there is nothing to adopt. So the refusal was
  unconditional in every case that mattered. Same shape as `#43`.
- **The refusal was redundant, not protective.** An existing test asserted it
  kept an operator's tag on their device. It does not:
  `_materialize_managed_tag_assignments` folds preserved assignments back into
  `desired_ids`, so they are never removed. That test was rewritten to assert
  the outcome it actually cared about, and it passes under adoption.
- **Checked before overriding the test.** The full suite caught it, and the
  first move was to find what it protected rather than to update it to match
  the change.

## Open

- Whether this is THE cause of the customer's `Ownership: Incomplete` is still
  inference from screenshots, not confirmation. `#58` means a surviving
  conflict now names itself.
