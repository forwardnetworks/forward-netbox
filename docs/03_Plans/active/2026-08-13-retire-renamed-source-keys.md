# Retire a departed source key when its device is re-bound under a rename

## Goal

Stop a Forward-side rename freezing a device's tags permanently. A renamed
device kept its old identity row, so under the new name every candidate read as
bound to a different key and the device was held on every run, with no operator
remedy.

## Constraints

- A binding must never be retired without evidence the old key is genuinely
  gone from Forward's current result - absence from one mutation's keys is not
  that evidence, which is precisely the bug.
- No live set, no retirement: `live_source_keys=None` preserves the hold
  exactly as it was. Holding is the safe reading when evidence is incomplete.
- A tie retires nothing. If adoption cannot pick a single candidate, no binding
  may be deleted on the strength of a guess either.
- The `(sync, device)` uniqueness constraint means the stale row MUST be
  deleted in the same locked step as the re-bind, or the re-bind itself raises.

## Touched Surfaces

- `forward_netbox/utilities/ownership.py` - `resolve_device_identities` gains
  `live_source_keys`; the adoption path retires the predecessor row;
  `reconcile_source_device_tag_claims` and `reconcile_sync_scope_tag_claims`
  thread the parameter.
- `forward_netbox/utilities/scope_reconciliation.py` - `tag_backfilled_devices`
  passes the report's full tag-scope result as the live set.
- `forward_netbox/tests/test_absent_device_does_not_block_tag_domain.py` - the
  characterization test that pinned the permanent hold becomes the regression
  test for the fix, plus three safety pins.

## Approach

The blocking rule asked "is this binding's key in this mutation?", and a
mutation is one tag's slice of the estate - so a rename was indistinguishable
from another live device. The right question is "does Forward still report this
key anywhere?", and `tag_backfilled_devices` already holds the answer: the
scope report's `_tagged_names` is the full tag-scope result.

A binding whose key is absent from that set is evidence of a rename. It is
retired at exactly one moment: when its device is about to be re-bound as the
single free candidate for the name Forward now reports, inside the ownership
write lock, immediately before the new row is created. Departed keys whose
devices are not being re-bound stay untouched - this is deliberately NOT a
general prune of departed identities, which remains unbuilt because a departed
key with no successor carries no evidence about what its device now is.

The vsys resolution path passes no live set and keeps the hold.

## Validation

Four tests: the rename binds under the new name and the old row is gone; no
live set means the hold remains (the pre-fix behaviour, kept on purpose); a key
still present in the live set keeps blocking (two devices, not a rename); and a
tie retires nothing.

## Rollback

Revert. Renamed devices return to being held forever, which is the defect.

## Decision Log

- **Retire on re-bind, not as a sweep.** A general prune of departed keys was
  attempted once before and produced dead code; a departed key with no
  successor name carries no evidence about its device. The rename case is the
  one place the evidence is complete: the old key is gone from the full result
  AND its device is the unique match for a name that is present.
- **Default to the old behaviour.** Every caller that cannot supply the full
  live set keeps the hold. The fix narrows the hold where evidence exists; it
  does not weaken it where evidence is absent.

## Open

- The customer's held devices clear on their next scope reconciliation run
  after upgrading; no operator step.
