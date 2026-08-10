# A device Forward reports but NetBox lacks must not refuse the whole domain

## Goal

Let ownership reconciliation complete when Forward reports devices that this
NetBox does not have.

## Contract

- A name with no device in NetBox is SKIPPED and counted. Nothing to tag,
  nothing to release, so no NetBox row changes.
- A name shared by several devices still REFUSES. `desired_ids` drives both the
  add and the remove, so dropping the key could release a claim from a device
  that holds one, and resolving it could tag the wrong device.
- The refusal reports counts. Device names are customer data.
- Both return paths carry the same keys.

## Constraints

- NetBox scopes device-name uniqueness to the SITE, so a test for genuine
  ambiguity needs the same name in two sites - which is also how it arises in a
  real estate.
- `reconcile_virtual_parent_claims` already resolves identities this way and
  skips per key. It is the one ownership domain that kept completing for the
  customer while these two failed.

## Touched Surfaces

- `forward_netbox/utilities/ownership.py`
- `forward_netbox/tests/test_absent_device_does_not_block_tag_domain.py`

## Approach

Split the two failure kinds that were lumped into one refusal: skip `missing`,
keep refusing `ambiguous`, and report `skipped_absent_devices` so the skip is
visible.

## Validation

- 6 tests, OK; full plugin suite 2020 tests, OK (4 skipped)
- The three missing-device tests confirmed to ERROR with the previous
  `if missing or ambiguous` restored; the ambiguity tests pass either way,
  which is the split the fix is built on

## Rollback

Revert. One absent device again refuses the whole tag domain.

## Decision Log

- **Found by the diagnostic, not by guessing.** The conflict catalogue shipped
  in 2.7.6 named `tag-mutation-identity-unresolved` on the customer's job. The
  earlier suspicion - reserved status-tag adoption - was wrong, and would have
  stayed wrong without the catalogue.
- **`missing` and `ambiguous` are not equally dangerous.** Treating them alike
  is what made an absent device fatal. Only the one that can mis-tag or wrongly
  release still refuses.
- **Two defects surfaced from the tests, not the premise.** The "nothing
  resolved" exit returned a different dict shape, so callers could not rely on
  the new key; and the first ambiguity fixture could not be built at all,
  because NetBox scopes name uniqueness to the site.

## Open

- The customer's eleven absent devices remain absent. This stops them blocking
  convergence; whether Forward should be tagging devices that NetBox has never
  had is a question for their side, and the count now makes it visible.
