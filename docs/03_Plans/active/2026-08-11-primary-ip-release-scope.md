# Release a primary-IP pointer held by a device that left scope

## Goal

Let an address move to its new device when the device holding it as a primary IP
has left the Forward tag scope. This is the customer's `ipam.ipaddress`
ingestion issue on 2727, `primary-ip-reassignment-blocked`.

## Constraints

- The plugin must never clear a primary pointer on a device it cannot prove it
  created. That proof is the exact-sync `ForwardDeviceIdentity` row, and it is
  unchanged.
- A runner with no resolved scope context stays fail-closed.
- Device names are customer data; any new operator signal reports counts.

## Touched Surfaces

- `forward_netbox/utilities/sync_ipam.py` - `release_owned_primary_ip_claims`
- `forward_netbox/tests/test_primary_ip_integration.py`

## Approach

`release_owned_primary_ip_claims` required BOTH an identity proof and current
scope membership. The scope requirement is removed; the identity proof is not.

The scope guard protected nothing, which the reproduction test now shows
directly. Refusing the release does not refuse the reassignment - the branch
moves the address regardless - so the merge replays an IP UPDATE against a main
where the holder still names it primary, and `IPAddress.clean()` refuses exactly
there. The observable outcome was never "the out-of-scope device is left alone";
it was an address that never moved, on every run, with no operator remedy,
because re-running cannot change a NetBox validation rejection.

A device leaving the tag scope is precisely the situation that produces it: it
keeps its NetBox row and its primary pointer while its address moves to a device
still in scope. The customer has 49 such devices.

Where the release is still correctly refused - a holder with no identity from
this sync - the sync now warns during staging, because the consequence
otherwise lands at merge time on a row that names neither device.

## Validation

`test_an_owned_holder_that_left_scope_is_still_released` merges the branch and
asserts the address actually moved; it fails on the old code with the customer's
exact error. `test_a_holder_this_sync_does_not_own_is_never_released` pins the
guard that carries the real safety property.

## Rollback

Revert. The address stops moving again, and the ingestion issue returns.

## Decision Log

- **Remove the scope requirement rather than special-case departed devices.**
  Scope membership answers "is this device in the current query result", which
  has no bearing on whether this sync owns the row it is correcting. Ownership
  is the property that matters and it is already proven separately.
- **Reproduce before fixing.** The existing test asserted the guard held but
  never merged, so it pinned the mechanism while hiding its consequence. The
  replacement merges.
- **Warn at staging for the unowned case.** It stays refused, but an operator
  should not have to wait for a merge-time rejection that names neither device
  to find out why.

## Open

- Whether the customer's specific row is the departed-holder case or the unowned
  case is not knowable from the support bundle, which records the model and the
  rule but not the object. The first is fixed; the second is now announced
  before the merge instead of after.
