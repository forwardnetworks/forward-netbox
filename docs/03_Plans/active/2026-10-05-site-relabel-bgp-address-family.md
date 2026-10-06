# Release BGP address families when merging site-relabel duplicates

## Goal

Stop the Merge site-relabel duplicates action refusing a pair because the newer
device's BGP scope still has an address family. A customer who upgraded to
2.9.13 saw 84 pairs merge and 2 refused with
`newer_device_delete_refused - blocked by netbox_routing.bgpaddressfamily`.

## Constraints

- The releasable allowlist stays an explicit list of models the sync itself
  builds. Operator-made BGP session, policy and peer templates stay protected.
- Everything in `netbox_routing` PROTECTs its parents, so a model that is built
  under a scope and is missing from the list blocks the delete of that scope,
  then of the router, then of the device.

## Touched Surfaces

- `forward_netbox/utilities/scope_reconciliation.py`
  (`SITE_RELABEL_RELEASABLE_ROUTING_MODELS`)
- `forward_netbox/tests/test_site_relabel_pairs_merge.py`

## Approach

`BGPAddressFamily` is created per scope by the sync and PROTECTs its
`BGPScope`. 2.9.13 made the release recursive but the model itself was never on
the list, so the recursion refused at it. Add it to the list. The other
protectors in `netbox_routing` are either already listed or are operator-made
templates, which must keep refusing.

## Validation

- A new test builds a newer device whose scope carries an address family and
  asserts the pair merges and the row is reported released.
- A second test pins that the three operator-made template models are not on
  the list.
- Full `invoke ci` pre-push gate.

## Rollback

Revert this branch. No migration and no persisted state. Released rows are
rebuilt by the next sync against the surviving device.

## Decision Log

- **Allowlisted one model, not a broad rule.** Releasing "everything that
  protects the device" is the unbounded delete this list exists to prevent.

## Also in this change: primary-IP fallback and its reasons

A customer on 2.9.13 still had about 1,500 devices without a primary IP. Their
own bundle said `590 tag(s) unresolved`, and the code comment explained why
those devices stayed bare: a device with any `Mgmt_` tag was excluded from the
management-address fallback "resolved or not". A tag that resolved nothing
leaves the device with no primary IP at all, so the fallback is strictly better
than nothing there. The fallback now skips only devices whose tag RESOLVED.
On that estate's current data the fallback resolves 233 of the 235 devices with
an unresolved tag.

The rest of the gap is not a failure to resolve: 579 firewalls are virtual
systems that report their parent's management address (22 devices on one
address). NetBox allows one primary-IP owner per address and the IPv4 query
keeps one device per address, so only one of them can hold it. The sync's log
now says so, with counts, instead of reading as an unexplained shortfall:
shared with another device, on no synced interface, several management
addresses, on several interfaces.

Touched: `forward_netbox/utilities/primary_ip.py`,
`forward_netbox/tests/test_primary_ip.py`,
`forward_netbox/tests/test_primary_ip_integration.py`.
Rollback is the same revert; a device that gains a primary IP keeps it until a
later sync changes it.

## Also in this change: say which duplicate names are held

The Site-Relabel Duplicates card reported "5 held - the sync's device map gave
no NetBox site for this name" with no names, so the operator could not check
which devices to look at. The page now lists the held device names under each
reason (first ten, then a count). Names are attached in the page view only;
the support bundle and Health share the counts-only helper and stay free of
customer names, pinned by a test.

Touched: `forward_netbox/views.py`,
`forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`,
`forward_netbox/tests/test_site_relabel_prompts.py`.
