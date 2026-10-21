# Tag the IP addresses a BGP peer needs

## Goal

The routing sync creates a BGP peer's neighbour address as an IP address with no interface, because the neighbour is a remote device. In IPAM those read as orphans: a customer's 14,988 unassigned addresses were almost all neighbours. Give them a tag an operator can filter in or out.

## Constraints

- Operator-run only, never inside a sync. Tagging an existing estate writes one change per address, which would trip the "Unusually large run" advisory and the held-for-review auto-merge guard on the first sync after an upgrade.
- Tag-only; nothing deletes or prunes on it.
- No-op when `netbox_routing` is not installed.
- Does not change what the sync writes.

## Touched Surfaces

- `forward_netbox/utilities/bgp_neighbour_ips.py` (new): the shared queryset and `tag_bgp_neighbour_ips`.
- `forward_netbox/management/commands/forward_tag_bgp_neighbours.py` (new).
- `forward_netbox/views.py`: the bundle's unassigned split now uses the shared queryset.
- `forward_netbox/tests/test_bgp_neighbour_tag.py`, `forward_netbox/tests/test_unassigned_ip_bundle.py`.
- `docs/01_User_Guide/operations.md`.

## Approach

`forward-bgp-neighbour` is reconciled to exactly the unassigned addresses that a BGP peer references as its peer or source address, on every run, in the same style as `forward-delete-eligible`: reusing `_apply_maintained_ipam_tag`; addresses are added, and removed once an interface carries the address or the peer goes. The bundle count and the tag set come from one helper so they cannot drift.

## Validation

- Tests: only unassigned peer addresses are tagged; a second run changes nothing; an interface-assigned address is never tagged; an address that gains an interface loses the tag; a deleted peer's address loses it; the tag set equals the bundle's `used_by_a_bgp_peer`; the command reports its result; no-op without netbox-routing.
- Cost: the first run writes about 15,000 tag changes to the change log. Measured on the dev stack with 15,000 synthetic peer addresses: the first run took 22 s (15,000 added) and a second run 0.1 s (nothing added).

- The command requires `--user`: a management command has no request, so without one NetBox would log none of the edits. Given a user, the run goes through `event_tracking` and each edit is change-logged and attributed.

## Rollback

Revert. Remove the tag from the Tags list in NetBox if it should disappear entirely.

## Decision Log

- Not done in the sync: it would put the whole estate's tag writes in one reviewed run.
- Not done at address creation: it would tag only new addresses and go stale once an address gains an interface.
- No button yet; a management command matches `forward_scope_ipam_audit`. A Scope Reconciliation button can follow if wanted.
