# Split unassigned IP addresses in the support bundle

## Goal

A customer's support bundle reported 14,988 IP addresses assigned to nothing, and it read as an orphan problem. Almost all are BGP neighbour addresses: the routing sync creates a peer's address as an IP address with no interface, because the neighbour is a remote device. Split the bundle's count so the expected ones and the real orphans are separate numbers.

## Constraints

- Counts only; no addresses, names or ids in the bundle.
- Only when `netbox_routing` is installed; otherwise the block is unchanged.
- No behaviour change to what the sync writes.

## Touched Surfaces

- `forward_netbox/views.py` (`_interface_state_bundle_payload`, new `_unassigned_ip_split`).
- `forward_netbox/tests/test_unassigned_ip_bundle.py`.

## Approach

An unassigned address counts as used by a BGP peer when a peer references it as its `peer` or `source` address. The block gains `used_by_a_bgp_peer`, `used_by_a_bgp_peer_in_a_vrf` and `used_by_no_interface_and_no_bgp_peer`; the last is the count that could be a real orphan.

## Validation

- Tests: a neighbour address and a real orphan are told apart, an address two peers share counts once, and an address on an interface is not in the count.
- Evidence it explains the customer's number: Forward lists 18,398 distinct neighbour addresses (11,413 in named VRFs, 6,985 global) against 14,988 unassigned in NetBox (10,519 in a VRF); the difference is neighbour addresses that already existed as another device's interface address and were reused.

## Rollback

Revert; the block returns to its two existing counts.

## Decision Log

- The unassigned addresses are by design and are not cleaned up. A tag or description on them would write to about fifteen thousand rows to label something that is not wrong, so it is left as a possible later change.
