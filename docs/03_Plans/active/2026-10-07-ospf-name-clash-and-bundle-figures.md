# OSPF name clashes resolve themselves; bundle answers the next questions

## Goal

A customer's 3.0.3 sync still skipped one OSPF instance and one OSPF interface
with `name-not-unique-per-device`, although the support bundle showed no
duplicate names left (0 groups across 1,117 instances). The clash is therefore
between an incoming row and a different instance on the same device, and the
issue carried neither the device nor the name. The same bundle could not say
whether endpoint addresses were still /32, what sat at site `default`, or why
devices stayed without a primary IP except by reading a log line.

## Constraints

- No change to which row an OSPF instance is matched on
  (`device`, `vrf`, `process_id`); only the name written may differ.
- A name must be the same on every sync, so a row never flaps between names.
- Bundle additions are counts and ids only; no device names.

## Touched Surfaces

- `forward_netbox/utilities/sync_routing_impl.py`: `free_ospf_instance_name`,
  `_other_ospf_instance_names`, wired into `ensure_ospf_instance`.
- `forward_netbox/utilities/primary_ip.py`: `format_fallback_summary` and
  `parse_fallback_summary` share one sentence.
- `forward_netbox/views.py`: `primary_ip` gains `interface_ip4_prefix_lengths`,
  `site_placement` and `fallback_reasons`.
- Tests: `test_ospf_instance_names.py`, `test_primary_ip.py`,
  `test_bundle_triage_diagnostics.py`.

## Approach

When another instance on the device already holds the desired name, the row is
kept under `<name> #<process_id>` (stepped with `-N` if that is taken too)
instead of being skipped. The OSPF interface path goes through the same
instance call, so it is covered by the same change. `netbox_routing` cannot be
installed on the 4.7 test runtime, so the lookup is pinned against a stub
queryset and the pure naming function directly; the 4.6 lane exercises the real
model.

## Validation

Targeted suites pass in the isolated stack (53 tests); the full gate runs on
push.

## Release Impact

Ships in 3.0.4. No migration, no query change.

## Rollback

Revert the commit; names written as `... #<process_id>` stay valid.
