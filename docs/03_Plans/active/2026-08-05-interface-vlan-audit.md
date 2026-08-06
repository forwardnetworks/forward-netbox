# Let an operator find the interface a sync refused

## Goal

A customer sync recorded `untagged-vlan-outside-device-site` against
`dcim.interface` and nothing else. The rule is now nameable; the row is not. Give
the operator a way to find the interface without asking us.

## Contract

- Read-only. Never writes, never persists.
- Names the device, interface, both sites and the VID — enough to act on without
  a second lookup.
- Counts are exact; the listing is bounded, so it is runnable on a deployment
  with tens of thousands of interfaces.
- Reports both `untagged_vlan` rules separately, because they have different
  fixes.

## Constraints

- Persisted diagnostics carry schema identifiers and never customer data, which
  is why the issue row cannot name the interface. That policy is not being
  changed: `record_issue` composes its own message and `diagnostic_shape`
  reduces context to key names. This writes to the operator's console instead,
  the same posture as the eleven audits that already exist.
- The invalid states cannot be built through the model — `save()` nulls an
  untagged VLAN when mode is unset and `full_clean()` rejects a cross-site one —
  so tests construct them with `queryset.update()`, which is also how they arise
  in the field.
- No customer identifiers in tracked content.

## Touched Surfaces

- `forward_netbox/utilities/interface_vlan_audit.py` (new)
- `forward_netbox/management/commands/forward_interface_vlan_audit.py` (new)
- `forward_netbox/tests/test_interface_vlan_audit.py` (new)

## Approach

Two querysets over `Interface`, mirroring NetBox's own rules at
`dcim/models/device_components.py:1122` and `:1126`:

- `cross_site` — `untagged_vlan.site` is non-null and differs from
  `device.site`. The null case is excluded rather than compared, because a
  global VLAN is valid on any device.
- `no_mode` — an untagged VLAN with `mode` empty or null.

## Validation

Seven tests: a clean deployment reports nothing, a global VLAN is not a
violation, a cross-site VLAN is reported with both sites named, the no-mode rule
is reported separately from the cross-site one, counts stay exact while the
listing is capped, a zero limit still counts, and an interface with no untagged
VLAN is never reported.

## Rollback

Revert. Nothing is written by this code, so removing it cannot leave state
behind.

## Decision Log

- **A command, not a change to what issues persist.** The alternative was
  storing device and interface names on the issue row. Refused: that is a
  deliberate storage policy protecting support bundles and exports, and this
  need is served by computing the answer on demand instead.
- **Both rules, not just the one the customer hit.** A sync cannot tell them
  apart at the point of failure any better than the operator can, and the
  no-mode pairing is only reachable through a writer that bypasses validation —
  which makes it worth surfacing when it exists.

## Open

- Not scoped to Forward-owned devices. An interface the plugin never touches can
  still be reported, which is arguably right — NetBox refuses it either way —
  but it means the count is not the same as "rows this sync will refuse".
- Does not report which devices moved sites, which is the mechanism that creates
  the cross-site pairing (the plugin writes `site` through `bulk_update`, which
  runs neither `save()` nor `clean()`, so nothing revalidates the interfaces).
