# Release 2.1.1 — IP global-dedup interface pin + Mgmt_ primary-IP audit

**Date:** 2026-06-26

## Goal
Blake's run resolved only 6 of ~520 Mgmt_-tag primary IPs (514 unresolved) and
logged many "target interface was not imported" IP skips (vlan2027, vlan224,
po109, …). Root-cause and fix the underlying defect.

## Root cause (code-confirmed)
`forward_ip_addresses_ipv4.nqe` / `_ipv6.nqe` collapse each address to one row.
In the **global (VRF-less) `global_rows`** branch, `chosen_device` and
`chosen_interface` are computed by INDEPENDENT `min()` over the grouped rows
(`min(device)` and `min(interface)` separately). For an address present on more
than one (device, interface) pair, the chosen interface can come from a different
candidate than the chosen device, yielding a `(device A, interface Y)` pair where
interface Y exists only on device B. The apply path binds the IP to that device's
interface (`sync_ipam.py`), the interface is absent → "target interface was not
imported" skip, and the per-device Mgmt_ resolver (`primary_ip.py` reads IPs
assigned to that device's interfaces) finds an empty list → unresolved.

The **VRF `vrf_rows`** branch already pins the interface to the chosen device
(`where candidate.device == chosen_device`); the global branch did not. This is an
inconsistency/oversight, not intent.

## Constraints
- Behavior-preserving for unique addresses (single-candidate groups are
  unaffected). Only changes which interface a *shared/global* address attaches to,
  making the (device, interface) pair always real.
- No release until Blake re-runs and confirms (per instruction).

## Touched Surfaces
- `forward_netbox/queries/forward_ip_addresses_ipv4.nqe` — `global_rows`:
  `chosen_interface` now filtered to `candidate.device == chosen_device`.
- `forward_netbox/queries/forward_ip_addresses_ipv6.nqe` — same.
- `forward_netbox/tests/test_ip_address_dedup_query.py` — regression guard: both
  global and VRF dedup must pin interface to device in each query.
- `forward_netbox/utilities/primary_ip_audit.py` + command
  `forward_primary_ip_audit` + `forward_netbox/tests/test_primary_ip_audit.py` —
  read-only audit that buckets Mgmt_ primary-IP resolution per device
  (resolvable / device_not_in_netbox / interface_not_matched /
  interface_present_no_ip) by reusing the resolver's own helpers. This is the
  diagnostic that localizes the customer's "514 unresolved" to the NetBox-side
  assignment gap. Never writes. Operations Guide documents it.

## Approach
Add `&& candidate.device == chosen_device` to the `chosen_interface` `where` in
each `global_rows`, mirroring `vrf_rows`. `chosen_device` is drawn from the same
max-prefix candidate set, so a matching candidate always exists.

## Validation
Regression guard test (string-level: constraint present in both branches of both
files). NQE logic is not locally executable — confirm live via the probe below.
Suite + lint/harness/sensitive before any release.

LIVE PROBE (Blake, before/after): per-device Django-shell classifier of the 514
unresolved into target-interface-missing vs interface-but-no-IP; re-run sync and
confirm (a) "target interface was not imported" skip count drops and (b)
"set primary IP on N device(s)" rises.

## Rollback
Revert the two one-line query `where` additions. No schema/data change.

## Decision Log
- Scope-complete: every dedup block was audited. `forward_mac_addresses.nqe`
  (chosen_interface `where candidate.device == chosen_device`) and BOTH VRF blocks
  in the IP queries already pin interface to device. The two global IP blocks were
  the only inconsistency — confirming an oversight, not intent. The fix makes all
  dedup blocks consistent.
- LIVE FINDING (customer network, current snapshot): old vs fixed IPv4 query both
  resolve 187/200 Mgmt devices (93%), identical output, 0 impossible pairs in the
  sample. So this defect is real but is NOT the dominant cause of Blake's 514
  unresolved — that is NetBox-side (IPs not assigned to the matched interfaces in
  NetBox at resolution time; the resolver reads NetBox assignments, not Forward).
  This fix is correctness hardening; it is NOT the Mgmt_ cure. Do not claim it
  fixes the 514. The NetBox-side bucket probe (run on the customer instance) is
  required to pinpoint the assignment gap before any Mgmt_-side change.
- Pre-existing NQE-lint hints on these files (no `@primaryKey`,
  `network.devices` multi-ref disabling device-parallel) are out of scope here;
  tracked separately.
