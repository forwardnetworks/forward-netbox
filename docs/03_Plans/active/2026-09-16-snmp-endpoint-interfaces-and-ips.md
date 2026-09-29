# SNMP endpoints get interfaces and IP addresses

## Goal

The SNMP endpoints `forward_devices.nqe` already imports as device rows
(Avocent/Opengear console servers, and any other SNMP endpoint when
`sync_generic_endpoints` is on) get one `dcim.interface` row per ifTable
`ifIndex` and one `ipam.ipaddress` row per `ipAddrTable` entry, so
NetBox's existing primary-IP resolution (Mgmt_ tag, management address)
has something to resolve against on these devices instead of nothing.

## Constraints

- No Python changes: query parameter injection (`sync_endpoints`,
  `sync_generic_endpoints`, `scope_endpoints_by_include_tags`,
  `device_tag_include_tags`, `device_tag_include_match`,
  `device_tag_exclude_tags`) is driven entirely by parsing each `.nqe`
  file's own `@query` signature (`declared_query_parameters`,
  `query_registry._default_query_parameters`) - adding these parameter
  names to a query's signature is sufficient; nothing in
  `query_registry.py`'s `BUILTIN_QUERY_MAPS` entries needed a
  `parameters` key even for the existing `forward_devices.nqe`.
- The eligibility gate (tag scope, CIMC exclusion, Avocent/Opengear
  console-server detection through `isConsoleServer`) must stay
  byte-identical to `forward_devices.nqe`'s endpoint branch. An interface
  or IP row whose device was never created is a dependency-skip in the
  apply engine, not a corruption, but the two gates disagreeing would
  silently strand endpoints without interfaces/IPs whenever the device
  row itself was refused (or the reverse: emit orphaned rows for
  endpoints that got no device).
- No customer names, identifiers, or scan-matching hostnames in
  committed content, commit messages, or pull-request bodies.

## Touched Surfaces

- `forward_netbox/queries/forward_interfaces.nqe` - existing device
  logic extracted into `device_interfaces(...)`; new
  `endpoint_interfaces(...)`; `@query f` gains the six endpoint/tag
  parameters and unions both branches.
- `forward_netbox/queries/forward_ip_addresses_ipv4.nqe` - new
  `endpoint_ipv4(...)`; `@query f` gains the same six parameters and
  unions it alongside the existing `global_rows`/`vrf_rows` union.
- Tests: `forward_netbox/tests/test_endpoints_import.py` (new
  `EndpointEligibilityGateParityTest`), `test_query_variants.py` (fixed
  `test_endpoints_on_without_supporting_map_warns`, whose example query
  now legitimately supports the parameter it was testing the absence
  of; added `test_endpoints_on_with_interfaces_map_passes`).

## Approach

**One row per ifTable ifIndex, name from ifXTable else ifTable.**
`endpoint_interfaces` enumerates `ifDescr` entries (`1.3.6.1.2.1.2.2.1.2.*`)
as the anchor - ifTable is universally present on anything answering SNMP
at all, so it is one row per `ifIndex` the endpoint actually reports. The
`ifIndex` is the OID suffix after that fixed-length prefix (`substring`,
clamped so a generous end bound needs no `length()` call). `ifName`
(`1.3.6.1.2.1.31.1.1.1.1.<ifIndex>`) is looked up by re-matching that same
suffix and preferred when present (a nicer, more modern name); `ifDescr`
is the fallback everything already has. `enabled` reads `ifOperStatus`
(`1.3.6.1.2.1.2.2.1.8.<ifIndex>`) == "1" (up). `type` is always `"other"`:
SNMP alone cannot tell physical media apart from a generic OID walk, and
`"other"` is the same fallback `device_interfaces` already uses for a
non-ethernet, non-derivable type.

**One row per ipAddrTable entry, always a /32 host address.**
`ipAdEntIfIndex` (`1.3.6.1.2.1.4.20.1.2.*`) is the anchor: `ipAddrTable`
is indexed BY the address itself, so this column's OID suffix IS the
dotted IPv4 address (validated with a regex before calling `ipAddress()`,
which throws on anything that is not a valid address - defense against a
walk returning something malformed rather than trusting SNMP framing
blindly) and its `rawValue` is the owning `ifIndex`, used to look up the
interface name the same way `endpoint_interfaces` does. Always `/32`:
`ipAddrTable` carries no mask this query can derive a real subnet from,
and a host address is the honest default for a directly observed
management address - this pipeline already treats `/31` and `/32` as
single-host addresses elsewhere (the `>= 31` checks in the existing
subinterface/bridge/tunnel/routed-VLAN branches). Not folded into
`global_rows`/`vrf_rows`: those resolve conflicts between a DEVICE's own
several subinterfaces reporting the same address, which does not apply
here - an endpoint's `ipAddrTable` is already its own single source, and
mixing it into that grouping would let an SNMP endpoint's address
silently out-rank or lose to a real device's, or vice versa, for no
principled reason.

**The gate is copied, not shared, and pinned by a parity test.** The
codebase's existing convention for this exact gate (see
`forward_devices.nqe` / `forward_devices_with_netbox_aliases.nqe`,
`test_endpoint_branches_stay_byte_identical`) is duplicate-plus-parity-test
rather than a shared NQE module function, so this follows the same
pattern instead of introducing a new shared-helper refactor mid-feature.
`EndpointEligibilityGateParityTest` compares the gate text (from
`foreach endpoint in network.endpoints` through
`where sync_generic_endpoints || isConsoleServer`) across all three files,
normalized per-line (`.strip()`) because `forward_devices.nqe` nests the
gate one indent level deeper inside its `(...)` union branch while the two
new files hold it in a top-level helper function - a difference in nesting
depth, not in logic.

## Validation

- `invoke ci` on the 4.6 stack.
- The NQE linter (`nqe-lsp-validate`) reports no new diagnostic beyond the
  four pre-existing ones already carried by `forward_devices.nqe`'s
  endpoint branch (two argument-type-mismatch warnings + two
  redundant-`toString()` info notes on `endpoint.name`/
  `endpoint.profileName`, present in all three files identically) and the
  one pre-existing `forward_ip_addresses_ipv4.nqe` device-parallel-multi-ref
  warning (four helper functions each independently loop `network.devices`;
  unrelated to this change, unchanged count).
- `EndpointEligibilityGateParityTest`: the gate text matches byte-for-byte
  (after per-line whitespace normalization) across `forward_devices.nqe`,
  `forward_interfaces.nqe`, and `forward_ip_addresses_ipv4.nqe`.
- `test_query_variants.py`: `forward_interfaces.nqe` now passes the
  opt-in-feature-map check for `sync_endpoints` (it used to be the
  doesn't-support-it example; that role moves to `forward_device_types.nqe`).

## Rollback

Purely additive NQE content behind the existing `sync_endpoints` /
`sync_generic_endpoints` toggles (already off by default); no migration,
no persisted shape change. Reverting either file restores the previous
interface/IP-address query with no data repair - the worst a rollback
does is stop reporting interfaces/addresses NetBox already has no rows
for from this source.

## Decision Log

- **Duplicate the gate, don't extract a shared NQE helper.** A shared
  `netbox_utilities.nqe` function would need `NetworkEndpoint` plus all
  six scope parameters and would still have to be called from
  `forward_devices.nqe`/its alias twin too to actually remove the
  duplication - touching already-shipped 2.9.7 query content mid-feature
  for a refactor, not a fix. Matches this codebase's own established
  precedent for this exact gate (copy + byte-identity test).
- **Always /32, never attempt a real subnet from ipAddrTable.**
  `ipAddrNetMask` lives in the same table but converting a dotted subnet
  mask string into a prefix length has no simple NQE stdlib path, and a
  wrong subnet is worse than an honest host address - this pipeline
  already treats /31/32 as the single-host case everywhere else.
- **type: "other" for every SNMP-derived interface.** Nothing in a bare
  ifTable/ifXTable walk distinguishes physical media; guessing would be
  less honest than the same fallback already used for indeterminate
  device interfaces.
