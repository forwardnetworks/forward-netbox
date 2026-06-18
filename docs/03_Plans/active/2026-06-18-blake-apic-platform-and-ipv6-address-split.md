# APIC Controller Platform Fix + IPv4/IPv6 IP Address Map Split

## Goal

Resolve two issues Partner reported from a live 1.5.2 CustomerOrg sync:

1. APIC **controllers** are classified onto the `ACI` NetBox platform alongside
   ACI leaf/spine switches, so their distinct software version (controllers
   6.0.6 vs switches 16.0.6) cannot be modeled separately. Controllers must land
   on the `APIC` platform.
2. Disabling the `Forward IPv6 Prefixes` map does not stop IPv6 **IP addresses**
   from syncing, because addresses ship as a single combined map. IPv4 and IPv6
   addresses must be independently toggleable, mirroring the existing split
   prefix maps.

## Constraints

- NQE query changes must be pushed to org-mode deployments (e.g. CustomerOrg) after
  release; flag this to operators.
- No customer data, credentials, or network IDs in repo/tests.
- The address split must not change which addresses are collected when both
  family maps are enabled (parity with the old combined map).
- Existing installs must not double-collect addresses after upgrade; the old
  combined built-in map is removed via data migration.
- APIC controller detection must take precedence over the ACI command-output
  fallback without regressing ACI switch or CIMC classification.

## Touched Surfaces

- `forward_netbox/queries/netbox_utilities.nqe` — add `deviceIsApicController`;
  `normalizeDevicePlatformName` checks it before the `isAciDevice` fallback.
- `forward_netbox/queries/forward_ip_addresses_ipv4.nqe`,
  `forward_netbox/queries/forward_ip_addresses_ipv6.nqe` — new per-family maps.
- `forward_netbox/queries/forward_ip_addresses.nqe` — removed (replaced).
- `forward_netbox/utilities/query_registry.py` — replace the combined
  `Forward IP Addresses` built-in with `Forward IPv4 IP Addresses` and
  `Forward IPv6 IP Addresses`.
- `forward_netbox/migrations/0023_split_ip_address_maps.py` — delete the old
  built-in row on upgrade (built_in=True only).
- `forward_netbox/tests/test_query_registry.py` — controller-precedence
  assertion; per-family address-query assertions; seeded-name/output-field
  contract updates.
- `docs/02_Reference/built-in-nqe-maps.md`,
  `docs/01_User_Guide/configuration.md` — document the split maps.

## Approach

APIC: `deviceIsApicController` returns true when a device carries the
`CISCO_APIC_CONTROLLER_DETAIL` command (the controller's own output). It is
OR'd with `isApicPlatform` as the first branch of `normalizeDevicePlatformName`
so controllers resolve to `APIC` before `isAciDevice` (which trips on the same
command via `deviceHasAciCommandOutputs`) can pull them onto `ACI`.

Address split: the existing combined query already had per-family helper
functions. Each new file keeps only its family's helpers plus the shared
host-IP dedup/aggregation pipeline. A host IP belongs to exactly one family, so
per-family dedup is equivalent to the combined dedup. Both maps are enabled by
default; operators disable either independently. A data migration removes the
old combined built-in row; `post_migrate` seeds the two replacements.

## Validation

- `forward_netbox.tests.test_query_registry` (controller precedence; per-family
  address filters; seeded names; output-field contract).
- Full `forward_netbox.tests` suite (1129 tests).
- `manage.py migrate forward_netbox` forward + reverse; `makemigrations
  --check`; verify only the two family maps remain seeded.
- `invoke harness-check`, lint.

## Rollback

Revert the listed files and the migration. Reversing migration 0023 is a no-op;
re-adding the combined map to the registry restores the prior behavior on the
next `post_migrate`. No schema change, no data loss (address rows are unaffected;
only the map definitions change).

## Decision Log

- Controller detection keyed on `CISCO_APIC_CONTROLLER_DETAIL` rather than OS
  string: APIC controllers can report a generic NXOS OS that lacks "apic", so
  the command output is the reliable signal — and it is exactly what the ACI
  fallback was over-matching.
- Address maps split into two self-contained files (not a shared imported
  module) to match the established `forward_prefixes_ipv4/ipv6` convention.
- Old built-in row deleted unconditionally for `built_in=True`: any query_id on
  a built-in map is the plugin's own org binding, not user logic; truly custom
  logic lives on `built_in=False` maps which take precedence and are untouched.
