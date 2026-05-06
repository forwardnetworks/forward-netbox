# Routing Import Diagnostics

## Goal

Add operator-visible diagnostics for optional routing and peering imports so beta routing rows that are intentionally skipped are counted and explained during preflight/planning instead of failing silently.

## Constraints

- Keep the routing surface behind `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = True`.
- Keep diagnostics read-only; do not seed them as import maps and do not create, update, or delete NetBox objects.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or live row examples.
- Preserve the existing NetBox-native and Branching-native sync path.

## Touched Surfaces

- `forward_netbox/queries/`
- `forward_netbox/utilities/query_registry.py`
- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/tests/`
- `docs/02_Reference/`

## Approach

1. Add an internal NQE diagnostic query for routing rows that the beta maps cannot import safely.
2. Report unsupported BGP address-family values and OSPF neighbor rows that lack the inferred peer/reverse-peer data required to build native NetBox routing objects.
3. Attach aggregate diagnostic counts and capped examples to enabled routing model results using the same result shape as the existing IP-address diagnostics.
4. Filter unsupported BGP address-family values in the shipped BGP AF maps so unsupported AF rows are reported by diagnostics instead of failing row application.
5. Normalize Forward `L3VPN_*` AFI/SAFI values to the native `netbox-routing` `vpnv4-*` and `vpnv6-*` address-family choices.
6. Add a global-table coalesce fallback for `netbox_routing.bgpaddressfamily` rows where `vrf` is null.

## Validation

- Live NQE diagnostic query executed successfully against the validation dataset with sanitized output: 415 diagnostic rows, all `ospf-neighbor-without-remote-peer` after BGP unsupported AF aggregation/filtering.
- Live NQE samples for `Forward BGP Address Families` and `Forward BGP Peer Address Families` returned 10 rows each with the expected import fields after AF filtering.
- Targeted tests passed:
  - `QueryRegistryTest.test_optional_bgp_maps_are_seeded_disabled`
  - `QueryRegistryTest.test_routing_import_diagnostic_query_is_not_seeded_as_import_map`
  - `ForwardMultiBranchPlannerPreflightTest.test_build_plan_records_routing_import_diagnostics`
  - `ForwardSyncRunnerTest.test_bgp_peer_address_family_adapter_creates_native_address_family`
- `invoke lint` passed.
- `invoke harness-check` passed.
- `invoke check` passed.
- `invoke test` passed: 190 tests.
- `invoke docs` passed.
- `invoke ci` passed, including Docker rebuild, Django tests, Playwright UI harness, docs, and package build.
- Sensitive scan over tracked docs/source paths found no customer identifiers, credentials, live network IDs, or live snapshot IDs.

## Rollback

Revert the diagnostic query, registry helper, fetcher wiring, BGP AF filtering, AF normalization, coalesce fallback, tests, docs, and this plan. Existing routing import maps continue to work without the diagnostic path, except unsupported BGP AF rows would again fail during adapter application.

## Decision Log

- Rejected: creating a visible/importable diagnostic map. The operator-facing need is counts and examples attached to the sync result, not additional NetBox object rows.
- Rejected: reporting every unsupported BGP RIB AF occurrence as a diagnostic row. Live validation showed that raw per-occurrence output is too noisy; the implemented query aggregates unsupported AF rows and filters import maps to supported/native choices.
