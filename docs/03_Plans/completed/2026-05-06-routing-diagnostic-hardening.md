# Routing Diagnostic Hardening

## Goal

Make beta routing imports more operator-visible by reporting BGP neighbors that are intentionally skipped because Forward does not expose a usable local AS for the native NetBox routing models.

## Constraints

- Keep routing and peering behind `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = True`.
- Keep the diagnostics read-only; do not seed diagnostic queries as import maps.
- Preserve the current native NetBox and Branching sync path.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or live examples.
- Do not add config-derived BGP policy objects until Forward exposes a stable normalized source for them.

## Touched Surfaces

- `forward_netbox/queries/forward_routing_import_diagnostics.nqe`
- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`
- User/reference docs under `docs/`

## Approach

1. Extend the routing diagnostic query with an aggregated `bgp-neighbor-without-local-as` reason.
2. Attach routing diagnostics to `netbox_routing.bgppeer` results as well as address-family, OSPF, and peering-session results.
3. Update tests and docs so the beta behavior is explicit.

## Validation

- Targeted query-registry/query-fetch tests passed:
  - `forward_netbox.tests.test_query_registry.QueryRegistryTest.test_routing_import_diagnostic_query_is_not_seeded_as_import_map`
  - `forward_netbox.tests.test_sync.ForwardMultiBranchPlannerPreflightTest.test_build_plan_records_routing_import_diagnostics`
  - `forward_netbox.tests.test_sync.ForwardMultiBranchPlannerPreflightTest.test_build_plan_attaches_routing_diagnostics_to_bgp_peer_results`
- `invoke harness-check` passed.
- `invoke lint` passed.
- `git diff --check` passed.
- `invoke test` passed: 191 tests.
- `invoke check` passed.
- `invoke harness-test` passed.
- `invoke docs` passed.
- `invoke sensitive-check` passed.

## Rollback

Revert the diagnostic query, diagnostic labels/model set, tests, docs, and this plan. The import maps will continue filtering BGP neighbors without local AS as before.

## Decision Log

- Chosen: aggregate BGP neighbors without local AS by device in diagnostics, because the import query already filters them and operators need counts without high-cardinality row noise.
- Rejected: importing those BGP neighbors with a placeholder ASN. Native NetBox routing peers require a real local ASN, and inventing one would make staged branch review misleading.
- Rejected: adding BGP policy objects in this tranche. Forward's documented normalized NQE model exposes peer, AFI/SAFI, and OSPF state here; route maps, prefix lists, and communities still need a separate config-derived contract.
