# Routing Evidence Enrichment And Skip Resolution

## Goal

Preserve additional Forward routing evidence in native NetBox plugin comments and reduce skipped routing rows by using conservative NQE-side identity inference, without changing object identity, coalesce behavior, or the beta feature boundary.

## Constraints

- Keep routing and peering behind `PLUGINS_CONFIG["forward_netbox"]["enable_bgp_sync"] = True`.
- Do not create new model targets or config-derived policy objects in this tranche.
- Preserve NetBox-native object writes and Branching-native staging.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or live row examples.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/queries/forward_bgp_*.nqe`
- `forward_netbox/queries/forward_peering_sessions.nqe`
- `forward_netbox/queries/forward_ospf_*.nqe`
- `forward_netbox/queries/forward_routing_import_diagnostics.nqe`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_query_registry.py`
- User/reference docs under `docs/`

## Approach

1. Add BGP peer type to native BGP peer comments.
2. Add Forward AFI/SAFI comments to native BGP address-family rows.
3. Add peer-specific Adj-RIB availability comments to native BGP peer address-family rows.
4. Add OSPF neighbor evidence comments to native OSPF interface rows.
5. Infer missing BGP local AS in NQE from explicit neighbor local AS, process AS, reciprocal peer evidence, or explicit internal-BGP peer AS.
6. Infer OSPF process-level local router ID in NQE from unique reciprocal neighbor evidence so safe local interface rows can import even when one neighbor lacks `remotePeer`.
7. Keep non-inferable rows in the internal routing diagnostics instead of mutating source data or inventing NetBox identities.
8. Update tests and docs.

## Validation

- Targeted routing adapter and registry tests passed in the local NetBox container.
- Live Forward NQE validation showed the safe inference path reduced skipped BGP rows from 579 to 48 and improved OSPF interface coverage.
- `invoke harness-check`
- `invoke lint`
- `invoke check`
- `invoke test`
- `invoke scenario-test`
- `invoke docs`
- `invoke ci`

## Rollback

Revert the comment helper changes, NQE inference changes, tests, docs, and this plan. The routing imports will continue applying the previous native objects and reporting the broader skipped-row diagnostics.

## Decision Log

- Chosen: comments-only enrichment, because the live Forward NQE data has useful evidence that does not map to first-class fields in the installed optional NetBox plugin models.
- Chosen: NQE-side identity inference, because the source queries can safely prove BGP local AS and OSPF local router ID from Forward's structured reciprocal state while keeping NetBox writes native and deterministic.
- Rejected: adding peer-specific Adj-RIB state to shared BGP address-family objects. That state is peer-specific, so it belongs on peer address-family comments.
- Rejected: modeling route maps, prefix lists, communities, or peering networks here because the normalized Forward NQE surface does not expose stable native object definitions for them.
- Rejected: importing rows with synthetic placeholder ASNs or router IDs. That would hide source ambiguity and create NetBox objects that Forward did not prove.
