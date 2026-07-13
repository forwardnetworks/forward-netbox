# ACI APIC CIMC Inventory

## Goal

Add a bounded APIC CIMC ingestion path that maps Forward-collected APIC server
hardware evidence into native NetBox `dcim.inventoryitem` rows without broad
custom-command parsing or additional per-device API calls.

## Constraints

- NQE remains the source of truth for row normalization; Python must not add a
  parallel APIC CIMC parser.
- The map must be optional and disabled by default because it depends on the
  APIC custom command `moquery -c eqptCh -a all`.
- The map must use `forward_netbox_shard_keys` and must not use Forward column
  filters.
- The query must not project raw custom command responses into NetBox rows,
  docs, fixtures, support bundles, or tests.
- Saved-query execution must tolerate Forward org repository head references
  that resolve to `head` or abbreviated commit identifiers; those values are
  not valid `/api/nqe` `commitId` payloads.

## Touched Surfaces

- `forward_netbox/queries/forward_aci_apic_cimc_inventory.nqe`
- `forward_netbox/utilities/query_registry.py`
- `forward_netbox/utilities/plugin_integrations/registry.py`
- `forward_netbox/utilities/forward_api_impl.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_forward_api.py`
- `forward_netbox/tests/fixtures/aci_discovery_expected.json`
- `docs/01_User_Guide/configuration.md`
- `docs/02_Reference/built-in-nqe-maps.md`
- `docs/02_Reference/model-mapping-matrix.md`

## Approach

1. Add a new optional built-in map named `Forward ACI APIC CIMC Inventory`
   targeting `dcim.inventoryitem`.
2. Parse `CISCO_APIC_CONTROLLER_DETAIL` output to resolve APIC node ID, pod ID,
   and NetBox device name.
3. Parse only the exact custom command `moquery -c eqptCh -a all`.
4. Join custom `eqpt.Ch` rows to APIC controller rows on pod ID and node ID.
5. Emit one inventory item named `CIMC` per APIC controller with non-empty CIMC
   version, model, and serial evidence.
6. Add the query to optional ACI integration metadata and user/reference docs.
7. Guard Forward API NQE payload construction so `head` and abbreviated hex
   commit identifiers are not sent as `commitId` values.

## Validation

- `invoke lint`: passed.
- `invoke docs`: passed.
- Targeted isolated NetBox tests passed:
  - `ForwardClientTest.test_run_nqe_query_omits_abbreviated_hex_commit_id`
  - `ForwardClientTest.test_run_nqe_query_omits_head_commit_id`
  - `QueryRegistryTest.test_aci_discovery_queries_match_fixture_contract`
  - `QueryRegistryTest.test_optional_aci_maps_are_seeded_disabled`
- Live ORG validation against Forward SaaS network `249852`, latest processed
  snapshot `1313884`:
  - Raw local NQE execution returned 3 CIMC inventory rows.
  - Parameterized shard-key execution returned exactly 1 matching row for a
    single APIC device shard key.
  - Saved org query execution returned 3 CIMC inventory rows from query ID
    `<redacted-query-id>`.
  - The saved-query proof used one NQE page and reported no HTTP failures,
    retries, or 429 responses.

## Rollback

Remove the optional query map, delete the shipped NQE file, remove the ACI
integration metadata entry, remove the docs/test fixture additions, and delete
or supersede `/forward_netbox_validation/forward_aci_apic_cimc_inventory` in
the Forward org repository. The API commit-ID guard can remain independently
because it prevents invalid SaaS payloads for all query-path maps.

## Decision Log

- Rejected `moquery -c compatRsSuppHw -a all` because it is a large compatibility
  catalog and does not represent current APIC hardware inventory.
- Rejected requiring `eqptFlash` or `eqptBoard` in the first tranche because
  `eqptCh` already provides CIMC version, model, serial, and vendor with the
  smallest sufficient source surface for this use case.
- Rejected Python-side CIMC parsing because it would create a second
  normalization contract outside the NQE map.
