# IP Address Network and Broadcast Skip

## Goal

Prevent `ipam.ipaddress` ingestion from failing when Forward reports an
interface address that NetBox cannot assign to an interface, such as a subnet
network ID or IPv4 broadcast address.

## Constraints

- Keep Forward-reported addresses unchanged; do not infer or mutate a host
  address.
- Keep the primary behavior in the shipped NQE map so rows that NetBox cannot
  model are excluded before branch planning and ingestion.
- Keep a Python adapter guard for custom or query-ID maps that still emit these
  rows.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or
  sampled rows.

## Touched Surfaces

- `forward_netbox/queries/forward_ip_addresses.nqe`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/utilities/query_registry.py`
- `forward_netbox/queries/forward_ip_addresses_unassignable_diagnostics.nqe`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`
- `docs/02_Reference/built-in-nqe-maps.md`
- `docs/02_Reference/model-mapping-matrix.md`

## Approach

1. Updated `Forward IP Addresses` to filter subnet network IDs and IPv4
   broadcast addresses before projecting NetBox `ipam.ipaddress` rows.
2. Preserved point-to-point endpoint prefixes that NetBox accepts, including
   IPv4 `/31` and IPv6 `/127`.
3. Added an adapter-level aggregate skip warning for query rows that still emit
   unassignable addresses.
4. Added an internal read-only diagnostic query that reports filtered counts and
   capped examples when `ipam.ipaddress` is enabled.
5. Updated query-registry and sync regression tests plus reference docs.

## Rollback

Revert this patch set to restore prior behavior where the shipped IP address
query emits all Forward interface addresses and NetBox validation can fail an
ingestion shard for unassignable network or broadcast addresses.

## Decision Log

- Chosen: query-side filtering plus adapter-side aggregate skip protection so
  built-in maps avoid invalid rows and custom/query-ID maps fail soft.
- Chosen: a separate internal diagnostic query instead of a seeded NQE map so
  operators can see filtered counts without creating another import surface.
- Rejected: mutating the reported IP to a host address because the plugin cannot
  infer a NetBox-native host identity from a device configuration that uses a
  subnet network ID.
- Rejected: adapter-only handling because branch planning should not count rows
  that are known to be unmodelable in NetBox.
- Rejected: logging every filtered row because large datasets can produce noisy
  job logs; examples are capped and the remaining rows are aggregated.

## Validation

- Confirmed in local NetBox that assigned interface IPs reject `/28` network and
  broadcast addresses, accept normal `/28` hosts, accept IPv4 `/31` endpoints,
  and accept IPv6 `/127` endpoints.
- `python manage.py test --keepdb --noinput` focused tests:
  - `ForwardSyncRunnerTest.test_apply_ipam_ipaddress_skips_unassignable_network_and_broadcast_addresses`
  - `ForwardSyncRunnerTest.test_apply_ipam_ipaddress_allows_point_to_point_endpoint_addresses`
  - `QueryRegistryTest.test_ipaddress_query_excludes_unassignable_interface_addresses`
  - `QueryRegistryTest.test_ipaddress_unassignable_diagnostic_query_is_not_seeded_as_import_map`
  - `ForwardMultiBranchPlannerTest.test_build_plan_records_unassignable_ipaddress_diagnostics`
- Live Forward API query validation passed for the updated bundled
  `Forward IP Addresses` query with `limit=1` and the internal diagnostic query
  with `limit=5` after resolving the numeric latest processed snapshot.
- Live diagnostic execution also passed through the plugin `fetch_all=True` path
  and returned aggregate reason counts without storing sample rows.
