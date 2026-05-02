# Prefix Host-Route Exclusion Release (v0.4.0)

## Goal

Prevent host routes from importing as `ipam.prefix` objects by excluding IPv4 `/32` and IPv6 `/128` rows in shipped prefix queries, then release the fix.

## Constraints

- Keep behavior NetBox-native and Branching-native.
- Do not include customer identifiers or credentials in committed artifacts.
- Keep host routes available for the IP address ingest path; only prefix ingest should change.
- Pass repo harness and test gates before release.

## Touched Surfaces

- `forward_netbox/queries/forward_prefixes_ipv4.nqe`
- `forward_netbox/queries/forward_prefixes_ipv6.nqe`
- `forward_netbox/tests/test_query_registry.py`
- `pyproject.toml`
- `forward_netbox/__init__.py`
- `forward_netbox/utilities/forward_api.py`
- `README.md`
- `docs/README.md`
- `docs/01_User_Guide/README.md`

## Approach

1. Tighten built-in prefix query predicates:
   - IPv4 from `<= 32` to `< 32`
   - IPv6 from `<= 128`/host-inclusive behavior to `< 128`
2. Add a regression test that asserts shipped query text excludes host-route lengths.
3. Validate against a live smoke dataset by comparing:
   - rows returned by shipping prefix queries
   - rows returned by host-only variants (`== 32`, `== 128`)
4. Bump release metadata and docs for `v0.4.0` and NetBox `4.5.9`.

## Validation

- `invoke test` (passes, 109 tests)
- Live smoke query check showing host routes exist in source data while shipping prefix queries exclude those lengths.
- `invoke ci` before release publish.

## Rollback

- Revert this commit and republish previous tag/package if needed.
- No migration/state rollback required.

## Decision Log

- Rejected: keep host routes in prefix query and filter later in sync adapters.
  - Reason: query-level exclusion is simpler, cheaper, and aligns with intended model mapping boundaries.
