# Optional Plugin Dependency Primer CI Fix

## Goal

Keep dependency-cache priming opportunistic so optional plugin models can be skipped
or reported by their normal row adapters instead of aborting sync setup when the
optional NetBox plugin is not installed.

## Constraints

- Preserve NetBox 4.5.9 and 4.6.1 compatibility.
- Do not add compatibility fallbacks for old Forward query shapes.
- Do not mask row-level adapter failures; missing optional plugins still need to be
  reported through ingestion issues and failed-row statistics.
- Keep Forward API/NQE behavior unchanged.

## Touched Surfaces

- `forward_netbox/utilities/sync_primitives.py`
- `forward_netbox/tests/test_sync.py`

## Approach

Wrap optional plugin dependency-cache primers with a small helper that catches
`ForwardQueryError` and returns an empty primer summary. This keeps cache priming as
a performance optimization only. The actual model adapter remains responsible for
validating optional plugin availability and recording row failures.

Add a regression test proving that optional routing-plugin primer failure does not
raise from `prime_dependency_lookup_caches()`.

## Validation

- `python manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_dependency_lookup_cache_primes_routing_interface_alias_candidates forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_dependency_lookup_cache_skips_optional_plugin_priming_failure forward_netbox.tests.test_sync.ForwardSyncRunnerTest.test_bgp_peer_adapter_records_failure_when_optional_plugin_is_missing`
- `python manage.py test --keepdb --noinput forward_netbox.tests`
- `invoke check`
- `invoke lint`

## Rollback

Revert the helper and regression test. No migrations or persisted state changes are
introduced.

## Decision Log

- Did not special-case OSPF or BGP by model name. The broader invariant is that
  optional dependency-cache primers are opportunistic and must not own sync failure
  behavior.
- Did not suppress adapter failures. Adapter paths still record missing optional
  plugin or dependency failures at row level.
