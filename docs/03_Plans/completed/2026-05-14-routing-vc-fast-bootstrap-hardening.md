# Routing, Virtual Chassis, and Fast Bootstrap Hardening

## Goal

Resolve customer-reported sync failures where optional routing rows produced ambiguous BGP scope lookups, invalid ASN validation failures, and a failed fast-bootstrap status that prevented later diff-based runs from using the completed ingestion as a baseline.

## Constraints

- Keep NQE as the source of truth for data normalization.
- Keep NetBox-native model writes and NetBox plugin model validation.
- Do not make optional beta routing rows block the core inventory baseline.
- Keep virtual chassis conservative: rows without a real NetBox position should be no-op/skipped rather than treated as true membership.
- Do not include customer identifiers, network IDs, snapshot IDs, screenshots, or credentials in committed tests/docs.

## Touched Surfaces

- `forward_netbox/utilities/sync_routing.py`
- `forward_netbox/utilities/sync_runner_adapters.py`
- `forward_netbox/utilities/sync_device.py`
- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/utilities/fast_bootstrap_executor.py`
- `forward_netbox/queries/forward_bgp_*.nqe`
- `forward_netbox/queries/forward_peering_sessions.nqe`
- `forward_netbox/queries/forward_routing_import_diagnostics.nqe`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_query_registry.py`

## Approach

Use exact `(router, vrf)` lookup for BGP scopes instead of the generic coalesce helper that drops `None` values. This prevents `router`-only lookups from matching multiple scopes on routers with multiple VRFs.

Filter BGP and peering NQE rows where local or peer ASN is lower than 1, and keep a Python-side ASN guard so stale or custom queries fail as row-level `ForwardQueryError` instead of surfacing a lower-level NetBox validation error.

Treat virtual chassis rows with a device but without `vc_position` as skipped no-op rows. The conservative bundled virtual chassis query still emits no rows by default, and stale/custom rows without positions no longer create a VC object or fail the sync.

Allow fast bootstrap to complete and mark a baseline-ready ingestion when all recorded issues are from optional models. Core-model issues still raise `SyncError` and keep the ingestion from becoming the diff baseline.

## Validation

- `python manage.py test --keepdb --noinput` focused tests for BGP scope lookup, ASN guard, VC no-op, fast-bootstrap optional issue behavior, and query registry assertions.
- `python manage.py test --keepdb --noinput forward_netbox.tests.test_sync.ForwardSyncRunnerTest forward_netbox.tests.test_sync.ForwardFastBootstrapExecutorTest forward_netbox.tests.test_query_registry.QueryRegistryTest`
- Live Forward API execution of updated BGP peer, BGP AF, BGP peer AF, peering session, and routing diagnostic NQE files with a small row limit.
- `poetry run invoke harness-check`
- `poetry run invoke lint`
- `git diff --check`

## Rollback

Revert the code and query changes together. If org-repository NQE was published, republish the prior bundled query versions or rebind affected maps to raw bundled query text from the previous package.

## Decision Log

- Rejected: globally preserve `None` in all generic coalesce lookups. That could change matching behavior for unrelated models with optional coalesce fields.
- Rejected: fail fast bootstrap on optional beta routing issues. That preserves strictness but causes large initial loads to lose their diff baseline after non-core routing skips.
- Rejected: interpret Forward HA/VPC/MLAG rows without `vc_position` as NetBox virtual chassis membership. NetBox virtual chassis requires positional membership and Forward control-plane relationships are not equivalent by default.
