# Row Failure Resilience and Global IP Duplicate Canonicalization

## Goal

Prevent `ipam.ipaddress` imports from failing when Forward reports multiple global-table interface rows for the same host IP with different masks, and make predictable per-row apply/delete failures record ingestion issues without aborting the rest of the shard.

## Constraints

- Keep the fix in shipped NQE where possible.
- Preserve NetBox-native row shape and interface assignment behavior.
- Do not introduce a new importer path or a Python-side normalization escape hatch.
- Keep preflight, query execution, branch creation, and merge failures fail-fast; only row-scoped apply/delete failures should continue.
- Keep committed evidence free of customer identifiers, network IDs, and snapshot IDs.

## Touched Surfaces

- `forward_netbox/queries/forward_ip_addresses.nqe`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`
- `docs/02_Reference/built-in-nqe-maps.md`
- `docs/02_Reference/model-mapping-matrix.md`
- `docs/01_User_Guide/troubleshooting.md`

## Approach

Canonicalize global-table IP rows by bare host IP before import and keep the most specific prefix length for the surviving row. Preserve the existing `address + vrf` behavior for VRF-scoped rows.

Add a NetBox adapter guard for pre-existing global IP objects so reruns update the existing host entry instead of creating a conflicting row.

Wrap individual row apply/delete operations in a transaction savepoint and keep processing after expected row-scoped exceptions. Record the failed row as an ingestion issue and increment failed/skipped statistics, while leaving model preflight and infrastructure errors outside this loop unchanged.

Add contract tests for the query text, the IP adapter guard, and the shared apply/delete row loops. Update the docs to explain that the plugin now collapses duplicate global-table host IPs deterministically and records isolated row failures without stopping the shard.

## Validation

- `invoke harness-check`
- `invoke harness-test`
- `invoke sensitive-check`
- `invoke lint`
- `invoke check`
- `invoke test`
- `invoke docs`
- `invoke ci`
- Forward Org Repository draft validation, one-row execution, commit, and post-commit validation for the shipped `forward_ip_addresses.nqe` query.

## Rollback

Restore the previous `forward_ip_addresses.nqe` global-table projection and previous `_apply_model_rows`/`_delete_model_rows` raise-on-row-error behavior, remove the new regression tests, and revert the troubleshooting/documentation notes if the row-continuation path causes an unexpected import change.

## Decision Log

- Global-table IP rows need host-level canonicalization because NetBox rejects duplicate global IP objects even when Forward presents the same host with different masks.
- The adapter should still update the existing NetBox object when a global host already exists, so the import remains stable across reruns and pre-existing NetBox state.
- Row-scoped NetBox validation and lookup failures should not abort a shard because the operator needs the rest of the rows staged plus a precise issue record for the bad row.
- Release this as `0.6.2` because it is a patch-level correction to `ipam.ipaddress` ingestion behavior and shard resilience, without expanding the public model surface.
