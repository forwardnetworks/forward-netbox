# Validation Org Source Proof

## Goal

Make the validation-org query audit prove source equality for saved query-ID
maps when the Forward repository index includes query IDs but omits concrete
commit IDs.

## Constraints

- Preserve query-ID mode as canonical for NetBox maps.
- Do not weaken the live validation-org gate into a path-only check.
- Keep credentials, network IDs, and raw customer data out of tracked evidence.

## Touched Surfaces

- `forward_netbox/utilities/query_binding_resolution.py`
- `forward_netbox/tests/test_validation_org_query_audit_command.py`
- Validation folder queries in the Forward org repository

## Approach

The live validation-folder audit found all expected query paths, but reported
`published_query_source_unavailable` for every query. The root cause was the
audit asking the client for `head`; the client can satisfy `head` from the
repository index, and index rows do not include query source text.

- Resolve the latest concrete query commit from query history before fetching
  committed source in `builtin_query_repository_sync_summary`.
- Preserve existing missing, stale, lookup-error, and source-unavailable gap
  reporting.
- Add regression coverage for an index row with `queryId` and no
  `lastCommitId`.

## Rollback

Revert the commit that changes `builtin_query_repository_sync_summary`; the gate
will return to the previous conservative `source_unavailable` behavior for
index-only `head` lookups.

## Decision Log

- Use concrete commit resolution instead of path-only validation because
  source equality is required to prove local bundled NQE and saved query IDs are
  aligned.
- Keep source-unavailable as a failing condition when source cannot be fetched
  after concrete commit resolution.

## Validation

- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-validation-fix --test-label forward_netbox.tests.test_validation_org_query_audit_command.ValidationOrgQueryAuditTest --no-keep-runtime`
- `rtk .venv/bin/invoke test-isolated --project-name forward-netbox-querybinding-fix2 --test-label forward_netbox.tests.test_query_binding --no-keep-runtime`
- Live validation org audit against Forward SaaS:
  - `status=pass`
  - `gate_status=proved`
  - `matched_count=44`
  - `missing_count=0`
  - `stale_count=0`
  - `source_unavailable_count=0`
  - `query_contract_summary.status=pass`
