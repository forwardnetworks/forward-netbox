# IP Address Missing Interface Skips

## Goal

Fast bootstrap can complete an `ipam.ipaddress` shard with row failures when a
Forward IP-address row targets an interface that was not imported into NetBox.
The affected rows are recoverable dependency misses: the correct behavior is to
preserve visibility while continuing the rest of the IP-address workload.

## Constraints

- Keep NQE as the source of truth for row shape and normalization.
- Do not include customer identifiers, network IDs, snapshot IDs, or screenshots.
- Preserve query-id execution for large deployments and later NQE diffs.
- Keep NetBox adapter behavior native: skip recoverable dependency misses and
  record operator-visible warnings.

## Touched Surfaces

- `forward_netbox/queries/forward_ip_addresses.nqe`
- `forward_netbox/api/views.py`
- `forward_netbox/forms.py`
- `forward_netbox/utilities/forward_api.py`
- `forward_netbox/tests/test_api_views.py`
- `forward_netbox/tests/test_forms.py`
- `forward_netbox/tests/test_forward_api.py`
- `forward_netbox/utilities/sync_ipam.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/utilities/query_binding.py`
- `scripts/playwright_forward_ui.mjs`
- `docs/01_User_Guide/configuration.md`
- `docs/02_Reference/built-in-nqe-maps.md`

## Approach

- Filter the shipped Forward IP Address NQE candidates through the importable
  Forward interface set before deterministic IP identity selection.
- Treat post-query missing target interfaces as aggregated `ipam.ipaddress`
  skip warnings instead of `ForwardSearchError` row failures.
- Keep the existing unassignable network/broadcast skip behavior unchanged.
- Update query registry and sync tests to lock the behavior.
- Add UI selectors that use a selected Forward Source to list committed Org
  Repository or Forward Library queries from Forward.
- Expand query selection into a Source -> Repository -> Folder -> Query Path
  -> Commit flow so operators can select Org Repository or Forward Library
  queries and optionally pin a revision.
- Add an explicit `Repository Query Path`, `Direct Query ID`, and `Raw Query
  Text` form mode and clear the inactive model fields before save.
- Add native bulk-edit query binding fields that use a committed Forward
  repository folder to populate explicit, per-selected-map query path choices,
  then switch model-compatible selections to portable query-path mode.

## Validation

- `invoke harness-check` passed.
- `invoke harness-test` passed.
- `invoke lint` passed.
- `invoke ci` passed.
- `invoke test` passed.
- `invoke docs` passed.
- `python scripts/check_sensitive_content.py` passed.
- Live NQE validation against the configured Forward source passed for the
  local query text and the committed query path.
- Live query selector API validation against the configured Forward source
  returned the expected Org Repository query choice and commit list.
- Focused tests covered repository folder hierarchy, query selection, commit
  selection, form mode cleanup, and native bulk-edit query-path binding.

## Rollback

- Revert the local code and docs commit.
- Re-publish the previous Forward Org Repository query source for the affected
  `Forward IP Addresses` query if needed.
- Re-run native bulk edit query binding against the prior Forward query folder,
  or clear `query_path` and restore raw query text from the saved map
  definition.

## Decision Log

- Rejected mutating IP addresses in Python: interface target identity should
  come from NQE and NetBox adapters should only apply or skip rows.
- Rejected treating every missing interface as a hard row failure: these rows
  are recoverable dependency misses and should not make large bootstrap runs
  appear failed when the rest of the shard applied.
