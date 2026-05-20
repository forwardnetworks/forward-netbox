## Goal
Add a source-level option to prune NetBox objects that fall out of the configured Forward device-tag scope, while preserving existing branching-native and query-id workflows.

## Constraints
- Keep NetBox-native plugin UX and source parameter model.
- Do not require query rewrites for users already on query-id.
- Preserve existing safety behavior for row-level failures and validation.

## Touched Surfaces
- `forward_netbox/forms.py`
- `forward_netbox/models.py`
- `forward_netbox/utilities/model_validation.py`
- `forward_netbox/utilities/query_fetch_execution.py`
- `forward_netbox/tests/test_forms.py`
- `forward_netbox/tests/test_sync.py`

## Approach
1. Add `device_tag_prune_out_of_scope` as a source parameter with form rendering, persistence, and validation.
2. Extend query context with prune intent.
3. Convert tag-filtered-out rows into delete candidates during full fetch paths when prune is enabled.
4. Force full fetch instead of diff for models where prune is enabled and tag scope is active, to deterministically compute out-of-scope deletions.
5. Add tests for form persistence and fetch/delete behavior.

## Rollback
Revert the new source parameter and fetcher prune branch to restore existing local tag-filter-only behavior.

## Decision Log
- Chose source-level toggle (not sync-level) to keep scope behavior tied to source ownership and existing tag controls.
- Chose full-fetch fallback under prune mode for correctness of delete set generation.

## Validation
- `invoke test`
- `invoke check`
- `invoke ci`
