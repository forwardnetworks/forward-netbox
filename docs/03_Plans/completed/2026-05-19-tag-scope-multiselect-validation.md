# Tag Scope Multi-Select Validation Plan (2026-05-19)

## Goal

Validate and harden source-level Forward tag scoping for NetBox sync with multi-select include/exclude tags, include-mode semantics (`any`/`all`), and compatibility for `query`/`query_id` execution.

## Constraints

- Keep NetBox-native workflow surfaces.
- Preserve backward compatibility for existing single-tag source parameters.
- Do not break raw `query`/`query_id` flows when query parameters are unsupported by the target Forward query.

## Touched Surfaces

- `forward_netbox/forms.py`
- `forward_netbox/api/views.py`
- `forward_netbox/models.py`
- `forward_netbox/utilities/model_validation.py`
- `forward_netbox/utilities/sync_facade.py`
- `forward_netbox/utilities/query_fetch_execution.py`
- `forward_netbox/tests/test_forms.py`
- `forward_netbox/tests/test_api_views.py`
- `forward_netbox/tests/test_sync.py`

## Approach

1. Add source-form controls for include/exclude multi-select tags and include-match mode.
2. Add `available-tags` API lookup from Forward source/network.
3. Validate and mask new source parameter keys.
4. Build scoped device-set query with `any`/`all` include semantics and exclude semantics.
5. Keep the query path parameter-native and fail fast on unsupported query shapes.
6. Validate with containerized tests, UI smoke checks, and live scoped ingestion.

## Rollback

- Revert the tag multi-select fields and API endpoint.
- Revert query-fetch scoping changes and related execution wiring.
- Restore prior single-tag local-scope behavior.

## Decision Log

- Keep `local` filter mode as default for broad compatibility.
- Support `query_parameters` mode for orgs that want parameterized query execution.
- Maintain old `device_tag_include`/`device_tag_exclude` keys as compatibility inputs while storing canonical list-based keys.

## Validation

1. Run `invoke harness-check`, `invoke harness-test`, `invoke lint`, `invoke check`, `invoke test`, `invoke docs`.
2. Run targeted test slices for updated forms/API/query compatibility behavior.
3. Run Playwright source-form smoke checks for tag selector rendering and wiring.
4. Run a live tag-scoped ingestion and confirm scoped counts in logs.
