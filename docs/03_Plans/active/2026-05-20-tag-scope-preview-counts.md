# 2026-05-20 Tag Scope Preview Counts

## Goal

Expose how many devices are in-scope for configured include/exclude source tags before and during sync.

## Constraints

- Keep behavior NetBox-native and compatible with existing source form/workflow.
- Do not require custom operators to run manual queries for scope visibility.
- Preserve current sync semantics; this is observability/UX only.

## Touched Surfaces

- `forward_netbox/models.py`
- `forward_netbox/views.py`
- `forward_netbox/templates/forward_netbox/forwardsource.html`
- `forward_netbox/utilities/query_fetch_execution.py`
- `forward_netbox/tests/test_models.py`

## Approach

1. Add source-level tag scope preview computation:
   - total devices
   - matched devices
   - excluded by scope
2. Render preview on Forward Source detail page.
3. Enrich preflight tag-scope log line to include total/matched/excluded counts.
4. Add regression tests for preview counts and missing-snapshot error handling.

## Validation

- `invoke ci`
- Source detail page renders tag scope preview without errors.
- Preflight logs include matched/total/excluded counts when source tag scope is configured.

## Rollback

- Revert this patch to remove preview panel and preflight count expansion.
- No schema/data migration rollback needed.

## Decision Log

- Chose source detail panel over form inline async widget to minimize UI complexity and keep native object-view pattern.
- Chose preflight log augmentation to provide runtime confirmation for long-running sync jobs.
- Kept query shape aligned with existing device-scope filter logic for consistency.
