# Filter single-map query selectors by NetBox model

## Goal

Make the manual NQE map edit flow as intuitive as bulk edit by filtering the
query dropdowns to the selected NetBox model.

## Constraints

- Keep the change NetBox-native and reuse the existing `APISelect` pattern.
- Preserve repository-path and direct-query-ID execution behavior.
- Avoid adding a second bespoke query-selection surface.

## Touched Surfaces

- `forward_netbox/forms.py`
- `forward_netbox/api/views.py`
- `forward_netbox/tests/test_forms.py`
- `forward_netbox/tests/test_api_views.py`

## Approach

- Pass the selected `netbox_model` through the single-map query selector
  widgets as a dynamic `model_string` query param.
- Teach the available-queries API to accept either a NetBox content type id or
  a model string so the selector can resolve the current model consistently.
- Keep the same query path/query ID execution semantics; only the chooser
  becomes model-aware.

## Validation

- `poetry run invoke test`
- `poetry run invoke harness-check`
- UI verification via the existing Playwright forward UI harness

## Rollback

Revert the selector param wiring and the model-string fallback in the API
view. No stored map data changes are required.

## Decision Log

- Rejected leaving the selector unfiltered because it makes manual edits harder
  to use and diverges from the bulk-edit workflow.
