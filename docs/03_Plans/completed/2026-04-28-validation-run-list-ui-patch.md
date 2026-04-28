# Validation Run List UI Patch

## Goal

Patch the validation-run list page regression found after the 0.3.0 release.
The list route must render seeded validation records without asking NetBox for an
unsupported edit URL on read-only validation records.

## Scope

- Remove unsupported edit actions from `ForwardValidationRunTable`.
- Add Playwright coverage for `/plugins/forward/validation-run/`.
- Publish as `0.3.0.1` after local and GitHub CI pass.

## Constraints

- Validation runs are reporting records, not operator-editable configuration.
- Keep the fix in NetBox-native table/action behavior.
- Do not change validation-run model semantics or persistence.

## Touched Surfaces

- `forward_netbox/tables.py`
- `scripts/playwright_forward_ui.mjs`
- Release metadata and user-facing install documentation.

## Approach

- Limit `ForwardValidationRunTable` row actions to the supported delete action.
- Exercise the validation-run list route in the Playwright harness before opening
  validation detail.
- Ship as a patch release because 0.3.0 already published the validation-run UI.

## Validation

- `python -m compileall -q forward_netbox scripts`
- `git diff --check`
- `pre-commit run --all-files`
- `invoke playwright-test`
- `invoke ci`

## Rollback

- Revert the patch commit and unpublish replacement guidance by superseding with a
  newer patch release if the published package has already been consumed.

## Decision Log

- Keep validation runs read-only instead of adding an edit view, because the stored
  data is generated evidence for a sync attempt.
- Add route-level browser coverage because the regression only appeared while
  rendering the list table actions.
