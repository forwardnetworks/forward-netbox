# 2026-05-20 Tag Selector Persist and 0.9.4.2

## Goal

Fix source form tag selector persistence on save for both include/exclude fields, validate via harness tests, and publish a small patch release.

## Constraints

- Keep NetBox-native source form behavior and existing API contract.
- Do not change query execution behavior or map semantics in this patch.
- Keep release scoped to the tag selector persistence defect and regression coverage.

## Touched Surfaces

- `forward_netbox/forms.py`
- `forward_netbox/tests/test_forms.py`
- version metadata (`pyproject.toml`, `forward_netbox/__init__.py`)

## Approach

1. Add a tolerant multi-choice form field that accepts scalar widget payloads and coerces them into list form.
2. Use that field for:
   - `device_tag_include_tags`
   - `device_tag_exclude_tags`
3. Add regression test covering scalar payload submit and parameter persistence.
4. Bump package version to `0.9.4.2`.

## Rollback

- Revert commit for this patch to restore pre-change form field behavior.
- Re-tag next patch release if rollback is needed after publication.

## Decision Log

- Chose form-layer coercion to handle widget scalar payloads consistently for include/exclude fields.
- Added regression coverage at form test layer to lock persistence behavior.
- Kept release version as a patch increment (`0.9.4.2`) due to narrow blast radius.

## Validation

- `invoke harness-test`
- `invoke scenario-test`
- `invoke ci`

## Expected Outcome

- Saving a source with selected include/exclude tags keeps selections persisted instead of clearing them with `Enter a list of values.`
- Patch release `v0.9.4.2` published with green CI.
