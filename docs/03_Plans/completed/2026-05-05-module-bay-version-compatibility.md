# Module Bay Version Compatibility

## Goal

Keep optional native module import compatible across NetBox versions when the
native `ModuleBay` model shape differs.

## Constraints

- Preserve native NetBox `ModuleBay` creation; do not add a plugin-side module
  bay abstraction.
- Do not branch on explicit NetBox version strings when model capability can be
  detected from the model field surface.
- Keep the module import path beta and optional-map enabled.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/tests/test_sync.py`

## Approach

Build module bay creation kwargs from the native model field surface. If the
target NetBox exposes an `enabled` field, create Forward-managed module bays as
enabled. If it does not, keep the older create shape unchanged.

## Validation

- focused `dcim.module` sync tests on NetBox 4.5.9
- `invoke ci`
- NetBox 4.6 branch CI-equivalent after merging the fix

## Rollback

Revert the field-aware module bay create kwargs and this plan.

## Decision Log

- Chosen: capability-detect the native field because it keeps the adapter aligned
  to NetBox's model surface.
- Rejected: hard-code a NetBox version check because it is brittle across
  backports and local development images.
