# Fix: render the SNMP-endpoint import toggle + release 2.4.0

**Date:** 2026-07-06

## Goal
The `sync_endpoints` field (2.3.2) was added to the source form but not to any
`FieldSet`, so NetBox never rendered the toggle — operators (design partner)
could not enable endpoint import from the GUI. Add it to both Parameters
fieldsets and release as the 2.4.0 minor.

## Constraints
- No NQE / apply change; forms-only. ADP org query unchanged (no republish).

## Touched Surfaces
- `forward_netbox/forms.py` — add `"sync_endpoints"` to both source-form
  Parameters `FieldSet`s (SaaS + on-prem).
- `forward_netbox/tests/test_endpoints_import.py` — assert the toggle renders in
  a fieldset.
- `pyproject.toml`, `forward_netbox/__init__.py`, the three README tables,
  `CHANGELOG.md` — version bump to 2.4.0.

## Approach
NetBox `FieldSet` stores fields in `.items`; the toggle exists in `form.fields`
but was absent from every fieldset, so it was silently unrendered. Add it next
to `sync_device_tags` in both fieldsets.

## Validation
`test_endpoints_import` (incl. the new fieldset-render test); full Django suite;
harness + sensitive; `gen_changelog --check`.

## Rollback
Revert the fieldset edits.

## Decision Log
- Minor (2.4.0): the SNMP-endpoint import feature is now actually usable from the
  GUI; the underlying feature shipped in 2.3.2.
- Platform.manufacturer=None is intentional (global platforms since 2.0), not
  changed here — flagged to the partner separately.

## Bundled changes
Fix: the "Import SNMP Endpoints as Devices" toggle now renders on the source
form (it was added in 2.3.2 but never shown), so endpoint import can be enabled
from the GUI.
