# Module Readiness "Ready" Reflects Bays, Not Out-of-Scope Devices (unreleased)

## Goal

Stop the Module Readiness panel/command from reporting `Ready: No` after every
missing module bay has been created, when the only remaining gap is module rows
for devices that are not in NetBox. Partner hit this: created all bays (missing
bays = 0) but `Ready` still showed `No` because 2487 module rows referenced
devices outside NetBox.

## Constraints

- `Ready` should mean "dcim.module sync will not fail for the devices NetBox
  has" — i.e. no missing module bays.
- Rows whose device is not in NetBox still skip with a non-blocking warning and
  are reported separately (missing_device_rows); they must not block readiness.

## Touched Surfaces

- `forward_netbox/utilities/module_readiness.py` — `ModuleReadinessReport.ready`
  is now `missing_bay_rows == 0` (was also requiring `missing_device_rows == 0`).
- `forward_netbox/tests/test_module_readiness.py` — test that 0 missing bays with
  missing-device rows reports `ready=True`.

## Approach

Module bays are the real prerequisite for module sync; a missing-device row is a
device-scope condition (the device is out of the sync's scope or not yet synced)
that the module apply skips harmlessly. So readiness keys on missing bays only.
The UI panel and CLI still surface `missing_device_rows` as a separate count.

## Validation

- `forward_netbox.tests.test_module_readiness` (existing missing-bay case still
  `ready=False`; new 0-bay/missing-device case `ready=True`).
- `forward_netbox.tests.test_scope_module_ui`.
- Full suite; local CI mirror.

## Rollback

Restore the `and self.missing_device_rows == 0` clause and drop the new test.
No schema/data impact.

## Decision Log

- Readiness = no missing bays, not "no skipped rows": skipped (missing-device)
  rows are expected whenever module data spans more devices than NetBox holds, so
  gating readiness on them made `Ready` permanently `No` even when nothing was
  actionable — confusing operators after they created the bays.
