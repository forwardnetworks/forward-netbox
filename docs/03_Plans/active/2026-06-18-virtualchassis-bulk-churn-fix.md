# Virtual Chassis Bulk Apply: Stop Re-Assigning Unchanged Members

## Goal

Close the last bulk-apply update-churn gap. `bulk_orm_apply_virtualchassis`
re-PATCHed every VC member device on every sync — it set
`virtual_chassis`/`vc_position` and queued the device for `bulk_update` without
checking whether the device was already a member at that position.

## Constraints

- No change to VC create/domain-update handling (already change-detected) or to
  the position-conflict / dependency-skip logic.
- Skip the write only when the device is already assigned to the same VC and
  position; still record the slot as occupied so conflict detection holds.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` — member-assignment loop in
  `bulk_orm_apply_virtualchassis` short-circuits unchanged members and counts
  `unchanged`.
- `forward_netbox/tests/test_bulk_adapter_parity.py` — re-apply test asserting
  no `Device.bulk_update` and an `unchanged` outcome on the second run.

## Approach

Before mutating, compare `device.virtual_chassis_id == vc.pk` and
`device.vc_position == position`. If both match, mark the position occupied,
increment the `unchanged` statistic, and continue without queuing the device.
Otherwise assign and queue as before.

## Validation

- `forward_netbox.tests.test_bulk_adapter_parity`
  (`test_virtualchassis_reapply_makes_no_writes`).
- Full `forward_netbox.tests` suite.
- `invoke harness-check`, lint.

## Rollback

Revert the member-assignment block to the unconditional assignment and delete
the test. No schema, data, or migration impact.

## Decision Log

- Completes the churn sweep started for macaddress/interface/ipaddress: every
  bulk apply path that writes existing rows now compares first, so a steady-state
  sync issues zero writes and reports accurate `unchanged` counts.
