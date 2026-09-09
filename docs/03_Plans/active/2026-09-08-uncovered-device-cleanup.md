# Uncovered devices: making the bucket actionable

## Goal

Let an operator delete the devices this sync created that Forward no longer
reports, without giving them a way to delete devices that still exist.

## Why

A customer reported 552 uncovered devices and a count that keeps growing.
`2026-09-02-uncovered-why-and-trend.md` answered why each one is uncovered and
whether the number is rising. It did not make any of them removable, and
nothing else does either.

The orphan prune looks like it should. It does not. It acts on
`out_of_scope = (previously_managed & netbox_names) - tagged_names`, which is
devices claimed by the run that produced the current result. A device created
by an earlier run and dropped from scope since is never in that set. The
customer's shape is recorded in `test_uncovered_absence_and_trend.py` as
"orphans zero, owned-uncovered present": the prune button is a no-op for them
while the count climbs.

So the bucket was diagnosable and not actionable, and the gap is structural
rather than a configuration mistake.

## Constraints

- **Absent does not mean gone.** Disabling a device in Forward removes it from
  the API exactly as decommissioning does - confirmed against this customer's
  live snapshot, from both the NQE result and the REST inventory. No interface
  available to this plugin distinguishes them. Sustained absence is the only
  thing that can, so the quarantine is load-bearing here rather than
  precautionary.
- **A deleting path names what it deletes.** This one deletes devices that are
  absent from Forward, created by this sync, and past the quarantine. Nothing
  else is reachable from it.
- **The unclaimed half is not ours.** A device this sync did not create has no
  code path here at all.
- No new Forward query. The census that classifies the set already runs.

## Touched Surfaces

- `scope_reconciliation.py` - `record_device_absence` tracks the uncovered set;
  `_delete_prunable_devices` extracted from the orphan prune and shared;
  `prune_uncovered_devices`; `_require_survivable_uncovered_shrink`;
  `ScopeCensusUnavailableError`; the report keeps `_absence_kinds` and
  `_owned_untagged_pks`.
- `forward_device_scope_reconciliation_audit.py` - `--prune-uncovered`,
  dry-run unless `--apply`.

## Approach

Three changes, in dependency order.

**Absence streaks cover the uncovered set.** They were kept for orphans only,
and `partition_quarantined_orphans` fails closed, so an uncovered device had no
absence row and could never leave quarantine however long it had been gone. A
cleanup without this is a permanent no-op.

**One delete path, not two.** The orphan prune's delete loop - ownership
release, PROTECT handling, child-before-parent ordering, cyclic-claim fail-
closed - moves into `_delete_prunable_devices` and both callers use it. A
second copy is how one path acquires a guard the other lacks.

**The cleanup is gated on cause.** Only devices the census marks `absent` are
eligible. `untagged` means Forward still reports the device and it merely
carries no include tag, which is a scoping decision; `vendor_excluded` is the
vendor guard doing its job. Neither is deletable. A census that did not run
raises rather than reading as "nothing is absent", because those two look
identical in the counts and only one of them makes deleting safe.

The shrink guard is new rather than reused. The orphan version measures orphans
against what the run previously claimed and returns early when orphans are
zero - which is exactly the shape this feature exists for, so it would guard
nothing. This one measures the set being deleted against every device the sync
has created, with the same absolute floor before the ratio, because a ratio
alone fires on a small estate where three of eight is normal.

## Validation

`test_uncovered_device_cleanup.py`, 15 tests, mostly negative space: a device
Forward still reports is never deleted; a vendor-excluded one is never deleted;
a device this sync did not create is never deleted; a recent absence is held;
no absence row at all is held; a failed census refuses; an empty Forward result
refuses; deleting most of what the sync created refuses; both overrides work.
Plus the streak tests that make any of it reachable.

Adjacent: `test_uncovered_absence_and_trend`, `test_device_scope_reconciliation_
audit_command`, `test_scope_module_ui`. Full Django suite.

## Rollback

Revert. The streaks stop covering the uncovered set, the command loses the
option, and the bucket returns to being diagnosable and not actionable. No
migration, so nothing to unwind.

## Decision Log

- **2026-09-08** -- Delivered as a management-command option rather than a UI
  button. The orphan prune's button caused the harm this repo has on record,
  and an operator running a dry run and reading the JSON is a different act
  from clicking red. A button can follow once the shape is proven at a
  customer.
- **2026-09-08** -- The quarantine is not overridable by default and the
  override is documented as being for a person looking at a named list, which
  is the same rule the orphan prune already carries.
- **2026-09-08** -- No attempt to distinguish disabled from decommissioned.
  It cannot be done from any interface this plugin has, and pretending
  otherwise is what would make this dangerous.

## Open

Whether the customer's 552 are mostly `absent` or mostly `untagged` is still unknown.
If they are mostly `untagged` this feature will correctly delete almost none of
them, and the real answer is a scope-tag conversation rather than a cleanup.
The dry run tells him which, and that is the first thing to ask for.
