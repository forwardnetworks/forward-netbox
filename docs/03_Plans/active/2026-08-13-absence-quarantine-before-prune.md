# Quarantine an absent device before the prune deletes it

## Goal

Stop a device that is merely disabled in Forward from being permanently deleted
from NetBox on the next sync. An absent device must stay absent across several
promoted runs, and for a stretch of wall-clock time, before the orphan prune is
allowed to touch it.

## Why

Confirmed against the customer's live snapshot on 2026-08-13: a device disabled
in Forward vanishes from `network.devices` entirely, and from the REST inventory
too. From every interface available to the plugin, **disabled is
indistinguishable from deleted**. So a maintenance window, a pending decom, or a
licensing lapse presents exactly as a device that left the estate, and with
`device_tag_prune_out_of_scope` on, the very next sync deletes it. That is how 76
devices went in one run.

The deletion is permanent; the disabling usually is not.

## Constraints

- The prune is a capability operators explicitly enabled. Do not turn it into a
  report-only feature - that removes something they asked for.
- Operators must never need a shell to configure or clear this. Thresholds are
  source parameters with form fields, and the held-back set is visible on the
  Scope Reconciliation panel.
- No new NQE calls. Forward engineering has already objected to call volume; the
  streak is derived from the reconciliation report the sync already computes.
- The bookkeeping row must never be able to block a delete. Everything else in
  the ownership tables uses `PROTECT` plus an explicit release; a hidden
  `PROTECT` relation is exactly what made ingestions undeletable once already.

## Touched Surfaces

- `forward_netbox/models.py` - `ForwardDeviceAbsence`
- `forward_netbox/migrations/0052_device_absence_quarantine.py`
- `forward_netbox/utilities/scope_reconciliation.py` - record the streak in
  `tag_backfilled_devices`, enforce it in `prune_orphan_devices`, report it
  from `compute_scope_reconciliation`
- `forward_netbox/forms.py` - two threshold fields
- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`
- `forward_netbox/views.py` - the override on the manual prune
- tests

## Approach

### Streak state

One row per `(sync, device)` recording `consecutive_absent_runs`,
`first_absent_at`, `last_absent_at` and the snapshot that last saw it absent.
`device` is `CASCADE`, not `PROTECT`: this is bookkeeping about a device, and it
must not be able to hold one hostage.

`tag_backfilled_devices` - the post-sync "reconcile device scope tags" job -
already runs once per promoted sync and already holds the out-of-scope set, so
it is where the streak advances. It maintains the backfilled and out-of-scope
tags for every sync regardless of whether scope tags are applied, so no
configuration leaves a sync unable to accumulate streaks. Devices no
longer out of scope have their row deleted outright - a device that came back
starts from zero, because the streak is only meaningful as an unbroken run.

A run that fails before that point never advances the streak, which is the
correct direction: an absence we could not confirm is not evidence of absence.

### Threshold

Prune-eligible requires **both** `consecutive_absent_runs >=
device_tag_prune_absence_runs` (default 3) **and** `first_absent_at` older than
`device_tag_prune_absence_hours` (default 72).

Neither test works alone. Runs alone is meaningless when a sync is scheduled
hourly - three runs is three hours, shorter than any maintenance window. Hours
alone is meaningless when a sync runs weekly and one confirmation would clear
the bar. Requiring both makes the quarantine at least 72 hours and at least
three confirmations whatever the schedule, which is the property we actually
want.

### Enforcement

`prune_orphan_devices` filters `orphan_pks` down to those past the threshold and
reports the held-back count. The existing zero-guard and shrink-guard are
unchanged and still run first - quarantine is an additional gate, not a
replacement.

The manual "Prune orphans" button takes an explicit override, because a human
looking at a named list of orphans and choosing to delete them is a different
act from a scheduled job doing it unattended. The automated path has no
override. This is the same split the shrink guard already uses.

### The absence row is also what keeps the device an orphan

This is not a detail of the streak; the quarantine does not work without it.

`out_of_scope` is derived from devices holding a live scope claim, and the
claim reconciliation releases every claim whose device is missing from the
current Forward result - on the FIRST run that observes the absence. So by run
2 the claim table no longer remembers that this sync ever managed the device.
It drops out of `out_of_scope` entirely, `record_device_absence` reads that as
"came back" and deletes the row, and the streak resets to nothing. Measured on
a real three-run sequence: started=1, then cleared=1, then nothing, for ever.

The consequence is not a slow quarantine, it is a dead one. The streak can
never pass 1, so with the default threshold of 3 no device is ever prune
eligible, the panel's held count drops to 0 after the first run, and the prune
becomes a permanent no-op for exactly the devices it exists to delete. It fails
safe and delivers nothing, which is the worst way to be wrong about this.

So `compute_scope_reconciliation` treats a device with an open absence row as
previously managed, alongside the live claims. The row is created only from a
device that held a claim and is cleared the moment the device returns to the
Forward result, so this widens what counts as managed without inventing a claim
the sync never made. It also removes a pre-existing order dependency: before
this, whether an orphan was still prunable depended on whether the prune job
happened to run before the tag-reconciliation job.

### A note on the first run

On an estate with no streak rows yet, nothing is prune-eligible. That is
deliberate and it fails in the safe direction, but it does mean an operator who
enables prune today deletes nothing today. The panel says so, and the manual
override is the way through.

## Validation

- Tests pin the negative space: a device absent for fewer than N runs is NOT
  pruned; a device absent for N runs but only 1 hour is NOT pruned; a device
  that reappeared has no row and is NOT pruned on the run after that.
- Tests pin that the streak resets rather than accumulating across a gap.
- A test asserts the absence row does not raise `ProtectedError` on device
  delete.
- A test asserts that zeroing both thresholds releases everything, so the
  form's "0 disables" help text is true rather than aspirational.
- A test asserts a garbage threshold falls back to the default instead of
  reading as "delete immediately".
- No test asserts the absence of Forward calls, because there is nothing to
  assert against: none of the new functions take a client or a network id, so
  the call count cannot change. Recording reads the report the sync already
  computed; the partition and the panel summary are single DB queries.

The existing `test_scope_shrink_guard.py` cases had to have the quarantine
switched off in their fixture. Left on, every one of them would have been held
for want of an absence row and would have asserted zero pruned devices without
reaching the shrink guard at all - passing, and proving nothing.

That switching-off cuts both ways, though, and it is why the guards are also
pinned *together*. Each file disables the other's guard, so between them the
composition was covered nowhere and "an additional gate, not a replacement" was
resting on reading order alone. `QuarantineDoesNotReplaceTheShrinkGuardTest`
holds it: a collapsed scope raises rather than returning a quiet held-count,
raises even when every device has served the quarantine in full, and the
scope-shrink override does not double as a quarantine override.

The streak is pinned through the real job rather than only through
`record_device_absence`: one test that a completed run starts then advances it,
one that a run dying before the recording does not advance it, and one that a
run dying *after* the recording takes the row down with it. The last is what
makes "inside the same transaction as the tagging" a fact rather than a comment.

## Rollback

Revert. Absent devices are prune-eligible immediately again, as in 2.7.13.

## Decision Log

- **Quarantine, not report-only.** Making prune report-only would remove a
  capability customers deliberately enabled. Delay is enough - a maintenance
  window ends long before the threshold.
- **Both thresholds, not either.** See above; each alone is defeated by a
  plausible sync schedule.
- **CASCADE on `device`.** Bookkeeping must not pin the object it describes.
- **Manual override, no automated override.** The unattended path is the one
  that caused the harm.
- **An absence row counts as previously managed.** The alternative was to stop
  releasing the scope claim while a device is quarantined, which keeps the
  memory in one place - but claim release is shared by both tag domains and
  gates ownership completion through `stale_claims`, so retaining claims there
  would have reached well beyond the prune. The absence row already has exactly
  the lifetime required.

## Open

- Whether the defaults (3 runs / 72 hours) match how this customer schedules
  syncs. They are source parameters, so this is tunable without a release.
