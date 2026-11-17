# A Held Unmanaged Delete Says Why, and Offers the Way Through

## Goal

When "Delete (verified gone)" on the unmanaged-devices list deletes nothing, the
operator can see which devices were held, what holds each, and what to do, and
the support bundle carries the same detail.

Before this, a customer selected 66 devices Forward does not report, ran the
delete, and nothing happened. The job reported success: 0 deleted, 25 held by
routing records (20 BGP scopes, 10 OSPF interfaces). The page said "Queued job",
the card showed nothing, the confirmation page had no blocker preview and no way
to remove the routing records, and the bundle's blocker survey did not cover this
group, so neither the customer nor support could see the cause.

## Constraints

- No customer names, network ids, snapshot ids or hostnames in the repo.
- Eligibility is unchanged: only devices no sync is bound to and Forward does not
  report, re-checked by the job, within the per-run cap.
- Routing records are removed only when the operator ticks a box that names what
  goes, only for the routing allowlist, and only for the devices that own them.
  Any other record that holds a device still refuses it and is reported.
- Job data records pks, model labels and counts, never device names.

## Touched Surfaces

- `utilities/scope_reconciliation.py`: `_delete_prunable_devices` can report, per
  device, what refused it; `prune_unmanaged_devices` takes `release_routing`
  and returns the per-device detail.
- `jobs.py`, `api/views.py`: the job and the REST action accept `release_routing`;
  the job stores `protected_by_device`, `held_summary` and `release_routing`.
- `views.py`, `templates/forward_netbox/forwardsync_prune_unmanaged.html`: the
  confirmation page previews what holds each selected device and offers the box.
- Scope Reconciliation card and `cleanup_checklist.py`: last unmanaged delete
  result, held devices, and the next step.
- Support bundle: the blocker survey includes the unmanaged-absent group; the new
  card figures are in `ui_figures` and the label index.
- Tests: `test_unmanaged_device_prune.py`, `test_unmanaged_prune_views.py`, new
  card and bundle tests.

## Approach

1. The shared delete loop fills an optional per-device map of refusals; callers
   that do not pass it behave as before.
2. `prune_unmanaged_devices(release_routing=True)` calls the same loop with the
   routing release on, which is the path the orphan prune already uses and which
   rolls a device back if a non-routing record also holds it.
3. The job's held summary splits held devices into those the box would free
   (every holder is an allowlisted routing record) and those that need another
   record removed first.
4. The confirmation page uses a time-budgeted blocker survey, so a large
   selection is summarised instead of skipped.

## Validation

- Unit tests derived from the symptom: a device held by routing records is held
  by default and reported by pk and label; with `release_routing` the same device
  is deleted and the routing records go with it; a device held by a non-routing
  record is still refused and named; eligibility refusals are unchanged.
- View tests: the confirmation page names the holders and the box; the job gets
  `release_routing` only when ticked.
- Card, checklist and bundle tests; the bundle label index test.
- Live on the test host: a device with BGP and OSPF records the sync does not own,
  absent from Forward, deleted through the page with and without the box.

## Rollback

Revert the commit. Nothing is migrated; stored job data from earlier runs simply
lacks the new keys.

## Decision Log

- Rejected: removing routing records by default. Nothing claims these devices, so
  their routing rows may be a person's; the operator chooses.
- Rejected: pointing the operator at the ordinary device bulk delete. It skips the
  check that Forward no longer reports the device.
- The held summary counts devices, not rows, because the box acts per device.
