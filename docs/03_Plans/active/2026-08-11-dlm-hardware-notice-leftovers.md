# Clean up DLM hardware notices left by a query the sync no longer runs

## Goal

Give an operator a supported way to remove the hardware notices that make the
DLM list appear to hold duplicates, and record why they exist.

## Constraints

- A device type must not be deleted. An empty one may have come from a Device
  Type Library import and is not evidence of a mistake.
- Deletion is opt-in and dry-run by default, matching orphan prune.
- No Forward call: an operator chasing a visible duplicate should not have to
  spend an NQE execution to see it.

## Touched Surfaces

- `forward_netbox/utilities/dlm_notice_audit.py` (new)
- `forward_netbox/management/commands/forward_dlm_hardware_notice_audit.py` (new)
- `forward_netbox/tests/test_dlm_hardware_notice_audit.py` (new)

## Approach

The customer's pairs are the same hardware under two names - `N9K-C93180YC-FX`
and `Nexus 93180YC-FX`, `C9500-40X` and `Catalyst 9500-40X` - with identical
end-of-support dates. Probing their Forward network confirmed the alias data
file maps both spellings to one NetBox model and one slug, so the alias file is
self-consistent and is not the source. Their enabled maps confirmed only the
alias variant runs, so nothing is writing both.

The rows are leftovers, and the reason is structural:

- `netbox_dlm.hardwarenotice` coalesces on `device_type`, so one notice per
  device type; two notices means two device types.
- Removals reach NetBox only from a Forward NQE diff, which reports what the
  CURRENT query stopped returning. A full run computes no removals at all -
  `delete_rows` is empty outside device-tag scope pruning.

So when a map is re-pointed at a different query, every row the previous query
wrote is orphaned permanently. Switching the device-type maps to their
alias-aware variants does exactly that, and the hardware notices are where it
becomes visible because they render as a flat list.

The audit uses the one signal that needs neither Forward nor the alias data: a
notice whose device type holds no devices describes hardware this NetBox does
not have. A notice is derived data - if it still applies, the next sync writes
it back.

## Validation

`forward_netbox/tests/test_dlm_hardware_notice_audit.py` builds the customer's
exact pair, asserts the notice on the device-bearing type survives, the leftover
goes only with `--apply`, and the device type is never deleted. Skips when
netbox_dlm is absent.

## Rollback

Revert. The command disappears; nothing else changes, since this adds no sync-
path behaviour.

## Decision Log

- **Delete the notice, never the device type.** The sync owns the notice. It
  does not own a device type an operator may have imported deliberately.
- **"No devices" rather than alias-sibling detection.** Matching a pair through
  the alias data would need the Forward data file and would still only be a
  heuristic. "This notice describes hardware not present" is decidable locally
  and is the property that actually matters.
- **Audit rather than automatic cleanup on sync.** The general problem - a full
  run never reconciles removals - is a sync-path design question, and quietly
  deleting rows during a sync is the failure mode that orphan prune already
  taught us to be careful about.

## Closed

- **The root fix shipped** (`691f67a`, #182):
  `forward_netbox/utilities/full_removal_reconciliation.py`, consumed by
  `query_fetch_execution.py:51-56` via `_full_run_removals`. The narrowing
  hazard is gated exactly where this plan said it had to be - by
  `prune_removals_allowed` and `network_complete_removals`, which raise
  `RemovalReconciliationRefused` rather than removing anything. Its own design
  is `2026-08-11-full-run-removal-reconciliation.md`.

## Open

- Diff-only models still do not reconcile removals until their next full
  execution. That is the accepted order rather than a defect, but it is the
  residue of this item and the reason a re-pointed diff-only map can stay
  stale for one cycle.
