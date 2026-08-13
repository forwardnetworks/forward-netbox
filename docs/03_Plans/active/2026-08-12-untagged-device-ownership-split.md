# Split untagged devices by whether this sync owns them

## Goal

Let an operator tell, without an export or a support round trip, which devices
carrying no include tag are this sync's responsibility.

## Constraints

- Read-only, local, and free: no Forward call, no deletion, no new query.
- The split must not imply a remedy. Which side an operator should care about
  depends on their estate, not on us.
- Devices are the most destructive object to remove. Nothing here deletes, and
  "no identity from this sync" is evidence of not-ours, never proof of garbage.

## Touched Surfaces

- `forward_netbox/utilities/scope_reconciliation.py` -
  `_unmanaged_device_summary`, and the report payload
- `forward_netbox/templates/forward_netbox/forwardsync_scope_reconciliation.html`
- `forward_netbox/tests/test_device_scope_reconciliation_audit_command.py`

## Approach

A deployment reported 407 devices carrying neither include tag while the panel
showed 0 orphans. That reads as a contradiction and is not one: an orphan is a
device this sync PREVIOUSLY CLAIMED and no longer sees, so a device it never
claimed is not an orphan of it. Both numbers were correct, and neither was
actionable, because the number that mattered was never displayed.

"Carries neither include tag" covers two opposite situations:

- **owned** - this sync holds a `ForwardDeviceIdentity` for the device, so it
  created it. Either the device left scope or the tag was never applied. Ours
  either way, and worth investigating.
- **unclaimed** - no identity from this sync. Another source created it, an
  operator did, or it is a leftover from a configuration that no longer
  applies. Imported SNMP endpoints predate the change that stopped generic
  endpoints being imported by default, and nothing has ever revisited them.

The split is decidable from the local database alone, which is why it belongs in
the report rather than in a diagnostic that has to be asked for.

The expectation for that deployment is that the 407 are almost entirely
`unclaimed`. The code deliberately does not depend on that being right: it
reports what is true either way, and a large `owned` count would indicate a real
tagging defect rather than a leftover.

## Validation

Four tests in the scope-reconciliation suite, including the customer's exact
shape - orphans zero while the untagged total is non-zero - so the two numbers
can never silently mean the same thing again.

## Rollback

Revert. The panel loses a row; nothing else changes, because this adds no
behaviour to the sync path.

## Decision Log

- **Report, do not offer to delete.** Today has already shown what acting on
  inferred ownership costs: a hardware-notice rule keyed on "device type holds
  no devices" flagged 33 rows where 5 were stale. Devices are a far worse thing
  to be wrong about.
- **Split by identity rather than by tag or role.** Identity is the plugin's own
  record of what it created. Device type and role are suggestive - these are
  console servers and SNMP endpoints - but they are attributes an operator can
  set themselves.

## Open

- What the deployment's numbers actually say. The split answers it on their next
  page load; nothing further is needed from us until then.
