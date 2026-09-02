# A delete the database will refuse must never be staged

## Goal

Stop staging deletes that cannot succeed, so a refused delete is never recorded
as done and the rows that block one are named honestly.

## Why

A deployment's sync reported sixteen protected-delete skips: ten `dcim.device`
held by `netbox_routing.bgppeer`, six `netbox_dlm.softwareversion` held by
`netbox_dlm.inventoryitemsoftware`. Nothing was lost - PROTECT refused every one
- but staging them was wrong three times over.

The durable state tombstones a staged delete as done (`workload_state.py:865-891`,
with `newly_explicit_deletes` at `:848` skipping identities already recorded
`delete`). So the row stays in NetBox, the plugin believes it is gone, nothing
retries, and the report goes quiet while the two systems disagree. The skips are
also noise on every run that does re-derive them. And they were the only visible
edge of a delete path that removes a device with no protecting child silently,
because a successful delete files no ingestion issue.

The guards that should have caught them were hand-written lists.
`netbox_dlm.softwareversion` protected against image files, validated rules,
device software and vulnerabilities, and omitted `InventoryItemSoftware` -
exactly what the deployment's messages named. `dcim.device` checked plugin
ownership only and asked nothing about references at all.

## Constraints

- The `dcim.device` ownership sweep is DELIBERATE and pinned by
  `test_owned_device_absent_from_first_authoritative_state_is_deleted`. This
  change must not disable it. An earlier draft did exactly that and the existing
  tests caught it - see the Decision Log.
- A destructive path must fail closed: an unreadable verdict holds the row back.
- A reference the delete path itself clears is not a blocker.

## Touched Surfaces

- `forward_netbox/utilities/workload_state.py` - new `_reference_protected_pks`,
  wired into the `dcim.device` and `netbox_dlm.softwareversion` guards
- `forward_netbox/tests/test_refused_deletes_are_not_staged.py` (new)
- `.pre-commit-config.yaml` - reorder-imports exclusion

## Approach

Ask Django's `Collector.collect` - the code `.delete()` itself runs - instead of
scanning the model's reverse relations, and ignore blockers whose model the
delete path clears on its way through.

The whole candidate set is collected in one pass first; only when something is
held is the per-row walk paid for.

## Validation

- New: protected row held, unprotected row still staged, fail-closed on an
  unreadable verdict, empty candidate set cheap.
- `test_workload_state`: 33 OK, including the two tests that rejected the first
  draft.
- Full Django suite: 2286 tests OK, 4 skipped.

## Rollback

Revert. Refused deletes are staged again and falsely tombstoned; nothing is
deleted that would not have been, because this change only ever REMOVES rows
from a delete set.

## Decision Log

- **`Collector`, not a relation scan.** The first implementation scanned
  `protecting_relations(Device)` - which does NOT name `BGPPeer`. A `BGPRouter`
  attaches to a device through a GENERIC key carrying no database constraint,
  and the protection appears only further down the cascade. That scan would have
  called the deployment's ten devices deletable and shipped a fix that fixed
  nothing.
- **Ignore what the delete path clears.** The second implementation held back
  EVERY managed device, because each carries a PROTECT `ForwardDeviceIdentity`.
  It disabled the sweep it was meant to make safe, and two existing tests caught
  it. Silently doing nothing is worse than the bug: it is indistinguishable from
  working until someone needs it.
- **Tag claims stay with the claim guard.** They are ignored here so they cannot
  masquerade as a reference; `_claimed_device_delete_identities` already holds
  those devices back, and that is where the policy belongs.
- **Not gated on `prune_removals_allowed`.** The original proposal. Wrong: the
  device sweep is intended behaviour with ownership as its gate, and that gate
  is pinned by a test.

## Open

- **The deeper defect is untouched for the CATALOGUE sweep only.** The device
  sweep was quarantined and shrink-guarded in 2.8.9, and the software-version
  catalogue sweep was scoped to attributable rows on 2026-09-02 (#318). What
  follows described both when it was written. Both sweeps deleted on "absent
  from the current Forward result", with ownership claims as the only gate - and a
  device absent from Forward LOSES its scope claim on the first run that
  observes the absence, so the guard evaporates exactly one run before the
  delete fires. A device with no protecting child is still removed unattended,
  without the prune gate or the 25% shrink guard that
  `full_removal_reconciliation.py` says device removal is supposed to sit
  behind. This change makes such a removal honest, not gated.
- ~~A delete that fails for a reason OTHER than protection is still tombstoned
  optimistically.~~ **Closed 2026-09-02** in
  `2026-09-02-refused-deletes-are-retried.md`: every non-success path in
  `delete_model_rows` records the identity, it travels on the ingestion, and
  promotion drops the entry so the next delta recomputes the delete.
- ~~`DEPENDENCY_SKIP_ISSUE_DETAIL_LIMIT = 10` means a report of exactly ten
  skips may be the cap rather than the count.~~ **Closed 2026-09-02** (#319):
  ten IS the count at the cap, and the tenth per-row issue now says further
  rows are rolled up.
- ~~`emit_dependency_skip_issue_summary`'s wording ("their NetBox parent is not
  synced yet") describes the opposite of a protected-delete skip.~~ **Closed
  2026-09-02** (#319): one sentence per direction, each with its own remedy.
