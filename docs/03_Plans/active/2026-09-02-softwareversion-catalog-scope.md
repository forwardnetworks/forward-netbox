# Scope the software-version catalogue sweep to what this sync can attribute

## Goal

Stop the `netbox_dlm.softwareversion` durable-state delta from enumerating
the entire `SoftwareVersion` table as delete candidates.

## Why

`apply_durable_workload_deltas` treated every row in the table that the
current result did not name as a delete - operator-created versions included.
Carried as open through `2026-08-21-ownership-sweep-quarantine.md`,
`2026-08-21-release-2.8.9.md` and `2026-08-22-release-2.9.0.md`: "quarantine
is device-shaped and does not apply; scoping it to plugin-provenanced rows is
its own question." With the device sweep quarantined in 2.8.9, this was the
last ungated whole-table delete producer.

## Constraints

- Attribution decides what is a candidate; the existing reference protection
  (`_locally_referenced_delete_identities`, peer protection) decides what is
  deleted. This change does not bypass either.
- The sweep's purpose - catching versions the sync wrote before durable state
  existed - is preserved for exactly the rows it can prove were its own.
- `CATALOG_SWEEP_MODELS` is the allowlist. A model outside it never has its
  table enumerated, however the branch is keyed.

## Touched Surfaces

- `forward_netbox/utilities/workload_state.py` - `CATALOG_SWEEP_MODELS`;
  `_software_version_catalog_rows(sync)` attributes through `DeviceSoftware`
  and `InventoryItemSoftware` on `_sync_exclusive_device_ids(sync)`.
- `forward_netbox/tests/test_workload_state.py`.

## Approach

Same attribution `_bootstrap_dlm_rows` already uses for the association
models: a device only this sync manages. A version referenced by such a
device is a candidate; one referenced by nothing, by another sync's devices,
or by an operator's, is left to whatever the current result says.

## Validation

- Negative space pinned: an operator-created unreferenced version survives; a
  version referenced only by another sync's device survives; the allowlist
  holds exactly one model.
- Positive: an attributed version no longer in the result is a candidate and
  is held by the reference gate while the association still stands - the
  existing "catalog deletes follow authoritative association deletes in the
  same run" test still passes, because those fixtures already own the device.
- Full Django suite.

## Rollback

Revert. The table-wide sweep returns, with its known cost.

## Decision Log

- **The test that enshrined the whole-table behaviour was rewritten, not
  deleted.** `test_software_catalog_reconciliation_deletes_only_unreferenced_
  rows` asserted that an unreferenced operator row is deleted. That was the
  defect stated as a requirement; the replacement asserts the opposite and
  says why.
- **`InventoryItemSoftware` is attributed too**, through its inventory item's
  device, so a version that only an inventory item references is still this
  sync's when the item's device is.

## Open

- Nothing.
