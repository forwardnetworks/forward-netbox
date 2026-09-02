# Measure drift for netbox-dlm

## Goal

Slice six of the adapter-only drift comparison: five of netbox-dlm's seven
sub-models, including `netbox_dlm.vulnerability` - the reporting deployment's
second-largest uncompared model at 37,795 rows, behind only
`dcim.inventoryitem`.

## Why

Unchanged from the earlier slices: an adapter model with no comparison reports
a workload upper bound, and `in_sync` stays unanswerable while any remain.

## Constraints

- Reuse the apply's own resolution; no second comparison.
- Write nothing - including the one write the firewall cannot see.
- An optional plugin's models must not be imported unless it is installed.
- A sub-model whose dependency chain has not been audited must decline, not
  guess.

## Touched Surfaces

- `forward_netbox/utilities/sync_dlm.py` - `preview` on five applies, plus a
  shared `_preview_outcome`
- `forward_netbox/utilities/drift_comparison.py` - a `_model_field_values`
  shim and lazy registration of the DLM entries
- `forward_netbox/tests/test_dlm_drift_comparison.py` (new)

## Approach

Almost every DLM write goes through `_upsert_values_from_defaults` or
`_coalesce_update_or_create`, both already overridden, so the firewall covers
them and only one shim was missing: `_model_field_values`, which is pure - it
drops values for fields a model does not carry and issues no query.

Because all five classify identically from what the shimmed upsert reports,
they share `_preview_outcome` rather than hand-rolling the same three lines
five times and drifting apart.

### The write the firewall cannot see

`apply_netbox_dlm_vulnerability` ends with
`cve.affected_software.add(software_version)` - an M2M write reached directly,
not through a `runner.` call. Same shape as `device.tags.add` in the
tagged-item path, and the same reason this function takes a flag rather than
relying on the shim.

That link is deliberately NOT part of the row's verdict. It is a catalogue-level
relation between CVE and SoftwareVersion rather than part of this row's
identity, and `.add()` on an existing link is a no-op - so counting a row as
drifted because the link is missing would report drift the Vulnerability row
does not have.

### softwareversion declines rather than creating

`apply_netbox_dlm_softwareversion` calls its ensure with `create=False`: the
catalogue map only enriches versions that already have a device-scoped basis.
So an absent one is a row the apply DECLINES, and counting it as a create would
report drift no run resolves. It counts as rejected, the same treatment cables
give a LAG endpoint.

### Two sub-models decline

`inventoryitemsoftware` and `inventoryitemroleplatform` are not wired. They
resolve through `_lookup_inventory_item` and
`ensure_dlm_inventory_item_role_platform`, whose chains have not been audited
for the writes-behind-a-runner-call trap that has now bitten five times
(`_ensure_vrf`, `_ensure_platform`, `_ensure_inventory_item_role`,
`_ensure_module_type`, `_ensure_module_bay`). Absence from
`_ADAPTER_COMPARISONS` is the documented answer, and a test pins that they
still return `None`.

### Registration is lazy

The DLM applies import the optional plugin's models, so registering them at
module scope would break every deployment without netbox-dlm installed. They
are added on first request for a `netbox_dlm.*` model instead.

## Validation

10 tests, skipped wholesale when netbox-dlm is absent. Two negative controls,
both confirmed failing without their guard:

- restoring `cve.affected_software.add` under preview fails
  `test_a_vulnerability_preview_adds_no_affected_software_link`;
- removing the softwareversion decline fails
  `test_a_softwareversion_with_no_device_basis_is_declined`.

524 tests green, including all four existing DLM suites, `test_apply_engine`
and all of `test_sync`.

One fixture trap worth recording: `_row()` carries `name` for the
device-software path, and the CVE apply reads that same key as the CVE's own
name - so a shared row made a matching CVE look drifted. Two meanings, one key.
The CVE tests use their own row.

## Rollback

Remove the netbox-dlm entries from `_ADAPTER_COMPARISONS`; they return to an
upper bound. `preview` defaults to `False` on all five, so the apply is
unchanged for every existing caller.

## Decision Log

- **Wire vulnerability despite the M2M**, because it is 37,795 rows and the
  M2M turned out to be the only unshimmed write - the same shape already
  handled once for tagged items.
- **Leave the catalogue M2M out of the verdict.** It is not part of the row's
  identity, and including it would report permanent drift for a link the apply
  repairs silently.
- **Decline two sub-models rather than assume their chains are clean.** The
  hidden-write trap has appeared in five separate paths; assuming a sixth is
  clean because it looks clean is how each of the previous five got missed.

## Open

- Two DLM sub-models remain uncompared, pending an audit of their chains.
- Two adapter models remain uncompared: peering and routing - last, and
  together, because peering inherits routing's 7-deep BGP-peer chain.
- The comparison contract still has no slot for a delete; routing has
  cascading deletes, so that question comes due there.
