# Measure drift for modules

## Goal

Slice five of the adapter-only drift comparison: `dcim.module`.

## Why

Unchanged. `in_sync` stays unanswerable while any adapter model is uncompared.

## Constraints

- Reuse the apply's own resolution and comparator.
- Write nothing - including writes NetBox core performs on our behalf.
- A row the apply skips must not read as drift.

## Touched Surfaces

- `forward_netbox/utilities/sync_inventory_module.py` - `apply_dcim_module`
  gains `preview`
- `forward_netbox/utilities/drift_comparison.py` - `_lookup_module_bay`,
  `_ensure_module_bay`, `_ensure_module_type`, dispatch entry
- `forward_netbox/tests/test_module_drift_comparison.py` (new)

## Approach

### The write that is not ours

Every write on this path is behind a `runner.` call, so the firewall covers
them: `_ensure_module_bay` upserts a `ModuleBay`, `_ensure_module_type` upserts
a `ModuleType` and a `Manufacturer` beneath it, and the module itself goes
through `_upsert_values_from_defaults`.

What makes this slice different is what that last call does. It passes
`create_instance_attrs={"_adopt_components": True}`, and NetBox core's
`Module.save()` then walks the module type's component templates and
instantiates them - console ports, interfaces, power ports, module bays - as
real rows on the device. So a preview that reached the save would not create
one row; it would create a dozen, on hardware the operator only asked a
question about.

The existing override never saves, and it accepts and ignores
`create_instance_attrs` for exactly this reason. The assertion that proves it
counts `Interface` rows before and after with an `InterfaceTemplate` present:
under a negative control that made the shim write, that count went 0 -> 1, so
the test catches the component instantiation specifically and not merely the
module row.

### Absent dependencies short-circuit

The preview runner resolves rather than creates, so an absent bay or module
type comes back `None`. The row is then unambiguously a create - a module
cannot already exist in a bay NetBox does not have, or with a type it does not
have - and short-circuiting also avoids a coalesce lookup on a null dependency
matching some unrelated row.

### The skip is not drift

A module type whose templates would create a component name already claimed by
a DIFFERENT module cannot be applied: NetBox adopts only components belonging
to no module, so the template instantiation trips the per-device unique-name
constraint. The apply reports and skips. The comparison counts it `rejected`,
because a row the apply refuses is not a difference between the two systems -
counting it as a create would show drift that no run could ever clear.

## Validation

11 tests. Two negative controls, each confirmed failing without its guard:

- Making the upsert shim write for real failed
  `test_a_preview_instantiates_no_components` (on the Interface count, 1 != 0 -
  the component instantiation itself), plus the two drifted-module tests.
- Disabling the unadoptable-component detection failed
  `test_a_component_claimed_by_another_module_is_rejected_not_a_create`,
  confirming that test is not passing vacuously - it was written defensively
  enough that it could have been.

Full run: 416 tests green, including `test_module_readiness` and all of
`test_sync`.

## Rollback

Remove `dcim.module` from `_ADAPTER_COMPARISONS`. `preview` defaults to
`False`, so the apply is unchanged for every existing caller.

## Decision Log

- **Assert on component rows, not just the module row.** The module row is the
  obvious thing to count and the least informative: the damage a preview could
  do here is the components NetBox core creates underneath it.
- **Short-circuit absent bay/type rather than resolving further.** Consistent
  with every prior slice, and it avoids a null-dependency coalesce lookup.
- **The unadoptable-component skip is `rejected`.** Same reasoning as the LAG
  endpoint in cables: a row the apply permanently refuses must not be reported
  as drift a future run will fix.

## Open

- Three models remain: lifecycle (netbox-dlm), peering, routing.
- Routing and peering last and together: peering is 30 lines but calls
  `_ensure_netbox_routing_bgppeer` first, inheriting a 7-deep write chain
  (VRF, two ASNs, an RIR, an IPAddress, BGPRouter, BGPScope, then the peer).
- netbox-dlm is 7 sub-models with cross-dependencies and an M2M inside a
  transaction; it has its own test modules, which makes it self-contained but
  not small.
- The delete question from the inventory slice is unchanged and still open.
