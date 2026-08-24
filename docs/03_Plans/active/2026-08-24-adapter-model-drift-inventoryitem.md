# Measure drift for inventory items

## Goal

Slice three of the adapter-only drift comparison: `dcim.inventoryitem`.

## Why

Unchanged from slices one and two. Every adapter-only model reports a workload
upper bound, so `in_sync` stays unanswerable while any of the eight is
uncompared.

## Constraints

- Reuse the apply's own resolution; do not reimplement the comparison.
- Write nothing.
- Never report a count the contract cannot express.

## Touched Surfaces

- `forward_netbox/utilities/sync_inventory_module.py` -
  `apply_dcim_inventoryitem` gains `preview`, and a sentinel
- `forward_netbox/utilities/drift_comparison.py` - four shims, a null-sync
  method, and `uncomparable_outcomes` on the shared loop
- `forward_netbox/tests/test_inventoryitem_drift_comparison.py` (new)

## Approach

### The writes were the easy half

All three are behind `runner.` calls - `_ensure_manufacturer` (already
overridden for the device path), `_ensure_inventory_item_role`, and
`_upsert_values_from_defaults` - so the firewall covers them and the flag does
not have to. `_ensure_inventory_item_role` is a new override and is the same
trap as `_ensure_vrf` and `_ensure_platform`: an upsert reached during
classification that a grep for ORM calls cannot see. It becomes a lookup
returning `None` for an absent role, which classifies the item as a create -
correct, because the item cannot already exist under a role NetBox lacks.

### The delete branch, which is not a write problem

When `dcim.module` is enabled, a module-native inventory row is DELETED rather
than upserted (`apply_dcim_inventoryitem` calls `delete_dcim_inventoryitem`).
Suppressing the delete is trivial - `_delete_by_coalesce` returns `False`. What
is not trivial is what to COUNT.

The report reads drift as `creates + updates` (`views.py:513`) and accounts for
deletes separately. So:

- counting it as an update double-counts it against that separate accounting;
- counting it as unchanged is a confident zero for a row that will change;
- counting it as rejected reports a perfectly good row as unusable, and feeds
  `comparison_rejected_rows`, which means something else.

There is no honest bucket. So the row returns
`MODULE_NATIVE_ROW_NOT_COMPARABLE` and the comparison declines the whole model,
which keeps its upper bound - the documented "no comparison" answer, and the
only one that cannot mislead in either direction.

The refusal is scoped to batches that actually contain such a row. A
deployment without module-native inventory still gets a real measurement, and
one with `dcim.module` disabled is unaffected because the branch is gated on
it. Both are pinned by tests, the second specifically so a future reader does
not "simplify" the gate away and silently drop the model for everyone.

Declining the whole model rather than the row is deliberate: a partial count
would understate drift by however many rows were dropped, silently.

## Validation

12 tests. Both guards proven by their own negative control, run separately:

- Making `_ensure_inventory_item_role` write failed
  `test_a_preview_creates_no_inventory_item_role`.
- Removing the module-native decline failed
  `test_a_module_native_row_declines_the_whole_model` and
  `test_one_module_native_row_declines_the_batch_it_is_in`.

Full run: 325 tests green across the three adapter suites, the bulk drift
suite, the preview contract and priming suites, and `ForwardSyncRunnerTest`.

## Rollback

Remove `dcim.inventoryitem` from `_ADAPTER_COMPARISONS`. `preview` defaults to
`False`, so the apply is unchanged for every existing caller.

## Decision Log

- **Decline rather than invent a bucket for the delete.** The contract has
  four keys and a delete is none of them. Adding a fifth would change what
  `views.py` and the report consume, which is a wider change than this slice
  should carry - and worth doing deliberately, not as a side effect.
- **Decline the model, not the row.** A partial count understates drift
  silently, which is the failure this feature exists to prevent.
- **`is_model_enabled` returns False on the null sync.** Matches the existing
  `_scope_tags_enabled` degradation: production always passes the real sync,
  and guessing that a delete applies would be worse than classifying the
  ordinary upsert.

## Open

- **Whether the comparison should carry deletes at all.** Today it cannot
  express one, and `dcim.inventoryitem` is the first path where that costs a
  measurement. If a later slice hits the same wall, a fifth count key plus the
  `views.py` and report changes to consume it is probably the right answer -
  but it should be its own change, with the double-counting question against
  the existing delete accounting settled first.
- Five models remain: FHRP groups, lifecycle (netbox-dlm), modules, peering,
  routing.
- Modules are the next real step up: `Module.save()` with `_adopt_components`
  makes NetBox core instantiate component rows, and Manufacturer, ModuleType
  and ModuleBay are written beneath it.
- Routing and peering last, and together.
