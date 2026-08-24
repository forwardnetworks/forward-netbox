# Review fixes for the adapter-model drift stack

## Goal

Correct six defects found by reviewing the five-slice adapter-model drift stack
(#289-#293) as one cumulative diff, before any of it merges.

## Why

The stack was built slice by slice, each with its own negative controls, and
each slice passed. Reviewing the five together surfaced defects that no single
slice's tests could have shown - three of them the same shape: the preview and
the apply disagreeing about the same row, in a direction that reports drift no
run will ever clear.

## Constraints

- Each fix must leave the apply path byte-identical in behaviour; these are
  preview-side corrections plus one shared-instance bug that affected both.
- No fix may depend on which models are wired into `_ADAPTER_COMPARISONS`.
- Every fix needs a test that fails without it.

## Touched Surfaces

- `forward_netbox/utilities/sync_ipam.py` - compute the VIP diff before
  mutating
- `forward_netbox/utilities/sync_inventory_module.py` - skip-check ordering,
  identity-aware short-circuit, None-safe warning
- `forward_netbox/utilities/drift_comparison.py` - cached/ambiguity-raising
  module-type lookup, cleaned-name bay lookup, real coalesce fields
- `forward_netbox/utilities/sync_interface.py` - remove leftover marker
- tests for each

## Approach

Take the six defects in severity order, each with a test that fails first.
Three share a root cause worth naming: the preview and the apply resolving the
same row differently, in the direction that reports drift no run will clear.
The rest are a mutated cache, a dead marker, and a wasted priming.

## The fixes

### 1. A cached object was mutated while measuring (found before review)

`_ensure_fhrp_vip` assigned `existing.address`, `.status` and `.role` while
computing what would change, and only then checked `preview`.
`get_unique_or_raise` caches the resolved INSTANCE, so the next row resolving
the same VIP got an already-corrected object and compared clean.

Two routers in one HSRP group is the ordinary topology, not an exotic one, and
the second one's drift silently disappeared. The diff is now computed into a
list of `(field, value)` pairs and applied only on the write path.
`test_two_devices_in_one_group_both_report_the_drifted_vip` failed `1 != 2`
before the fix.

### 2. The permanent-skip check ran after an absent-dependency short-circuit

`apply_dcim_module` short-circuited on a missing `module_bay` BEFORE
`_unadoptable_component_names`. But the apply creates the bay and then hits the
collision and skips permanently, so the preview reported a create for a row no
run will ever apply. The skip verdict does not depend on the bay existing, so
it now comes first.

Reordering exposed a second defect immediately: the skip's warning message
dereferenced `module_bay.name`, which is `None` under preview because the
preview resolves where the apply creates. It reads the name from the row now.

### 3. An absent module type is not a create

The short-circuit also fired on a missing `module_type`, justified as "a module
cannot exist with a type NetBox does not have". That is wrong: the module's
coalesce set is `("device", "module_bay")`, so the type is not part of its
identity. A bay that already holds a module gets an UPDATE when the card is
swapped for an unseen type. Only the bay short-circuits now.

### 4. The module-type lookup threw away the priming

`_ensure_module_type` issued a raw `ModuleType.objects.filter(...).first()` per
row. `prime_dependency_lookup_caches` had already bulk-loaded exactly that
lookup, so the priming was wasted and the per-row resolution cost 2.8.7 exists
to remove was reintroduced for this model. It goes through
`_get_unique_or_raise` now, which also RAISES on an ambiguous match where
`.first()` silently took the lowest pk - letting a preview succeed on a row the
apply refuses.

### 5. The bay lookup missed the cleaned name

The real `_ensure_module_bay` coalesces on `module_bay_plan_row(row)["name"]`,
which is stripped and clipped. The override matched only the raw name, so a
Forward `"Slot 1 "` missed a NetBox `"Slot 1"` and became a phantom create. It
falls back to the cleaned name.

### 6. The preview used default coalesce sets, not the operator's

`_coalesce_sets_for` reads `_model_coalesce_fields`, which the real runner
fills from resolved query specs (`sync_execution.py:115`). A preview never runs
that resolution, so the dict stayed empty and every call fell back to the
hard-coded defaults at the call site.

An operator who narrows `dcim.inventoryitem` coalescing to `("device", "name")`
because serials are unreliable gets an apply that resolves and updates the
existing row, and a preview that misses it and reports a create - phantom
drift on every such row, every run. Resolved lazily and memoised, falling back
to the caller's defaults if a spec cannot be resolved.

### 7. Test scaffolding left in production

`# NEGATIVE-CONTROL-MARKER` in `sync_interface.py`, from this session's own
negative control. Nothing referenced it. Removed.

## Validation

436 tests green across all five adapter suites, the bulk drift suite, the
preview contract/priming/cost suites, `test_module_readiness` and all of
`test_sync`.

Two new tests pin the review findings directly:
`test_a_claimed_component_is_rejected_even_when_the_bay_is_absent` (which
errored on the `None` dereference before that was fixed too) and
`test_a_swapped_card_in_an_existing_bay_is_an_update_not_a_create`.

## Rollback

Each fix is independent of the others and of which models are wired up. The
`_conflict_policy`, `_coalesce_sets_for` and VIP-mutation fixes correct the
preview runner and the apply respectively, and should survive any rollback of
an individual model's dispatch entry.

## Decision Log

- **Review the stack cumulatively, not slice by slice.** Every slice passed its
  own tests and its own negative controls. Three of these six defects are
  disagreements between preview and apply that only a reader holding both sides
  at once would notice.
- **Resolve coalesce fields lazily rather than eagerly in `__init__`.** Only
  the adapter models ask, and a spec lookup per model on every preview
  construction would cost callers that never use it.

## Open

- The remaining review findings are lower severity and NOT fixed here: an empty
  coalesce-lookup list classifies as "would create" where the apply raises
  `ValueError` (a row with neither tag nor tag_slug); `.get()` versus indexing
  makes preview and apply disagree about a malformed row (create versus
  rejected); `_unadoptable_component_names` costs ~8 uncached queries per
  module row. The first two are honest-but-wrong classifications of rows that
  are already broken; the third is a cost question that wants a measurement
  rather than a guess.
- Three models still uncompared: lifecycle (netbox-dlm), peering, routing.
