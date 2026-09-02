# Measure drift for FHRP groups

## Goal

Slice four of the adapter-only drift comparison: `ipam.fhrpgroup`.

## Why

Unchanged. Every adapter-only model reports an upper bound, so `in_sync` stays
unanswerable while any of the eight is uncompared.

## Constraints

- Reuse the apply's own resolution and comparisons.
- Write nothing - and this path writes in three places, two of them direct.
- A row the apply refuses must not read as drift.

## Touched Surfaces

- `forward_netbox/utilities/sync_ipam.py` - `apply_ipam_fhrpgroup` and
  `_ensure_fhrp_vip` gain `preview`
- `forward_netbox/utilities/drift_comparison.py` -
  `_coalesce_update_or_create` override, and the dispatch entry
- `forward_netbox/tests/test_fhrpgroup_drift_comparison.py` (new)

## Approach

### Three objects, one verdict

A single row means up to three persisted objects: the `FHRPGroup`, its
virtual-IP `IPAddress`, and the `FHRPGroupAssignment` binding it to an
interface. The row's verdict is the strongest of the three - a create if any
would be created, an update if any would be rewritten, unchanged only when all
three already match. Anything less would under-report: a group that exists with
a drifted VIP is drift.

### The other upsert primitive

`_upsert_values_from_defaults` funnels into `coalesce_update_or_create`, but
FHRP calls `runner._coalesce_update_or_create` DIRECTLY with explicit lookups,
for both the group and the assignment. Overriding only the first left the
firewall with a hole exactly where the caller was most explicit about what it
was writing. Both are overridden now.

### Three writes, two of them direct

- the group and the assignment go through `_coalesce_update_or_create`
  (covered by the new override);
- the VIP saves directly inside `_ensure_fhrp_vip`, on both its create and its
  update branch;
- the canonical-name migration is a direct `group.save(update_fields=["name"])`
  in the apply itself.

The rollback `group.delete()` is unreachable under preview, because nothing is
ever created to roll back.

### The refusals

A VIP owned by another kind of object is a conflict the apply refuses -
`rejected`. A VIP shared with another FHRP GROUP is different and deliberately
so: the apply leaves it where it is, writes nothing for it, and still creates
the group and assignment. That is the fix for the 13-group add/remove churn
this module documents, so the comparison must call it `unchanged` rather than a
perpetual update.

## Validation

12 tests. Three write guards, three separate negative controls, all confirmed
failing without their guard.

**One of those controls found a real coverage gap rather than confirming a
test.** Removing the VIP-create guard broke nothing: when the group is absent
the row short-circuits to a create before `_ensure_fhrp_vip` is ever called, so
the guard was unreachable in every test that existed. The case that reaches it
- group present, VIP absent - was missing.
`test_a_group_present_without_its_vip_creates_no_address` now covers it, and
re-running the same control against it fails as it should. A control that
passes is a finding, not a pass.

**And the sweep caught a real regression in the apply path.** Reading
`runner.last_upsert_would_change` unconditionally raised `AttributeError` on
the real runner - that attribute exists only on the preview runner - breaking
fourteen existing FHRP tests. It is now read only under `preview`. Worth
recording because the unit tests for this slice all passed while it was broken:
they only ever exercise the preview runner. The existing apply tests are what
caught it, which is the argument for running the whole module rather than the
new file.

Full run: 412 tests green, including all of `test_sync`.

## Rollback

Remove `ipam.fhrpgroup` from `_ADAPTER_COMPARISONS`. `preview` defaults to
`False` on both functions. The `_coalesce_update_or_create` override should
stay - it corrects the firewall regardless of which models are wired up.

## Decision Log

- **Strongest-of-three for the row verdict.** Any weaker rule under-reports,
  and under-reporting drift is the failure this feature exists to prevent.
- **A shared VIP is `unchanged`, not an update.** The apply writes nothing for
  it by design; calling it drift would make the report show a difference that
  every subsequent run also shows.
- **Override both upsert primitives.** One override is a firewall with a hole
  in it.

## Open

- Four models remain: lifecycle (netbox-dlm), modules, peering, routing.
- Modules next: `Module.save()` with `_adopt_components` makes NetBox core
  instantiate component rows, beneath Manufacturer/ModuleType/ModuleBay writes.
- Routing and peering last, and together.
- The delete question from the inventory slice is unchanged and still open.
