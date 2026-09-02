# Adapter-model drift comparison, slice nine: netbox-cisco-aci

## Goal

Measure drift for the eight `netbox_cisco_aci.*` models, so `in_sync` is
answerable on a deployment running the ACI plugin.

## Why

#206's eight slices measured every adapter-only model it named. The ACI maps
postdate #206, so its eight models were never in scope, and after slice eight
they were the largest block still reporting an upper bound - and the customer
who asked for drift measurement runs the plugin.

## Constraints

- The verdict rule is the LEAF rule (`preview_leaf_outcome`): every ACI model
  has its own query and its own rows, so a parent create is reported under the
  parent's model. Folding it into the child counts one object twice.
- No new firewall shim. The audit
  (`grep -n "objects\.\(create\|get_or_create\|update_or_create\)\|\.save()"
  sync_aci.py`) returns nothing: every write is already behind
  `runner._upsert_values_from_defaults`, every lookup only reads.
- The real apply is unchanged in behaviour. The guards added to the ensures
  only fire on `None`, which the real upsert never returns.

## Touched Surfaces

- `forward_netbox/utilities/sync_aci.py` - `preview=False` on the eight apply
  functions; `_preview_verdict`; `_parent_absent` guards after every parent
  ensure.
- `forward_netbox/utilities/drift_comparison.py` - `_aci_comparisons`,
  `_register_aci_comparisons`, the lazy branch.
- `forward_netbox/tests/test_aci_drift_comparison.py` (new),
  `test_empty_comparison_is_not_a_measurement.py` (the stand-in moved off a
  real model for good).

## Approach

Registration follows the peering pattern exactly - lazy, because the apply
functions import the optional plugin's models. The one piece of substance is
the absent-parent guard. Under a preview the firewalled upsert returns `None`
for a parent it would create, and `coalesce_lookup` drops `None`, so a child
under such a parent would be resolved by `name` alone and could match a
sibling under another tenant - `unchanged` for a row the apply would create.
That is the absent-VRF defect slice seven found in the routing chain, in a
second family. Each ensure now short-circuits its child to `None` (a create)
when any parent is `None`.

## Validation

`test_aci_drift_comparison.py`: the firewall over all eight models, one
classification per model, the leaf rule (a node under a pod that would be
created is ONE create), the dedup path, the absent-parent guard for tenant,
VRF and fabric, rejection of an unparseable node id, and registration of all
eight. `test_sync_aci.py` (the real apply, fake runner) unchanged and green.
Full Django suite.

## Rollback

Revert. The eight models return to the upper bound; nothing else reads the
guards.

## Decision Log

- **Leaf rule, stated at the call site.** `_preview_verdict` names why in its
  docstring, because the two rules are opposite and getting it backwards is
  silent in both directions.
- **The guard lives in the ensures, not the apply functions**, so the recursive
  parent chain (bridge domain -> VRF -> tenant -> fabric) is covered once at
  every level rather than re-derived per leaf.
- **The stand-in for "uncomparable" is now a model string nothing registers.**
  It rotted twice on real models; it cannot rot on `forward_netbox.nothing_
  registers_this`.

## Open

- Nothing.
