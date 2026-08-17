# Give the diff delete path the allowlist the baseline path already has

## Goal

Stop a Forward NQE diff from deleting the models baseline reconciliation
refuses by name.

## Why

The two delete producers were guarded asymmetrically for two releases.

`compute_full_removals` enforces `BASELINE_REMOVAL_MODELS` and refuses
`dcim.site`, `dcim.devicetype`, `ipam.vrf` and `dcim.device`, with the reasoning
written out directly above the set: shared catalogues may be Device Type
Library imports, global IPAM is never pruned by device scope, device and site
removal is operator-gated through Scope Reconciliation -> Prune orphans because
absence from a query result is not evidence a device is gone.

`_split_diff_rows` enforced nothing. Every `DELETED` row became a delete for any
model, and the only gate between it and `delete()` is the ACI suppression check.
So the exact models one path protects, the other deleted unattended.

A deployment on 2.8.1 reached `delete()` on an `ipam.vrf`, a `dcim.devicetype`
and three `dcim.site` rows in a single sync. Nothing was lost, because PROTECT
refused all five - a database constraint caught what a gate should have. A site
with no devices, or a device type with no devices, has nothing holding it.

## Constraints

- The plugin must still converge on the rows it owns. Interfaces, addresses,
  cables, MACs and the DLM/routing children have to keep being removed, or a
  stale row never leaves.
- No silent behaviour change. A held-back delete is reported, because silence
  reads as "there was nothing to remove".
- The report may not carry customer data. Model labels are schema identifiers;
  site slugs and device names are not.

## Touched Surfaces

- `forward_netbox/utilities/full_removal_reconciliation.py` - `DIFF_REMOVAL_MODELS`,
  `DIFF_REMOVAL_REFUSED_MODELS`, `diff_removals_allowed`
- `forward_netbox/utilities/sync_runner_contracts.py` - `_split_diff_rows`
- `forward_netbox/tests/test_diff_removal_allowlist.py` (new)
- `forward_netbox/tests/test_sync_runner_contracts.py` - one existing test moved
  off `dcim.site`

## Approach

An allowlist, fail closed, stated next to the baseline one so the two can be
read against each other - the asymmetry existed partly because they were far
apart.

The filter goes where the rows are produced, in `_split_diff_rows`, which is the
same layer the baseline path filters in and covers both of its call sites plus
any third one.

The diff list is deliberately wider than the baseline list. Baseline
reconciliation can only speak for models it has persisted rows for; a diff
speaks for whatever the query covers. The additions - virtual chassis, tagged
items, peering sessions and the eight ACI models - are all rows the plugin
solely authors, and ACI keeps its existing `should_suppress_aci_deletes` brake
on top.

The tempting argument for allowing more is that a Forward `DELETED` row is
stronger evidence than baseline absence. It is not. It means the row was in the
query result at the before-snapshot and is not at the after-snapshot, and a
device disabled in Forward, a failed collection and a narrowed query all produce
exactly that.

## Validation

`forward_netbox/tests/test_diff_removal_allowlist.py`. The important half pins
the NEGATIVE space, because asserting only that permitted models are still
deletable is what let "every model" ship twice:

- every model with a delete handler on the runner is in exactly one of the two
  sets, so a model added later cannot land in neither and inherit "deletable"
- each refused model asserted by name, including the customer's five
- an unknown model fails closed
- device-derived models still delete
- the hold-back is reported, names the model, and carries no row values
- upserts for a refused model are untouched - only removal is refused

## Rollback

Revert. The diff path deletes any model again.

## Decision Log

- **Refuse outright rather than gate on `device_tag_prune_out_of_scope`.**
  Plumbing the operator flag into the splitter would add a second way to reach
  device deletion, and there is already one that is reviewed, shrink-guarded and
  warned about. Enabling prune also routes global models to full execution
  anyway, so the flag would rarely change the outcome.
- **Filter at production, not at `apply_deletes`.** A single choke point covers
  more producers, but it also carries the operator prune rows, and re-filtering
  those risks changing the prune flow - a different feature from this fix.
- **Renames are exempt, absences are not.** The first cut refused every
  delete for a refused model, and the full suite caught it: an end-to-end test
  renames a site and asserts the old row is gone. A `MODIFIED` row whose
  identity key changed is Forward reporting the SAME object under a new
  identity, with the after-side written in the same batch - refusing that
  delete preserves nothing and strands a duplicate forever, which is the
  orphaning this machinery exists to prevent. Only `DELETED` rows - "was in
  the before-result, absent from the after-result" - carry the ambiguity the
  policy exists for.
- **Two sets plus a parity test, not one set plus a default.** The refused set
  is not decorative; the test reads it, so deleting a model from it without
  adding it to the allowlist breaks the build instead of quietly permitting it.

## Open

- Nothing blocking. Whether `dcim.device` should ever be removable by a sync
  without an operator step is now asked in exactly one place instead of two.
