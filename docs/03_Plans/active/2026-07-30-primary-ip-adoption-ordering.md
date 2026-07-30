# Primary IP Adoption Ordering

## Goal

Fix the `dcim.device` `ValidationError` that 2.6.6 could only make diagnosable.

A customer merge failed one `dcim.device` row with a `ValidationError` whose
cause was unknown. 2.6.6 recorded a schema-level diagnosis and made the failure
non-terminal, but did not explain it. This is the explanation and the fix.

## Contract

- A device or virtual machine that adopts an IP as `primary_ip4`, `primary_ip6`
  or `oob_ip` merges *after* the change that assigns that IP to an interface.
- Ordering no longer depends on which `ObjectChange` was recorded first.
- A candidate edge that would make the change graph unsortable is dropped, not
  applied: a cycle fails every row in the merge, which is worse than the single
  row this edge exists to save.

## Constraints

- Keep `netbox_branching` in the ingestion path.
- Do not change what `Device.clean()` validates, and do not bypass validation
  to make the row merge.
- Persisted diagnostics stay schema-level; no customer data.

## Touched Surfaces

- `forward_netbox/utilities/bulk_merge.py`
- `forward_netbox/tests/test_bulk_merge.py`
- This plan.

## Approach

**Root cause.** `Device.clean()` validates that `primary_ip4`/`primary_ip6`/
`oob_ip` resolve to an interface of that device, reading `vc_interfaces` from
the destination. So the IP-to-interface assignment must be merged before the
device adopts the address.

The framework never orders that pair.
`SquashMergeStrategy._build_fk_dependency_graph` builds exactly four edge
classes — UPDATE→CREATE, CREATE→CREATE, DELETE→UPDATE, DELETE→DELETE. **There is
no UPDATE→UPDATE case**, and this module added none: its two supplementary
builders produce DELETE→UPDATE and child-before-parent DELETE edges only.

While both rows are new the ordering holds by accident, through the CREATE chain
`device → ipaddress → interface` (the generic FK is handled, so the chain is
intact). It breaks when the device *and* the IP already exist in main and the
branch only re-points them — a `Mgmt_`-tagged device adopting an address that is
being moved onto its interface in the same sync. The two UPDATEs then carry no
edge and fall back to the sort's `last_change.time` tie-break.

Device-first raises:

```
ValidationError({'primary_ip4':
  'The specified IP address (...) is not assigned to this device.'})
```

It is a **race, not a deterministic failure**, which is exactly why it presented
as a single device out of thousands and why replaying the sync never reproduced
it.

**Fix.** `_add_ip_adoption_dependencies` adds the missing UPDATE→UPDATE edge:
an UPDATE on a model in `_DEFERRED_CREATE_FK_FIELDS` whose deferred field points
at `ipam.ipaddress` depends on that IP's UPDATE when both are in the same merge.
It runs with the other supplementary builders, after the framework graph and
before the `squash_dependency_graph_built` signal, and reuses the existing
acyclic guard (generalised with an `edge_label`, behaviour unchanged) so a
cycle-forming edge is dropped rather than wedging the sort.

## Validation

- Read-only ordering probe against the live dev stack, before the fix: with the
  device change older the sorter emitted `[device, ipaddress]` and both
  `depends_on` sets were empty; with the IP change older it emitted
  `[ipaddress, device]`. After the fix both inputs emit `[ipaddress, device]`
  with the edge present.
- `IPAdoptionOrderingTest` — 6 tests pinning both timestamp directions,
  `oob_ip`, virtual machines, the no-op when the IP is not in the batch, the
  non-IP deferred field (`virtualchassis.master`), and the cycle drop.
- `test_bulk_merge`, `test_set_based_merge`, `test_ingestion_merge`,
  `test_accepted_merge_failures`: 137 tests, OK.

## Rollback

Revert the commit through the normal protected pull-request path. The change is
additive — one new edge builder plus a keyword argument with an unchanged
default — so reverting restores the previous ordering exactly. No migration and
no persisted state are involved.

## Decision Log

- 2026-07-30: Fixed the ordering rather than relaxing `Device.clean()` or
  catching the `ValidationError`. The validation is correct; the merge was
  presenting it with a half-applied state.
- 2026-07-30: Scoped the edge to deferred fields that point at `ipam.ipaddress`
  rather than all UPDATE→UPDATE FK references. A general UPDATE→UPDATE builder
  is a much larger behavioural change to the sort, and nothing observed
  justifies it yet. `dcim.virtualchassis.master` is deliberately excluded — it
  is in the deferred set but points at a Device.
- 2026-07-30: Reused `_acyclic_delete_edges` instead of applying edges directly.
  A cycle raises `RuntimeError` and fails the entire merge, and per the
  merge-failure dead end any failed row blocks baseline promotion — so a
  cycle-safe edge is worth more than a guaranteed one.

## Evidence

- `Device.clean()` in NetBox 4.6.5 raises on `primary_ip4`, `primary_ip6` and
  `oob_ip` when `assigned_object` is not in `self.vc_interfaces(if_master=False)`
  and no `nat_inside` match applies.
- `_build_fk_dependency_graph` and both supplementary builders in this module
  were inspected directly; none emits an UPDATE→UPDATE edge.
- This does **not** rely on the customer's data. The failing row itself is still
  unconfirmed against their instance: 2.6.6 records `invalid_fields` for a
  `ValidationError`, so their next sync will name the offending field and either
  confirm `primary_ip4`/`primary_ip6`/`oob_ip` or point elsewhere. Existing
  issue rows carry `raw_data={}` and cannot confirm it retrospectively.
