# OSPF rows that no sync can ever resolve

## Goal

Stop reporting drift that cannot be fixed. On a customer estate 180 of 2854
`netbox_routing.ospfinterface` rows drifted on every run - including a
comparison taken 22 minutes after a sync that applied 8240 changes with zero
failures, against the same snapshot. `In sync` could never become Yes, and an
operator who learns that stops reading the number at all.

## Why

`forward_ospf_interfaces.nqe:33` selects `foreach neighbor in area.neighbors`:
**one row per OSPF neighbour**, each carrying `local_interface`. A broadcast
segment with three neighbours is three rows with the same interface.

`ensure_ospf_interface` (`sync_routing_impl.py:807`) upserts with
`coalesce_sets=[("interface",)]`: **one OSPFInterface per NetBox interface**.

So the three rows collapse onto one object and each writes its own `comments` -
remote device, remote interface, remote interface IP and remote router ID all
differ per neighbour. The last row wins. Every later comparison then finds the
other two still wanting to write theirs, and reports them as drift. Forever.

The count is stable rather than growing because it is exactly the surplus:
`N - 1` rows per multi-neighbour interface, on every run.

`netbox_routing.ospfarea` has the same shape - `coalesce_sets=[("area_id",)]`
with a row per reporting device - and showed 5 of 28.

This is NOT the volatile-counter churn fixed for BGP peers
(`test_bgp_peer_comment_churn.py`); `ospf_interface_comments` renders nothing
that moves between snapshots. The rows are stable and still never converge,
because the problem is arithmetic, not volatility.

## Constraints

- **The single-neighbour case must be byte-identical.** Most interfaces have
  one neighbour, and a merge that reformatted them would rewrite the estate to
  fix a minority.
- **The apply and the comparison must collapse identically.** The defect is the
  two disagreeing about how many objects a row set means; doing it in one of
  them would be the same defect in a new place.
- **The merge must not depend on Forward's row order**, or a churn that recurs
  every run becomes one that recurs whenever ordering moves - which looks
  intermittent and is harder to diagnose.
- **A real change must still be reported.** A fix that stops looking is not a
  fix.

## Touched Surfaces

- `forward_netbox/utilities/row_collapsing.py` (new)
- `forward_netbox/utilities/sync_routing_impl.py` - `COLLAPSED_COMMENTS_KEY`
  and the one lookup in `ospf_interface_comments`
- `forward_netbox/utilities/sync_reporting.py` - `apply_model_rows`
- `forward_netbox/utilities/drift_comparison.py` - `compare_model_rows`
- `forward_netbox/tests/test_row_collapsing.py`,
  `forward_netbox/tests/test_ospf_neighbour_rows_converge.py` (new)

## Approach

Collapse rows to one per object **upstream of both** the apply and the
comparison, in a module both import.

For `ospfinterface` the group key is the device plus the canonical interface
name, expanded through the lookup's own alias table so `gi0/0` and
`GigabitEthernet0/0` group exactly when they would resolve to the same NetBox
interface. The longest alias wins, because the table holds both `gi` and
`gigabitethernet` and the short one also matches the long one's expansion.

The merged row is the first neighbour's row, sorted, carrying the other
neighbours' detail appended to its comments under a reserved key that
`ospf_interface_comments` reads. A one-neighbour group returns its row
untouched and never carries the key, so its comments are exactly what they
were.

For `ospfarea` nothing is merged - the area's fields are the area's, not the
reporting device's - only a deterministic winner is chosen, so that two devices
disagreeing about `area_type` stop taking turns.

## Validation

- `test_ospf_neighbour_rows_converge` drives `compare_model_rows` with three
  neighbours on one interface. **Against the shipped 2.9.2 code it reports 2
  updates** - `N - 1`, exactly the field symptom - and 0 after the fix.
- The upgrade cost is pinned rather than discovered: an estate synced by the
  old code rewrites **once** per affected interface and is then stable.
- Order independence, alias grouping, single-row byte-identity and
  "a real change still reports" each have their own test.
- The existing OSPF, peering and BGP-churn suites are unchanged and still pass.
- Full Django suite.

## Rollback

Revert. The collapse is additive and reversible; the only persisted effect is
the merged `comments` text on multi-neighbour interfaces, which a revert
rewrites back on the next sync.

## Decision Log

- **Collapse rather than dedupe in the report.** Counting only the first row
  per object would fix the number and leave the sync still writing one object
  N times, with the ObjectChange and branch-merge cost that implies.
- **Not fixed in the NQE query.** Aggregating neighbours query-side is the
  cleaner data model, but the bundled queries are published into customer org
  libraries and a shape change there is a migration for every deployment. This
  is a Python change to a maintenance branch.
- **`bgppeer` is NOT included.** It showed 126 drifted rows on the same estate
  and may be the same shape, but its coalesce key spans a router and scope
  built from other fields, and nothing here has demonstrated the collapse. A
  fix that looked right and was not would be worse than the honest gap.

## Open

- **`netbox_routing.bgppeer`: 126 rows on the customer estate, unexplained.**
  Volatile counters were already excluded from `bgp_peer_comments`, so it is
  something else. Needs the same treatment this got: reproduce first.
- **The comparison cost is untouched.** 1010213 ms for 648353 rows in 246591
  queries, `dcim.macaddress` alone 297643 ms of it with 247134 ms in SQL. The
  collapse removes some adapter row work, but the macaddress cost is a separate
  question and this does not claim to have moved it.
