# The macaddress preview stops paying for objects it never saves

## Goal

Cut the cost of the `dcim.macaddress` drift comparison, which one deployment
measured at 270,640 ms for 121,900 rows - 42% of its entire 10.6-minute
report - without moving a single count.

## Why

The deployment's 2.9.0 drift report timed every model, which is why this is
known at all. The adapter-model comparisons were the suspect - they loop per
row - so they were measured first against the deployment's own bulk baseline
(2.2 ms/row): tagged items 0.877 ms/row, inventory items 1.952, cables 3.216,
all with flat 2-4 queries/row. The per-row loop is not the problem.

`dcim.macaddress` is, and it is a BULK path. Profiling 4,000 rows showed
12.3 of 12.5 seconds in Python, not SQL - the slowest single query was 2 ms.
The signature was ~2,000 `extras_customfield` and ~2,000 content-type queries:
NetBox model construction and validation, per row, for objects a preview never
saves.

## Constraints

- The APPLY path must be byte-identical. Every change sits behind
  `if preview:`.
- The counts must not move: creates keyed and collapsed exactly as the apply
  keys them, updates keyed by pk exactly as the apply keys them.
- The one classification divergence this buys must be pinned by a test and
  named in the code, so closing it later is a decision, not an accident.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` - two `if preview:` short
  circuits in `bulk_orm_apply_macaddress`
- `forward_netbox/tests/test_drift_comparison.py` -
  `MacAddressPreviewCostContractTest`
- `forward_netbox/tests/test_adapter_drift_scale.py` - the measurement
  harness (gated behind `FORWARD_ADAPTER_SCALE`), including the bulk-path
  measurement that found this

## Approach

Two per-row blocks did work that only matters when something is saved:

- **The create branch** constructed a real `MACAddress` and ran
  `full_clean(validate_unique=False, ...)`. Construction alone triggers the
  custom-field machinery's queries. Under preview it now records a
  `SimpleNamespace` sentinel carrying `pk=None` and the two assignment
  attributes a later duplicate row compares against - so a duplicate incoming
  row walks the same path it walks in the apply (the update branch sees
  `pk=None` and contributes nothing), and the counts stay identical.
- **The update branch** snapshotted, mutated the object and ran a FULL
  `full_clean` - uniqueness query included - to perform a reassignment a
  preview does not perform. It now records `update_objects[mac.pk]` and
  moves on. Keyed by pk as before, so duplicate incoming rows for one
  persisted MAC still collapse to one update.

### The divergence, bought deliberately

`full_clean` is also what classified an invalid row as failed. Without it, a
row NetBox would reject counts as a create (create branch) or an update (a
reassignment of an object's primary MAC). That overstates drift for invalid
rows, never understates it - an operator investigates a number that will not
converge, where an understated number tells them nothing is wrong.

This is the one place preview and apply are ALLOWED to disagree about a row,
after a session spent eliminating exactly that class of bug - which is why it
is a pinned test (`test_an_invalid_row_counts_as_a_create_under_preview`) and
a named block comment rather than a side effect. The deployment whose numbers
motivated this reports `Failed 0`, so the divergence is currently vacuous
there.

## Validation

Measured on the same box as the earlier numbers, 4,000 rows, half existing:

| | ms/row | queries | wall |
| --- | --- | --- | --- |
| before | 3.117 | 9,000 | 12,470 ms |
| after | 0.052 | 13 | 207 ms |

60x. Extrapolated to the deployment's 121,900 rows: ~270 s becomes ~6 s,
returning ~4.4 minutes of its 10.6-minute report.

Contract tests: create/update/unchanged classification unchanged; duplicate
incoming rows collapse to one create (negative control: breaking the
sentinel's `pk=None` fails exactly that test); the divergence pinned.

476 tests green, including all of `test_sync` and `test_apply_engine` - the
apply path this file also implements.

## Rollback

Revert. The comparison returns to constructing and validating per row, which
is correct and 60x slower.

## Decision Log

- **Measure before optimising, and measure the RIGHT suspect.** The adapter
  loops looked expensive and measured cheap; the bulk path looked done and
  measured dominant. Both conclusions came from the same harness.
- **Take the divergence, pinned.** The alternative - keeping `full_clean` for
  classification fidelity on rows that are already broken - costs 42% of the
  report for a distinction the drift numbers barely express.
- **A sentinel over a model instance.** Constructing even an unvalidated
  `MACAddress` pays the custom-field cost; the downstream code reads exactly
  two attributes and a pk, so that is what the sentinel carries.

## Open

- `dcim.interface` at 357,274 rows is the deployment's next-largest compared
  model; whether its bulk preview carries similar dead weight has not been
  measured.
- Three adapter models remain uncompared: lifecycle (netbox-dlm), peering,
  routing.
