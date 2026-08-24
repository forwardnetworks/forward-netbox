# The macaddress preview stops paying for objects it never saves

## Goal

Stop the `dcim.macaddress` drift comparison constructing and validating model
objects it never saves, without moving a single count.

Note the goal has been narrowed since this plan was written. It began as "cut
the 270,640 ms this model cost one deployment"; measurement showed the change
does not touch that deployment's case at all. See Validation.

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

**That measurement was incomplete, and the conclusion drawn from it was
wrong.** Seeding MACs that existed but were UNASSIGNED makes every seeded row
take the update branch - a drifted shape. Re-measured across the shapes that
actually occur:

| estate shape | before | after |
| --- | --- | --- |
| converged (present AND correctly assigned) | 0.063 ms/row | 0.063 - no change |
| present, needs reassignment | 2.786 ms/row | 0.066 |
| all creates (first sync) | 3.481 ms/row | 0.041 |

So the win is ~70x on a first sync (121,900 MACs, ~7 min -> ~5 s) and on
drifted estates, and NOTHING on a converged one, because the unchanged branch
never constructed anything to begin with.

The reporting deployment's macaddress is converged (drift 0, In sync), so this
change does not speed up its next run. The earlier claim that it returns ~4.4
minutes of that report is retracted.

Contract tests: create/update/unchanged classification unchanged; duplicate
incoming rows collapse to one create (negative control: breaking the
sentinel's `pk=None` fails exactly that test); the divergence pinned.

476 tests green, including all of `test_sync` and `test_apply_engine` - the
apply path this file also implements.

## Rollback

Revert. The comparison returns to constructing and validating per row, which
is correct and ~70x slower on create-heavy runs, and identical on converged
ones.

## Decision Log

- **Measure before optimising, and measure the RIGHT suspect.** The adapter
  loops looked expensive and measured cheap; the bulk path looked done and
  measured dominant. Both conclusions came from the same harness.
- **Take the divergence, pinned.** The alternative - keeping `full_clean` for
  classification fidelity on rows that are already broken - costs ~70x on a
  first sync for a distinction the drift numbers barely express.
- **Re-measure when the fixture shape is a guess.** The first numbers here
  were taken at "50% already present", chosen for branch coverage rather than
  because it modelled anything. It modelled a drifted estate, and the
  deployment in question is converged - so a real 70x improvement was written
  up as a saving that deployment will not see. The create/assignment ratio was
  the whole story and was not a knob until it had already misled a conclusion.
- **A sentinel over a model instance.** Constructing even an unvalidated
  `MACAddress` pays the custom-field cost; the downstream code reads exactly
  two attributes and a pk, so that is what the sentinel carries.

## Open

- **That deployment's 270 s for this model is still unexplained, and the local
  hypotheses are now exhausted.** Converged macaddress measures ~0.065 ms/row
  and is LINEAR to 64,000 rows (0.071 / 0.065 / 0.091 at 4k / 16k / 64k), so
  its 121,900 rows should cost ~11 s. Ruled out by measurement, each with a
  knob left in the harness so the next person does not repeat it:

  | hypothesis | knob | result |
  | --- | --- | --- |
  | non-linear at scale | `FORWARD_ADAPTER_SCALE_ROWS` | linear to 64k |
  | priming scales with estate, not batch | `FORWARD_ADAPTER_IFACES_PER_DEVICE` | 30x the interfaces, 1025 -> 1081 ms |
  | per-row interface-lookup fallback | `FORWARD_ADAPTER_IFACE_MISS` | 3.8x, so ~30 s at its scale - not 270 |
  | per-row construct/clean | (this change) | irrelevant when converged |
  | branch-rewritten queries | read `_dependency_preview_work` | the preview job activates no branch |

  What remains is environmental or a data shape this fixture does not model:
  its hardware and concurrent load, custom fields configured on `MACAddress`,
  or rows taking a branch the fixture does not produce. Closing it needs
  per-model instrumentation from that deployment - a query count and a
  breakdown by classification outcome - not another local fixture. The fixture
  only models what it is told to, which is how the retracted claim above
  happened in the first place.
- `dcim.interface` at 357,274 rows was measured and is NOT a problem for that
  deployment: 0.060 ms/row converged (~21 s). On a first sync it is 1.799
  ms/row (~10.7 min), so the exposure there is first-sync, and it carries the
  same construct-per-create shape this change removed for macaddress.
- Three adapter models remain uncompared: lifecycle (netbox-dlm), peering,
  routing.
