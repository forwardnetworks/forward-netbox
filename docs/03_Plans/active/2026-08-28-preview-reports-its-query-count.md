# The preview reports its query count and its SQL time, not only its runtime

## Goal

Make the drift report say WHY a model was slow, by recording queries per model
AND the time those queries spent in the database, alongside the milliseconds it
already records.

## Why

A deployment reports `dcim.macaddress` at 276,377 ms for 122,478 rows - 2.26
ms/row - and that model is fully converged: 0 change candidates, drift 0, in
sync. The same converged shape measures 0.065 ms/row locally, a ~35x gap.

Every hypothesis testable against a local fixture has been eliminated, each
with a knob left in `test_adapter_drift_scale.py` so nobody repeats the work:
non-linearity (linear to 64,000 rows), estate interface count (30x the
interfaces moved 1025 ms to 1081 ms), the per-row interface-lookup fallback
(3.8x, so ~30 s at that scale rather than 276), per-row construct and
`full_clean` (irrelevant when converged, measured identically with and without
the fix), and branch-rewritten queries (the preview job activates no branch).
netbox-branching 1.1.3 is now ruled out too: across that upgrade the figure
moved 2.220 -> 2.257 ms/row, which is to say not at all.

What remains splits in two, and the two want opposite fixes. A high query count
is chatter to batch. A low count against a high runtime is work inside Python -
NetBox model instantiation, custom fields, serialization. The report could not
tell them apart, and a runtime alone never will.

**The count alone does not finish the split, and the local numbers say which
way it will fail.** The converged comparison issues a FLAT handful of queries -
13 for 4,000 rows, not one per row - so this deployment will almost certainly
report a low count, and "low count, high runtime" reads as Python. It has a
second reading the count cannot exclude: few queries, each slow. A sequential
scan over 122,478 rows on a table this fixture cannot model, or a Postgres a
network hop away where every round trip costs milliseconds, produces the same
low count and wants an index or a move - not a rewrite. Only the share of the
runtime spent inside `execute` separates them, and reporting a count without it
would point the next person at Python on the strength of a number that cannot
say so.

## Constraints

- The count must be taken the way production runs. `connection.queries` only
  populates under `DEBUG`, which a release deployment never sets - so the
  number that matters most is exactly the one that would be absent.
- A model with no comparison must report `None`, not `0`. Zero queries reads as
  "free", which is a measurement that was never made. The same rule holds for
  the SQL time, and it has a real shape rather than a hypothetical one: the
  count shipped in the first commit of this branch and the timing in the
  second, so a payload carrying a count and no SQL time will exist. Zero there
  would read as "the database was free" - the exact wrong conclusion to hand
  someone reading a 276-second model.
- The timing must survive a query that RAISES. A statement that times out is
  disproportionately likely to BE the slow one; recording the elapsed time only
  on the success path would drop the measurement in the case that most needs
  it.
- No behaviour change to the comparison itself. This measures; it does not fix.

## Touched Surfaces

- `forward_netbox/views.py` - `_QueryMeter` (was `_QueryCounter`; it now times
  as well as counts), the per-model wrap, and both numbers in the coverage
  block and in each model result
- `forward_netbox/utilities/drift_report.py` - carries the count through, and
  onto `slowest_compared_model`
- `forward_netbox/templates/forward_netbox/forwardsync_drift_report.html`
- `forward_netbox/tests/test_preview_reports_its_cost.py`

## Approach

`connection.execute_wrapper` rather than `connection.queries`, for the reason
in the constraints. The wrapper is always active and costs one integer
increment per query, so it can stay on in production - which it must, because
the deployment that needs it is a customer's.

The count is reported in three places: summed across compared models in
`comparison_coverage`, per model in `model_results`, and on
`slowest_compared_model` - because naming the slowest model is half an answer
and the count is the half that says which kind of slow it is. The SQL time is
carried through the same three places, summed over the same models so that a
share is read against the runtime it is a share OF. The report renders
"Slowest: dcim.macaddress at 276377 ms in 13 queries, 271900 ms of it in SQL".

The elapsed time is taken in a `finally` around `execute(...)`, for the reason
in the constraints, and costs one clock read per query on top of the increment.

## Validation

11 tests, split deliberately across the two things that can be wrong:

- Six feed a synthetic payload and pin what the REPORT does with the number,
  including `None` rather than `0` for a payload written before this existed.
- Three exercise the METER itself, because the first six would all pass while
  the preview reported a count it never took - the plumbing verified and the
  measurement absent. One of them asserts the count is taken with `DEBUG=False`,
  which is the property that ruled out `connection.queries`.

Two negative controls, both confirmed failing without their fix: removing the
report plumbing fails the three payload tests; disabling the counter's
increment fails the two meter tests that can observe it.

The SQL timing adds six more on the same split: three that pin what the REPORT
does with it (including the count-without-timing payload above), and three on
the meter - it times what it counts, it times nothing when nothing runs, and a
failing query still reports what it spent. Negative controls confirmed both
ways: replacing the `finally` body with `pass` fails exactly the two meter
tests that can observe elapsed time (including the failing-query one), and
removing the three report lines errors exactly the three payload tests.

495 tests green across the drift, preview, sync and view suites.

## Rollback

Revert. The report returns to reporting a runtime with no way to explain it.
Reverting only the second commit leaves a count with no way to tell a slow
query from slow Python, which is the state this plan argues is not enough.

## Decision Log

- **`execute_wrapper`, not `connection.queries`.** The latter is empty in every
  environment where this question is worth asking.
- **`None`, not `0`, for an uncompared model.** Same rule the runtime already
  follows, and for the same reason.
- **Test the meter separately from the plumbing.** Two tests written earlier
  this session passed against the defects they named; splitting these was a
  direct response to that.
- **Ship the measurement before the fix.** The remaining hypotheses need this
  number to be worth testing, and inventing a scenario to justify a fix is
  precisely how an earlier claim in this work had to be retracted.
- **Time the queries as well as counting them, in the same change that ships
  the count to a customer.** The count was going to come back low - the local
  converged shape says so - and a low count with nothing beside it argues for
  the wrong fix. Sending a customer's operators to instrument a second run
  because the first one measured half the question is a cost this avoids for a
  clock read per query.

## Open

- This does NOT fix the 276 s. It makes the next report able to explain it.
- How to read the number that comes back, decided BEFORE seeing it so the
  reading is not fitted to it:

  | queries | SQL share | reading | fix |
  | --- | --- | --- | --- |
  | high | high | chatter | batch the per-row queries |
  | high | low | chatter, cheap each | batch, or accept |
  | low | high | few slow queries | index, vacuum, or DB locality |
  | low | low | work inside Python | custom fields, model construction |

- Only the bottom-left cell is new, and it is the one the count alone would
  have mis-read as the bottom-right. It also names environmental suspects the
  fixture cannot model at all: table bloat behind 122,478 rows, a query plan
  that flips to a sequential scan at that size, and a Postgres on another host.
- If it lands bottom-right, the next suspects are still custom fields
  configured on `MACAddress` and the per-instance cost of NetBox model
  construction - and custom fields, at least, are reachable locally as a
  SENSITIVITY sweep (measure at 0, 10, 30 fields on `MACAddress`) even without
  knowing that deployment's configuration: if 30 fields barely move ms/row, the
  hypothesis dies regardless of how many they have.
