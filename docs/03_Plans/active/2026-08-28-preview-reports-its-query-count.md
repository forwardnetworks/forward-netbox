# The preview reports its query count, not only its runtime

## Goal

Make the drift report say WHY a model was slow, by recording queries per model
alongside the milliseconds it already records.

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

## Constraints

- The count must be taken the way production runs. `connection.queries` only
  populates under `DEBUG`, which a release deployment never sets - so the
  number that matters most is exactly the one that would be absent.
- A model with no comparison must report `None`, not `0`. Zero queries reads as
  "free", which is a measurement that was never made.
- No behaviour change to the comparison itself. This measures; it does not fix.

## Touched Surfaces

- `forward_netbox/views.py` - `_QueryCounter`, the per-model wrap, and the
  count in both the coverage block and each model result
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
and the count is the half that says which kind of slow it is. The report renders
"Slowest: dcim.macaddress at 276377 ms in N queries".

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

381 tests green across the drift, preview and sync suites.

## Rollback

Revert. The report returns to reporting a runtime with no way to explain it.

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

## Open

- This does NOT fix the 276 s. It makes the next report able to explain it.
- If the count comes back high, the fix is batching. If it comes back low, the
  next suspects are custom fields configured on `MACAddress` and the per-instance
  cost of NetBox model construction at 122,478 rows - neither reproducible on a
  fixture without knowing that deployment's configuration.
