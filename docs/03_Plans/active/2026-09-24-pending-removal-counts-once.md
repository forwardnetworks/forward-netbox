# Pending-removal counts shown once, not duplicated per map

## Goal

A customer's dependency preview showed 1,394 pending `dcim.inventoryitem`
removals on a map that had fetched zero rows. The comparison behind that
number is computed once per MODEL (`_compare_rows_by_model` pools every
map's rows before comparing), but three built-in maps target
`dcim.inventoryitem`, and the same model-wide delete/create/update numbers
were attached to every one of them - so a model with N maps sharing it
counted its removals N times over in any total that summed across maps
(the drift report's `total_removes`, and the durable-workload-state
diagnostic carried the identical broadcast problem). Make each removal,
creation and update count once, attributed to exactly one map, with its
siblings explicitly marked as sharing it rather than silently repeating it.

## Constraints

- The comparison itself stays model-wide by design (`_compare_rows_by_model`
  pools rows across every map of a model before comparing) - this fixes
  where the result is ATTRIBUTED, not how it is computed.
- A map that legitimately has its own Forward-declared deletes
  (`data["delete_count"]`, separate from the shared comparison) must keep
  showing them - the fix only stops adding the SHARED comparison numbers a
  second (and third) time.
- Backward compatible for direct unit callers of
  `_dependency_model_result_summary`: the new parameter defaults to
  attributing the comparison, matching every existing call site that only
  ever compares one map's result to one model's comparison.

## Touched Surfaces

- `forward_netbox/views.py`: `_dependency_model_result_summary` (new
  `attribute_model_comparison`/`comparison_shared_with_sibling_maps`
  parameters), `_dependency_dry_run_payload` (the `model_results` list
  comprehension becomes a loop that tracks which model has already been
  attributed).
- `forward_netbox/utilities/query_fetch_execution.py`:
  `_record_durable_workload_state_summaries` (same "first map wins" pattern
  for the durable-workload-state diagnostic).
- `forward_netbox/utilities/drift_report.py`: unaffected - `total_removes`
  sums `model_results[*]["delete_count"]` and is now correct automatically
  once each row carries its own true number.
- Tests: `test_dependency_preview_summary.py` (extended),
  `test_durable_workload_state_attribution.py` (new).

## Approach

1. **Attribute the comparison to exactly one map per model, in fetch
   order.** `_dependency_dry_run_payload` tracks `attributed_models` (a
   set) while building `model_results`; the first `ForwardModelResult` seen
   for a model gets `attribute_model_comparison=True`, every later one for
   the same model gets `False`. `sibling_map_counts` (a `Counter` over every
   map's model string) is passed through as
   `comparison_shared_with_sibling_maps` on every row, so an operator
   reading ANY map's numbers - the one carrying them or a sibling - knows
   there are others to check.
2. **Inside `_dependency_model_result_summary`**, `attribute_comparison =
   comparison is not None and attribute_model_comparison`. When true,
   behavior is exactly what it always was (`delete_count` folds in
   `comparison["deletes"]`, `estimated_changes` is `creates + updates`,
   `change_estimate_kind` is `"exact_comparison"`). When false, the map
   shows only its OWN `data["delete_count"]` and reads as an upper bound of
   its own fetched rows, with a new `change_estimate_kind` value,
   `"shared_with_sibling_maps"`, distinguishing it from a model that
   genuinely had no comparison at all (`"workload_upper_bound"`).
3. **The durable-workload-state diagnostic gets the identical treatment**
   in `_record_durable_workload_state_summaries`: the first
   `ForwardModelResult` for a model gets the real consolidated dict: every
   later one gets `{"type": "durable_workload_state",
   "shared_with_sibling_map": True}` instead of a second copy of the same
   numbers.
4. **`drift_report.py` needed no direct change.** `total_removes` already
   summed `delete_count` across every `model_results` row; once each row
   carries its own true (non-duplicated) number, the sum is correct without
   touching that file.

## Validation

- `test_dependency_preview_summary.py`: the attributed map gets the full
  comparison; a sibling map with zero rows shows zero, not the shared
  deletes; a sibling map still shows its own Forward-declared deletes
  separately from the shared ones; the parameter has no effect when there
  is no comparison at all (backward compatible with every existing call).
- `test_durable_workload_state_attribution.py`: the first map for a model
  gets the full diagnostic; a sibling gets the marker, not a copy; three
  maps sharing a model only the first carries it; unrelated models and a
  model with no summary are unaffected.
- Regression: `test_drift_comparison`, `test_drift_coverage_is_explained`,
  `test_drift_rows_are_distinguishable`, `test_preview_reports_its_cost`,
  `test_preview_isolation`, `test_converged_models_are_measured`,
  `test_failed_run_model_evidence`, `test_health` - 157 tests total, green.
- Full `invoke ci` before push.

## Rollback

Single change set, no migration. Revert restores the previous
per-map-duplicated counts.

## Decision Log

- **Attribution by fetch order, not by which map "looks most relevant."**
  Any deterministic rule works equally well for correctness (only one map
  may carry the shared number); fetch order needs no extra bookkeeping and
  is what `fetcher.model_results` already provides for free.
- **A new `change_estimate_kind` value rather than reusing
  `"workload_upper_bound"`.** A sibling map's estimate IS an upper bound of
  its own rows, but conflating it with "no comparison ran for this model at
  all" would hide the fact that a comparison DID run and its real numbers
  are on a sibling row - exactly the information an operator needs to find
  the removals `1,394` actually came from.
