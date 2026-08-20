# The preview paid per row for work the apply does in bulk

## Goal

Make the drift preview cost what the apply costs on a converged estate, and
make every row on the drift report identify itself.

## Why

A deployment's drift report priced its own comparison for the first time - the
instrumentation added in 2.8.4, reporting from the estate that has the scale:

    Comparison cost   1452552 ms for 558380 rows
                      Slowest: dcim.interface at 842445 ms

Twenty-four minutes, 58% of it in one model, to measure an estate whose real
drift was 440 rows. `dcim.interface` alone was 387 rows of drift across 357,864
rows read - so essentially every row the slow model looked at was unchanged.

The cause is one missing call. `apply_model_rows` calls
`prime_dependency_lookup_caches` before handing rows to the classification, so
the real apply reads parent devices, interfaces, tags and VLANs in a handful of
bulk queries. `compare_model_rows` calls `bulk_orm_apply_simple_models`
directly - deliberately, so the comparison cannot drift from the apply - and
that route skipped the priming. The identical classification code then resolved
each parent one row at a time against caches that started empty and only ever
filled from their own misses.

Measured on the same fixture before and after, `dcim.interface` rows that match
an existing object:

| rows | queries before | queries after |
|-----:|---------------:|--------------:|
|    6 |             15 |             3 |
|   24 |             51 |             3 |

Flat, not merely smaller. That is the shape of every routine preview.

The second defect is on the same page. There is one row per WORKLOAD, and the
report labelled each by its model, so an estate with three `dcim.inventoryitem`
maps got three rows all reading `dcim.inventoryitem` - one showing 0 Forward
rows and 56 pending removals, another 48 and 0 - and "Not compared" printed the
same name three times in a row. The rows were right and only the label was
ambiguous, but a page that prints one identifier three times with different
numbers beside it reads as a rendering fault, which is corrosive to a report
whose entire job is to be believed.

The third item is unrelated to the customer and belongs to the release machine.
`verify_release_provenance._github_json` used `urllib.request`, which forces
`Connection: close` on every request, and this host stalls close-mode
responses. It cost four release cycles and was worked around with a
`sitecustomize` shim that lived outside the repository - so every tagged tree
still carried the broken path.

## Constraints

- The comparison must keep routing through the real apply path. A separate
  normaliser would drift from it, and the symptom would be a drift figure wrong
  in whichever direction is least noticeable.
- Priming must be read-only. A preview that wrote anything into an operator's
  NetBox as a side effect of being looked at is the one failure this module
  already guards against.
- No label may carry an execution value; shard keys are device data and do not
  belong on a rendered page.
- The provenance verifier must not retry an HTTP status. A 403 or a 404 is an
  answer about the release, and retrying it turns a clear refusal into a slow
  one.

## Touched Surfaces

- `forward_netbox/utilities/drift_comparison.py` - prime before classifying.
- `forward_netbox/utilities/drift_report.py` - per-workload labels, and the two
  consumers that report a name.
- `forward_netbox/templates/forward_netbox/forwardsync_drift_report.html` -
  render the label.
- `scripts/verify_release_provenance.py` - `_github_json` transport.
- `forward_netbox/tests/test_preview_primes_its_lookup_caches.py` (new).
- `forward_netbox/tests/test_drift_rows_are_distinguishable.py` (new).
- `.pre-commit-config.yaml` - both new modules keep a docstring ahead of their
  imports, so they join the reorder-imports exclusion.

## Approach

1. `drift_comparison.compare_model_rows` primes the dependency lookup caches
   before classifying, exactly as `apply_model_rows` does. Priming is bulk
   SELECTs into runner-local dicts and the `PreviewRunner` already seeds every
   cache it touches, so a preview may do it verbatim.

2. `drift_report` labels each row by its model, qualified by the query behind
   it only where a model has more than one workload, falling back to position
   where the query name is absent or shared. The execution value that would
   also separate them is shard data and stays off the page. `unmeasured_models`
   and `slowest_compared_model` report the label.

3. `verify_release_provenance._github_json` speaks HTTP/1.1 through
   `http.client` and sends no `Connection` header, with four bounded retries
   covering transport faults only - an HTTP status is an answer and is raised
   on the first response.

## What is deliberately NOT changed

The create path still costs two queries per row: a row with no counterpart is
instantiated, and NetBox charges for content type and custom field defaults per
instance. The only way to avoid that is to stop routing the preview through the
real apply, which is the exact divergence `drift_comparison` exists to prevent
- a cheaper comparison that answers differently is worse than a slow one,
because an operator acts on the number. The cost is measured and pinned as
per-row rather than quietly described as fixed.

## Validation

Evidence below. Full Django suite and `invoke ci` before the release cut.

## Evidence

- `test_preview_primes_its_lookup_caches` - identical counts primed and cold;
  matching rows cost a constant number of queries at 6 and at 24; primed is
  strictly cheaper than cold; the create path is still per-row.
- `test_drift_rows_are_distinguishable` - a sole workload keeps its bare model
  name; three workloads for one model get three distinct labels; workloads
  sharing a query name, and workloads with none, still separate; the
  "Not compared" list names each once; no label carries an execution value.
- The HTTP change was exercised against the live GitHub API on the affected
  host: the new path completed 8 of 8 requests at ~400 ms each, while the
  `urllib` path it replaces could not complete 6 requests in two minutes.

## Rollback

Each of the three is independent and revertible on its own. Reverting the
priming call restores the previous cost and changes no reported number - the
counts are pinned identical either way, which is what makes it safe to revert
under time pressure. Reverting the labels restores the ambiguous names.
Reverting the transport restores a verifier that cannot complete on this host.

## Decision Log

- **Prime rather than shortcut.** The obvious alternative - detect unchanged
  rows without instantiating them - is faster still and would let the
  comparison disagree with the apply. Rejected on the same reasoning that put
  the comparison on the apply path to begin with.
- **Qualify only ambiguous rows.** Labelling every row `model (query)` would
  put a redundant suffix on the twenty-odd models that have exactly one
  workload, to fix the two that do not.
- **Position, not execution value, as the last resort.** Two workloads for one
  model can share a query name. The value that always separates them is shard
  data, so the label counts instead.
- **`http.client` rather than a retry loop over `urllib`.** Retrying would have
  worked - eventually - at 30 seconds per stalled attempt, against a host that
  fails roughly five close-mode requests in six. Removing the header the host
  reacts to is the fix; retries stay for ordinary transport faults.
- **The create-path cost is documented, not optimised.** See above.

## Open

- The customer's next preview supplies the after number for the 24 minutes.
  Expect the interface model to collapse; the models with large create counts
  will not.
- This host still stalls close-mode HTTP for every other caller. The verifier
  no longer depends on it, which is not the same as it being fixed, and the
  machine needs attention outside a release window.
