# Measure real drift instead of reporting a workload upper bound

## Goal

Make the drift report state how many objects actually differ between Forward and
NetBox, for the models where that can be computed truthfully, and say plainly
which models it could not compute.

## Why

`EXACT_COMPARISON` is defined in `utilities/drift_report.py` and produced
nowhere. The only payload reaching `compute_drift_report` comes from
`_dependency_dry_run_payload`, which sets `change_estimate_kind` to
`workload_upper_bound` for every model because `estimated_changes` is just
`row_count + delete_count` - every fetched row counted as a change.

So `In sync`, `Drifted models` and `Total drift` read "Not measured" for every
deployment on every run, permanently, and no amount of re-running changes it.
A deployment on 2.8.0 reported exactly this, having upgraded partly in the hope
it would clear. Its `Estimated apply work` read 694,477 against roughly 4,000
devices, which reads as a catastrophic drift figure and is in fact just the row
count.

## Constraints

- **The comparison must reuse the apply path's own normalise-and-classify code.**
  A preview that normalises even slightly differently would report drift that
  does not exist, or hide drift that does. That is worse than reporting nothing,
  because an operator would act on it. This rules out a parallel implementation
  in the preview.
- No new Forward calls. The Forward side is already fetched; the missing half is
  a NetBox-side read.
- The write path is the code that has caused this repository's worst incidents.
  Splitting classification out of it must not change what it writes, and the
  existing apply tests are the evidence for that.
- `comparison_available` is currently `all(models)`, so one uncovered model
  disables the whole report. That gate has to change or the feature cannot ship
  incrementally.

## Touched Surfaces

- `utilities/apply_engine_bulk.py` - split classification from writing
- `utilities/drift_report.py` - per-model exactness, coverage-aware totals
- `views.py` - `_dependency_dry_run_payload` emits real counts where available
- `templates/forward_netbox/forwardsync_drift_report.html`
- tests

## Approach

### What can be compared, and what cannot

Seven bulk-ORM paths each already classify rows as applied or unchanged before
writing: `simple_models` (site, manufacturer, devicetype, vlan, vrf, prefix),
`macaddress`, `interface`, `device`, `ipaddress`, `virtualchassis`,
`tree_models`. Each has an isolated, pure field comparator -
`_model_field_value_matches`, `_interface_field_differs`,
`_device_field_differs` - and an entangled normalise-and-lookup step.

The adapter-only models (the lifecycle rows, cables, inventory items, modules,
tagged items, FHRP groups, routing and peering) have no bulk path. They can be
compared eventually - `sync_primitives` computes `update_fields` the same way -
but each needs its own row resolution, and that is a separate body of work.

So this is deliberately partial, and the report has to say so rather than imply
whole-estate drift.

### The split, and why it is not simply "stop before the write"

The obvious shape - call the apply function and return before `bulk_create` -
is unsafe, and finding out why is the main result of scoping this.

`bulk_orm_apply_simple_models` **writes during dependency resolution**, before
it has classified anything. At `apply_engine_bulk.py:601-602` a missing VRF is
created by a recursive call:

```python
if missing_vrf_rows:
    bulk_orm_apply_simple_models(runner, "ipam.vrf", missing_vrf_rows)
```

A read-only drift preview built on that would create VRF rows in the operator's
NetBox as a side effect of *looking*. Normalisation also calls
`runner._record_issue` and `runner.logger.increment_statistics`, so a preview
would additionally record ingestion issues and move run statistics for a run
that never happened.

So the extraction has three parts, not one:

1. **Dependency resolution** becomes explicit and gains a read-only mode. In
   preview mode a missing dependency is not created; the rows that need it are
   counted as creates, which is what they would be.
2. **Normalisation** takes a sink for issues and statistics rather than reaching
   into `runner`, so a preview can discard them (and, later, report them as
   "rows that would fail").
3. **Classification** - the part that was always the goal - is then pure:
   normalised rows plus resolved existing objects in, `(creates, updates,
   unchanged)` out. Apply calls it and writes; the preview calls it and stops.

Slice one is `bulk_orm_apply_simple_models`, because it is the generic path, it
is the one that contains this trap, and it proves the whole pipeline end to end.
The bespoke paths follow the same shape and must each be audited for the same
class of hidden write before being wired up.

### Reporting partial coverage

`comparison_available` stops being `all(models)`. Each row reports its own
exactness, already carried by `change_estimate_kind`, and the summary reports
drift over the measured models plus an explicit count of the unmeasured ones.
"Drift 412 across 13 of 27 models" is useful and true. "Not measured" is neither.

## Validation

- Parity is the whole risk, so the tests that matter are the existing apply
  tests: the split must leave them untouched and passing.
- New tests pin that a preview and a real apply against the same fixture agree
  on the counts - which is the property the feature actually sells.
- A test pins that an uncovered model reports upper-bound rather than being
  silently counted as zero drift, because reporting a confident zero for
  something never compared is the failure mode with the worst consequence.
- **A test pins that a preview writes nothing.** Count rows in every model the
  preview touches before and after, and assert equality - including the VRF
  case above, where a preview against a Forward result naming an unknown VRF
  must leave the VRF table exactly as it found it. This is the negative space
  for this feature, and it is the assertion that would have caught the
  side-effecting design.

## Slice one, as built

`compare_model_rows` in `utilities/drift_comparison.py` calls
`bulk_orm_apply_simple_models(..., preview=True)`. Covers `dcim.site`,
`dcim.manufacturer`, `dcim.devicetype`, `ipam.vlan`, `ipam.vrf` and
`ipam.prefix`.

Making it read-only took **six** suppressions, not the one hidden write this
plan predicted. The two extra classes were both found by running it, not by
reading it:

1. `:563` creates missing manufacturers - predicted.
2. `:602` creates missing VRFs - predicted.
3. Three non-dict exits (`return False` for an unknown model, `return True`
   when every row was rejected, and the `bulk_orm_apply_tree_models`
   delegation) would each have surfaced as **zero drift** rather than "not
   compared". A bool is falsy and a caller reading counts off it reports a
   confident zero.
4. `full_clean` on creates rejects a row whose required dependency was
   deliberately not created - a device type whose manufacturer is absent fails
   on a null FK while still being, plainly, a row NetBox does not have. A
   preview counts what would change; validation belongs to the apply.

`compare_model_rows` additionally refuses anything that is not a count mapping,
so a path this work has not audited can never surface as zero drift.

### Reporting

`comparison_available` is now `any`, not `all`, and the report carries
`measured_model_count`, `unmeasured_model_count` and `unmeasured_models`.

`in_sync` deliberately stays `None` until every model is compared. Zero drift
across a measured subset is reported as `total_drift`, which states its
coverage; answering "Yes" off a partial measurement would tell an operator they
are in sync when nothing checked the rest, which is the same confident-zero
failure as (3) above wearing a different hat.

## Slice two: the bespoke paths, audited

The audit was run before any of them was wired up, and it justified itself.
Slice one's hidden writes were dependency creates. These are worse - they are
writes to models the function does not own, and one of them is a delete:

| path | writes beyond its own rows |
| --- | --- |
| `macaddress` | none - **cleared** |
| `interface` | **`cable.delete()`**, plus two recursive self-calls |
| `device` | creates `Tag` and `TaggedItem` rows |
| `ipaddress` | `Device.objects.bulk_update` (primary-IP assignment) |
| `virtualchassis` | `Device.objects.bulk_update` |

A preview on `interface` would have deleted cables. On `device` it would have
created tags and tag assignments. "Return before the final `bulk_create`" is
not sufficient for any of the four, and the dispatch now carries this table as
a comment so nobody assumes otherwise.

Only `macaddress` is wired up. The other four return `None` and keep their
upper bound.

### The shim is the cost of coverage

Routing a preview through the apply path means satisfying whatever slice of the
runner that path touches. `macaddress` alone needed `_content_type_for`,
`_get_unique_or_raise`, `_lookup_interface`, `_dependency_failed`,
`_mark_dependency_failed`, `_record_aggregated_skip_warning` and five caches -
each checked for writes, each delegated to the same primitive the real runner
uses so the preview resolves objects exactly as the apply would.

That is the honest per-path cost, and it grows. `_record_issue` is now
signature-agnostic rather than being widened every time a caller passes a new
keyword.

### Counting is uniform now

Counts come from the per-row outcomes the classification already reports, not
from arithmetic per function. Every path increments `applied` for a row it
would write and `unchanged` for one it would not, so there is one definition
instead of one per path - and the paths differ enough (skips, per-row
rejections, in-memory duplicates) that row-total arithmetic quietly disagreed
with them.

`rejected` reads from the outcomes alone. Summing it with the issue count
double-counted, because a single unusable row both files an issue and
increments `failed`.

### Still to do

`interface`, `device`, `ipaddress` and `virtualchassis` - the reporting
deployment's highest-volume models. Each needs its cross-model writes
suppressed individually, and `interface`'s cable delete needs care rather than
a guard: the classification may depend on it having happened.

## Rollback

Revert. The report returns to "Not measured", which is where it has always been.

## Decision Log

- **Reuse apply's classification, do not reimplement it.** A second normaliser
  drifts from the first, and the symptom is a drift number that is wrong in
  whichever direction is least noticeable.
- **Ship partial coverage.** Requiring all 27 models before showing any number
  means three times the work before the reporting deployment sees anything, and
  its volume is entirely in models slice one and two cover.
- **Do not relabel the panel instead.** That was the cheaper option and it was
  considered; the measurement is what the page has always promised, and
  convergence answers a narrower question ("did the last run change anything")
  than "how far apart are these two systems".

## Open

- Whether the added NetBox-side read makes the preview job materially slower on
  a large estate. It is DB-side rather than Forward-side, so it costs no NQE
  calls, but it is roughly the read half of a staging pass.
- The adapter-only models need their own slice, and until then the report will
  say so.
