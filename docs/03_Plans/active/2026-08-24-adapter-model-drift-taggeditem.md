# Measure drift for the adapter-only models, starting with tagged items

## Goal

Give the adapter-only models a real drift comparison, the way the bulk-ORM
models got one in `2026-08-15-measure-real-drift.md`. This is slice one of
eight: `extras.taggeditem`.

## Why

`in_sync` on the drift report is deliberately unanswerable today, and the
reason is named in that plan's Open section and in #206: the adapter-only
models - the lifecycle rows, cables, inventory items, modules, tagged items,
FHRP groups, routing and peering - have no bulk path, so none of the preview
machinery reaches them. Every one of them reports an upper bound, which is
honest and useless, and while any model is uncompared the estate is never
"fully measured".

Tagged items go first because they are the smallest of the eight: one
dependent object, one assignment, no recursive dependency chain, and seven
existing regression tests already covering the apply.

## Constraints

- **The comparison must reuse the apply's own resolution and comparator.** A
  second implementation drifts from the first and reports a number that is
  wrong in whichever direction is least noticeable. Same constraint as the
  bulk slices, and the reason `last_upsert_would_change` is computed with
  `_model_field_value_matches` rather than a fresh comparison.
- **A preview must write nothing**, and for this path that is two writes, not
  one - see below.
- No new Forward calls. This is a NetBox-side read.
- A model that cannot be compared must keep reporting an upper bound rather
  than a zero. Absence from `_ADAPTER_COMPARISONS` is that answer.

## Touched Surfaces

- `forward_netbox/utilities/sync_interface.py` - `apply_extras_taggeditem`
  gains `preview`
- `forward_netbox/utilities/drift_comparison.py` - a read-only
  `_upsert_values_from_defaults` on `PreviewRunner`, the per-row adapter
  comparison, and the dispatch
- `forward_netbox/tests/test_taggeditem_drift_comparison.py` (new)

## Approach

The bulk models classify a batch and return counts. The adapter models apply
one row at a time, so the loop belongs to the caller: `_compare_extras_
taggeditem` iterates, calls the apply with `preview=True`, and tallies the
per-row verdict. That shape is what the remaining seven will reuse.

`_ADAPTER_COMPARISONS` maps a model string to its comparison. Absence from it
is the "no comparison" answer, so adding a model is a deliberate act that
follows an audit of that path's `runner.` calls - not just a grep for ORM
writes, which is the method that missed `_ensure_vrf` in the bulk work.

### The two writes, and why one needed a flag

`runner._upsert_values_from_defaults` writes the `Tag` row. That is behind a
`runner.` call, so the preview runner's firewall covers it: the override
resolves through the same coalesce lookups and stops. This is the primitive
EVERY adapter model writes through, so the one override is most of what makes
the remaining seven reachable.

`device.tags.add(tag)` is the other, and it is a different shape from anything
the bulk slices hit. It is an M2M write reached through a module-level helper
(`_device_add_tag`), not through a `runner.` method - so the firewall does not
see it and cannot neutralise it. That is why `apply_extras_taggeditem` takes a
`preview` flag rather than relying on the shim: this write has to be skipped
by name, in the function.

Worth stating plainly for the next slice: **the firewall is not total.** It
covers writes behind `runner.`, and a path that writes directly - or through a
module-level helper, or through a NetBox-core `save()` side effect - needs its
own guard. Cables (`cable.save()`, `cable.delete()`, and NetBox core creating
`CableTermination` rows) and modules (`Module.save()` with `_adopt_components`,
where NetBox core instantiates component rows) are both in that category.

### Classification

- tag absent in NetBox -> `creates`. NetBox cannot already carry an assignment
  to a tag it does not have.
- tag present, not assigned -> `creates`.
- tag present and assigned, tag fields match -> `unchanged`.
- tag present and assigned, tag fields differ -> `updates`. The apply would
  rewrite the `Tag` row, and calling that unchanged would under-report drift.
- device unresolvable, or the row missing its keys -> `rejected`, counted
  apart from drift. A defect in the row is not a difference between the two
  systems.

An outcome the comparison does not recognise refuses the whole model rather
than falling into `unchanged`, which is the confident-zero failure this
feature keeps producing and the one with a real consequence.

## Validation

12 tests, of which three are the negative space, and each was run as its own
negative control rather than assumed:

- Reintroducing `device.tags.add()` under preview made
  `test_a_preview_does_not_assign_a_tag_that_already_exists` fail.
- Making the read-only upsert create the `Tag` made
  `test_a_preview_creates_no_tag_and_no_assignment` fail.

Those are two DIFFERENT tests catching two DIFFERENT writes, and running the
controls separately is what established that. The first control failed only
one of the two, because when the tag is absent the shim returns `None` and
there is no assignment to make - so neither test is redundant and neither
covers both writes. A single combined control would have suggested otherwise.

Two parity tests apply for real afterwards and assert the preview predicted
the write count, which is the property the number actually sells.

Full run: 300 tests green across `test_taggeditem_drift_comparison`,
`test_drift_comparison`, `test_preview_runner_satisfies_the_priming_contract`,
`test_preview_primes_its_lookup_caches`,
`test_empty_comparison_is_not_a_measurement`,
`test_drift_coverage_is_explained` and `test_sync.ForwardSyncRunnerTest`.

## Rollback

Remove `extras.taggeditem` from `_ADAPTER_COMPARISONS`; it returns to an upper
bound, which is where every adapter model is today. The `preview` parameter
defaults to `False`, so the apply path is unchanged for every existing caller.

## Decision Log

- **Per-row loop in the caller, not a batch API on the apply.** The adapter
  models have no batch to classify, and inventing one would be the parallel
  implementation this feature's first constraint rules out.
- **`updates` counts a drifted `Tag` row.** The apply writes it, so the
  preview must count it. The alternative - counting only the assignment -
  reports zero for a run that would write.
- **A flag on the apply, not another shim method.** Routing the M2M write
  through a `runner.` method purely so the firewall could catch it would add
  indirection to the write path - this repository's highest-incident code -
  to serve the preview. The flag is honest about which write is being skipped
  and why.

## Open

- Seven models remain: cables, inventory items, FHRP groups, lifecycle
  (netbox-dlm), modules, peering, routing. Suggested order is roughly that,
  with routing and peering last and together: peering is 30 lines but calls
  `_ensure_netbox_routing_bgppeer` as its first step, so it inherits the whole
  7-deep BGP-peer chain (VRF, two ASNs, an RIR, an IPAddress, BGPRouter,
  BGPScope, then the peer).
- `in_sync` stays `None` until all eight land. That is unchanged by this slice
  and correct.
- `dcim.virtualchassis` remains deliberately unmeasured; see
  `2026-08-15-measure-real-drift.md`.
