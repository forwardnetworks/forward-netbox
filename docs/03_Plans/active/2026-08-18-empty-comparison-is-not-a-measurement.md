# An empty row list is not a measurement

## Goal

Stop the drift report showing `In sync: Yes` for a model nothing compared.

## Why

A deployment's drift report read `Models compared 2 / 32`. One of the two was
`netbox_dlm.softwareversion`, with 45 Forward rows, drift 0 and a green
`In sync: Yes`.

That model cannot have been compared. It appears nowhere in
`apply_engine_bulk.py`; it is applied by the `sync_dlm` adapter. Handed to the
comparison dispatcher it falls through to `spec.get(model_string)`, which
returns `None` - "no comparison for this model" - and the drift report is
supposed to keep its upper-bound estimate and say `Not measured`.

The badge came from a branch that ran before the dispatcher was consulted:

    if not rows:
        return {"creates": 0, "updates": 0, "unchanged": 0, "rejected": 0}

The arithmetic is unarguable - no incoming rows, so nothing to create or
update - and the conclusion is still wrong, because it answers on behalf of
models this code has no way to compare. Emptiness was doing a job it cannot do:
distinguishing "this model genuinely has nothing incoming" from "this model's
rows never reached the comparison". From inside that function the two are
identical, and only the first justifies a zero.

The consequence is the worst-shaped one available. Of 32 models the page showed
two affirmative claims, and at least one of them was produced by the single
branch that never looked at NetBox. An operator reading the page sees green
where there is no evidence at all - strictly worse than `Not measured`, which
is at least true.

This is the same defect the 2.8.2 tranche was about: a check reporting a
confident answer about something it never measured. It was fixed in the drift
report, in the tree detector and in the release command, and it was still
sitting here.

## Approach

Delete the shortcut and let the dispatcher answer for an empty row list exactly
as it does for a populated one. It already returns the right thing in both
cases: a zeros mapping for a model it can compare (the `not normalized_rows`
branch, which costs no queries) and `None` for one it cannot.

One code path produces the answer instead of two, and the answer stops
depending on a property - emptiness - that was never evidence of agreement.

## Touched Surfaces

- `forward_netbox/utilities/drift_comparison.py` - the shortcut, removed.
- `forward_netbox/tests/test_empty_comparison_is_not_a_measurement.py` - new.
- `.pre-commit-config.yaml` - the new test is docstring-first, which
  `reorder-python-imports` and `black` never agree on; same exclusion as its
  siblings.

Nothing in the dispatcher changes. It already answered correctly for both an
empty and a populated row list; it simply was not being asked.

## Constraints

- A comparable model with no rows must still report zero, not "not measured".
  Reporting no comparison there would lose coverage the drift report already
  has, trading one wrong answer for a different one.
- No new dispatch table. A list of "models the comparison supports" maintained
  here would be a second source of truth, free to drift from the dispatcher it
  describes - which is how this bug reads in the first place.

## Validation

`forward_netbox/tests/test_empty_comparison_is_not_a_measurement.py` pins both
halves: an adapter-only model reports `None` whether it is handed rows or not,
and a spec model and a bespoke model with no rows still report zero.

Full Django suite, because every bespoke preview path is now reachable with an
empty row list where it previously was not.

## Rollback

Revert. The report returns to showing green for uncompared models.

## Decision Log

- **Route empty through the dispatcher rather than gate the shortcut behind a
  supported-model check.** The gate needs a list of supported models; the
  dispatcher *is* that list. Asking it is both shorter and incapable of
  disagreeing with itself.
- **Fix this inside the 2.8.3 recovery.** The release has to be re-gated and
  re-authorized regardless, so the marginal cost is one commit, and the report
  that motivated it is open.

## Open

- Eight models with working preview paths - `dcim.site`, `dcim.devicetype`,
  `dcim.manufacturer`, `dcim.interface`, `dcim.macaddress`, `ipam.vlan`,
  `ipam.vrf`, `ipam.prefix` - reported `Not measured` on that deployment, each
  with change candidates exactly equal to its Forward row count, which is the
  upper-bound fallback. They should have been measured. This change does not
  address that and no hypothesis for it has been tested; the discriminating
  evidence is the `comparison_coverage` object in the preview payload, which
  reports the preview's own view of measured and total models.
