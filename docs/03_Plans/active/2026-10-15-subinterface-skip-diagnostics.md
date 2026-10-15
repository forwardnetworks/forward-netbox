# Subinterface skip diagnostics

## Goal

A support bundle says why each subinterface row was skipped and what NetBox holds that decides it, so an operator's "50 interface skips" is diagnosed from the bundle alone. Four different conditions on a subinterface row all recorded "waiting on dcim.interface".

## Constraints

- Counts, pks and slugs only; no device, interface or VRF names in the bundle.
- No behavior change to which rows apply or skip. Diagnostics first; any adoption fix waits for the evidence this produces.
- No customer identifiers in the repo, tests or docs. Tests use synthetic names.
- No fix without a failing test first.

## Touched Surfaces

- `forward_netbox/exceptions.py`: `ForwardDataError` takes a `reason` slug.
- `forward_netbox/utilities/sync_reporting.py`: `dependency_skip_reason` prefers the raiser's reason; skips past the per-model detail cap are tallied by reason into the rollup issue.
- `forward_netbox/utilities/apply_engine_bulk.py`, `sync_interface.py`: the subinterface skips carry a reason and, when an existing row is involved, its pk.
- `forward_netbox/views.py`: `by_skip_reason` in the issue summary, `skip_reason` per issue, and an `interface_state` section.
- Tests: `forward_netbox/tests/test_subinterface_skip_reasons.py`.

## Approach

1. Reasons: `subinterface-parent-not-found`, `subinterface-name-taken-by-parentless-interface`, `subinterface-name-taken-by-top-level-row`, `subinterface-parent-not-saved`.
2. The rollup issue records `beyond_cap_reasons`, so rows that never got an issue of their own are still counted.
3. `interface_state`: dotted virtual interfaces with and without a parent (with a pk sample), dotted non-virtual interfaces by type, interfaces carrying many addresses across several VRFs (by pk, with whether the device is bound to a sync or marked absent), and IP addresses assigned to nothing.

## Validation

- `forward_netbox.tests.test_subinterface_skip_reasons`, `test_subinterface_apply`, `test_bundle_triage_diagnostics`, `test_skip_rollup_direction`.
- `invoke harness-check`, `invoke lint`, then `invoke test`.

## Rollback

Revert the commit. The bundle loses the new fields; nothing persisted changes shape.

## Decision Log

- Reused the existing `skip_reason` slug rather than a second field; the new slugs override the derived one only where a raiser names a reason.
- Not adopting parentless same-name interfaces yet: the cause of one customer's 50 skips is not proven, so the bundle reports it first.
