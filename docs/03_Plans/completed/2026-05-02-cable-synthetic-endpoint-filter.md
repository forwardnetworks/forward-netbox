## Goal

Prevent `dcim.cable` ingestion failures caused by Forward links whose remote endpoint is not a real snapshot device (for example synthetic nodes).

## Constraints

- Keep strict, exact endpoint matching for real NetBox device/interface pairs.
- Do not change release/version metadata in this patch.
- Keep behavior deterministic across diff, full sync, and multi-branch planning paths.

## Touched Surfaces

- `forward_netbox/utilities/sync.py`
- `forward_netbox/queries/forward_inferred_interface_cables.nqe`
- `forward_netbox/tests/test_sync.py`
- `forward_netbox/tests/test_query_registry.py`

## Approach

1. Update `_apply_model_rows()` to interpret adapter return `False` as an explicit non-fatal skip outcome.
2. Update `_apply_dcim_cable()` to skip (warn + `False`) when device or interface endpoints are unresolved and not dependency-failed.
3. Add an NQE query-level guard for cables: only emit links whose remote `link.deviceName` is in the completed non-custom snapshot device set.
4. Add tests for skip semantics and query-shape assertions.

## Rollback

Revert this patch set to restore prior behavior where unresolved cable endpoints hard-fail the ingestion stage.

## Decision Log

- Chosen: combine query-side and adapter-side guards so synthetic endpoints are filtered early and safely ignored if they still appear.
- Rejected: query-only fix; adapter still needs runtime protection for stale/inconsistent rows.
- Rejected: adapter-only fix; query should avoid emitting known-noise rows in the first place.

## Validation

- `pre-commit run --all-files`
- `python manage.py test --keepdb --noinput forward_netbox.tests.test_sync` (container)
- `python manage.py test --keepdb --noinput forward_netbox.tests.test_query_registry` (container)
