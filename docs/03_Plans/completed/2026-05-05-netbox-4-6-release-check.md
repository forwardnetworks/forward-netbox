# NetBox 4.6 Release Check

## Goal

Update the NetBox 4.6 compatibility branch from the beta image target to the
released NetBox `v4.6.0` target while preserving the same optional-map
preflight guidance and sync feature surface as `main`.

## Constraints

- Keep NetBox 4.6 compatibility isolated from the `main` NetBox 4.5 release
  line.
- Preserve native NetBox 4.6 `dcim.cablebundle` behavior on the compatibility
  branch.
- Do not auto-enable optional built-in NQE maps during sync execution.
- Do not include customer-specific identifiers, snapshots, networks, or data.

## Touched Surfaces

- `development/.env`
- `forward_netbox/choices.py`
- `forward_netbox/utilities/query_fetch.py`
- `forward_netbox/utilities/query_registry.py`
- `forward_netbox/utilities/multi_branch.py`
- `forward_netbox/tests/test_sync.py`
- `docs/01_User_Guide/troubleshooting.md`

## Approach

Merge the current `main` release surface into the NetBox 4.6 branch, resolve the
4.6-specific cable bundle conflicts by keeping the branch execution backend, and
update the local development target to `v4.6.0`. Treat `dcim.cablebundle` as an
optional model alongside `dcim.module` so disabled built-in maps fail fast with a
clear operator action.

## Validation

- `python -m py_compile forward_netbox/utilities/multi_branch.py forward_netbox/tests/test_sync.py forward_netbox/utilities/query_fetch.py forward_netbox/utilities/query_registry.py`
- `python manage.py makemigrations forward_netbox --check --dry-run --verbosity 3`
- `git diff --check`
- focused 4.6 optional-map preflight tests
- 4.6 sync/model tests covering cable bundles and modules
- `invoke lint`
- `invoke harness-check`
- full branch CI gate before publishing this compatibility branch

## Rollback

Revert the branch merge and this plan, then restore `development/.env` to the
previous NetBox beta image target if the released 4.6 image exposes an
incompatibility.

## Decision Log

- Chosen: keep cable bundles and modules as optional map-backed models so the
  operator explicitly enables beta or version-specific surfaces.
- Rejected: auto-enable optional maps when the model is selected because that
  makes version-specific imports implicit and harder to diagnose.
