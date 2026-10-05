# Move the 4.6 lane's optional plugins to their latest releases

## Goal

Accept the newest releases of three optional plugins on `maint/2.9.x`:
`netbox-peering-manager` 0.3.0 -> 0.3.1, `netbox-validity` 3.5.2 -> 3.6.0 and
`netbox-routing` 0.4.3 -> 0.5.0. NetBox 4.6.10, `netbox-branching` 1.1.3,
`netbox-dlm` 0.10.0 and `netbox-cisco-aci` 0.4.0 are already the latest
releases this lane can take, so they do not move.

## Constraints

- Every one of the three declares a NetBox range that includes 4.6
  (`min_version` 4.5.0; peering-manager and validity cap at 4.7.99, routing
  has no cap), so none requires leaving the 4.6 lane.
- Both constraint files must stay in lockstep; only the branching pin may
  differ between them.
- `poetry lock` alone does not move an already-satisfied pin; the lock is
  moved with `poetry update --lock <package>`.
- No plugin behavior change: this is a pin move. The gate decides whether
  `netbox-routing` 0.5.0 behaves differently from 0.4.3 under the routing
  adapters.

## Touched Surfaces

- `pyproject.toml`, `constraints.txt`, `development/constraints-upgrade-from.txt`,
  `poetry.lock`
- `forward_netbox/utilities/plugin_integrations/registry.py` (both version
  fields where a plugin has two), `forward_netbox/utilities/validated_runtime.py`
- `scripts/validate_sbom.py`, `tasks.py` (the SBOM runtime-pyproject snippet)
- `docs/01_User_Guide/configuration.md` (install example)
- tests that name the accepted versions: `test_optional_plugin_versions.py`,
  `test_plugin_integrations.py`, `test_sync.py`

## Approach

Follow the optional-plugin version-bump checklist: change every site that names
an accepted version, then run the harness and the version tests, which name any
site that was missed.

## Validation

- `python scripts/check_harness.py` passes.
- Full `invoke ci` pre-push gate on NetBox 4.6.10 with the new plugin set,
  including the routing, peering and config-backup (validity) suites.

## Rollback

Revert this branch. No migration and no persisted state.

## Decision Log

- **Bumped all three in one change** because each one's compatibility range was
  verified against the 4.6 lane first, and a single gate run covers them.
- **`netbox-routing` takes 0.5.0 on this lane** because it declares no NetBox
  ceiling and the 3.x lane already runs the same adapters on it; a gate failure
  is the signal to hold it back, not a reason to skip the check.
