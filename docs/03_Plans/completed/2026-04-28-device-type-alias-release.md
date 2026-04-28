# Device Type Alias Release

## Goal

Publish the optional Device Type Library alias-map support as `0.3.1`.

## Constraints

- Keep default non-data-file maps available and enabled by default.
- Keep alias-aware maps disabled until an operator uploads, attaches, and
  materializes the Forward data file into the selected snapshot.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or
  generated data-file exports.
- Keep release metadata consistent across package metadata, plugin version,
  user-agent, and install documentation.

## Touched Surfaces

- `pyproject.toml`
- `forward_netbox/__init__.py`
- `forward_netbox/utilities/forward_api.py`
- `README.md`
- `docs/README.md`
- `docs/01_User_Guide/README.md`

## Approach

- Bump the package, plugin, and Forward API user-agent version to `0.3.1`.
- Add `0.3.1` release notes while preserving `0.3.0.1` as the superseded patch
  release.
- Build fresh wheel and source artifacts from the tagged commit.
- Publish GitHub Release and PyPI artifacts only after local gates and GitHub CI
  pass.

## Validation

- `invoke harness-check`
- `invoke harness-test`
- `invoke lint`
- `invoke check`
- `invoke test`
- `invoke scenario-test`
- `invoke docs`
- `invoke ci`
- `invoke package`
- Sensitive-content scan and release artifact verification.

## Rollback

- If publication fails before upload, revert the release metadata commit.
- If publication succeeds but a defect is found, supersede with a newer release
  because PyPI files and GitHub tags should not be reused.

## Decision Log

- Use `0.3.1` instead of republishing `0.3.0.1` because PyPI does not allow
  replacing existing distribution files.
- Keep data-file alias support optional because public `/api/nqe` cannot force
  Forward latest-data-file execution for plugin syncs.
