# Beta 2 Formatting Release

## Goal

Publish a corrected `0.5.0b2` beta after GitHub Actions found formatting drift in the first beta commit.

## Constraints

- Keep the functional `0.5.0` beta scope unchanged.
- Do not rewrite PyPI artifacts after publication; use a new beta version.
- Keep committed files free of customer identifiers, credentials, network IDs, and snapshot IDs.

## Touched Surfaces

- `pyproject.toml`
- `forward_netbox/__init__.py`
- release documentation in `README.md`, `docs/README.md`, and `docs/01_User_Guide/README.md`
- formatter-normalized Python files reported by GitHub Actions

## Approach

1. Apply the same pre-commit formatting GitHub Actions requested.
2. Bump the beta version from `0.5.0b1` to `0.5.0b2`.
3. Rerun the full local CI-equivalent gate.
4. Commit, tag, publish the corrected prerelease, and verify GitHub Actions.

## Validation

- `pre-commit run --all-files` passed before this plan was added.
- Full local CI gate must pass before publishing `0.5.0b2`.
- GitHub Actions must pass for the pushed correction commit.

## Rollback

Revert the beta 2 correction commit and leave the previously published beta artifacts as superseded prerelease artifacts. No database state rollback is required.

## Decision Log

- Rejected: force-moving the already-published beta tag.
  - Reason: PyPI artifacts are immutable, so a new beta version is clearer and auditable.
