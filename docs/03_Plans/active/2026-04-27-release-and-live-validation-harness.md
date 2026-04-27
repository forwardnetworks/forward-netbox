# Release And Live Validation Harness

## Goal

Keep the release and live-validation workflow reproducible enough that local checks, GitHub CI, GitHub Releases, and PyPI artifacts all prove the same tree.

## Constraints

- Keep local and CI command sets aligned.
- Keep release artifacts tied to the exact published commit and tag.
- Keep PyPI as the primary package distribution path while preserving GitHub Releases as an aligned artifact mirror.
- Keep live Forward validation outside normal CI because it depends on real tenant access.
- Run sensitive-content checks before public commits, tags, or releases.

## Touched Surfaces

- `.github/workflows/`
- `tasks.py`
- `scripts/`
- `pyproject.toml`
- `docs/00_Project_Knowledge/release-playbook.md`
- `docs/00_Project_Knowledge/validation-matrix.md`
- `docs/01_User_Guide/troubleshooting.md`

## Approach

Preserve the current local command surface:

- `invoke lint`
- `invoke check`
- `invoke scenario-test`
- `invoke test`
- `invoke playwright-test`
- `invoke docs`
- `invoke package`
- `invoke ci`

Keep `invoke ci` as the release gate and make it include harness, scenario, sensitive-content, docs, package, and UI checks as those tasks mature.

Add release automation only when it reduces manual release surgery without hiding evidence. The ideal end state is one repeatable script or GitHub Action that builds wheel/sdist artifacts, updates checksums, publishes or refreshes the GitHub release, and verifies that `main`, the release tag, and published assets match.

Keep a lightweight live validation path for real Forward access. The live path should prove that the latest code resolves a source, network, and snapshot; executes built-in queries; and reaches at least `ready_to_merge` for the branch-backed flow. The output should be operator-readable and should not write tenant-specific IDs into committed files.

## Validation

- `invoke harness-check`
- `invoke harness-test`
- `invoke sensitive-check`
- `invoke lint`
- `invoke check`
- `invoke scenario-test`
- `invoke test`
- `invoke playwright-test`
- `invoke docs`
- `invoke package`
- `invoke ci`
- GitHub CI success on `main` and release tag before publishing a public release.
- Manual live smoke evidence kept out of committed files.

## Rollback

Revert release automation scripts or workflow changes, restore manual release-playbook steps, and keep the current `invoke ci` path as the minimum release gate.

## Decision Log

- Rejected live Forward validation in CI because it would require tenant credentials and customer-like state in a public automation path.
- Rejected release publication without a full local `invoke ci` gate because previous failures came from gaps between local checks and GitHub CI.
- Rejected unchecked GitHub release asset replacement because artifacts must remain aligned with the exact tag.
