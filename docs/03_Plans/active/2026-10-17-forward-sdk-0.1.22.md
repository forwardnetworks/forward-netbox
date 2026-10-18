# Accept forward-sdk 0.1.22

## Goal

Move the pinned `forward-sdk` from 0.1.20 to 0.1.22. The 0.1.21 and 0.1.22 releases add on-demand collection (`snapshots.collect()`), org-property listing, a create retry after Forward's rate-limit 429, a refusal of `.` and `..` path parameters, and a fix to how the NQE schema model reads a type's `deprecated`. The plugin uses none of the additions yet; this keeps it current and unblocks them.

## Constraints

- Exact pin, as before: `pyproject.toml`, `constraints.txt`, `poetry.lock` and the SBOM check all name 0.1.22.
- No behaviour change to what the plugin sends to Forward: every call the plugin makes (`networks`, `snapshots`, `nqe`, `checks`, `change_sets`, `ai`) is covered by the full suite.
- Supersedes Dependabot PR 567 (0.1.21 only), which left `constraints.txt` and the SBOM check on 0.1.20.
- No customer identifiers in the repo.

## Touched Surfaces

- `pyproject.toml`, `poetry.lock`, `constraints.txt`, `scripts/validate_sbom.py`.
- Nothing under `forward_netbox/`.

## Approach

1. Change the four pin sites; regenerate the lock with `poetry lock` (the keyring backend must be disabled on this host or poetry hangs).
2. Run the full suite on the tree, which builds the test image from `constraints.txt` and so installs 0.1.22.
3. Merge, and close Dependabot PR 567 as superseded. It rides the next release.

## Validation

- The lockfile diff touches only `forward-sdk` (version, two hashes) and the content hash.
- The full test suite and the pre-push CI gate on the tree with 0.1.22 installed.
- The SBOM check names 0.1.22.

## Rollback

Revert the commit; the lock and constraints return to 0.1.20.

## Decision Log

- Bumped straight to 0.1.22 rather than taking Dependabot's 0.1.21, since 0.1.22 is current and its fixes (the 429 create retry, the path-parameter refusal) are ones the plugin benefits from.
- Not used yet: `snapshots.collect()` (would let a change trigger a collection in its workspace instead of waiting for one) and `configuration.get_org_config_values()` (would read `ai_allowed` directly). Both are follow-ups.
