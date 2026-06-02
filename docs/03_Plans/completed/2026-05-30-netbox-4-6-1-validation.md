# NetBox 4.6.1 Validation

## Goal

Update the local and CI NetBox 4.6 validation target from `v4.6.0` to `v4.6.1` while keeping the shared `4.5.x`/`4.6.x` compatibility branch intact.

## Constraints

- Keep `v4.5.9` in the CI matrix for 4.5 compatibility.
- Keep 4.6 behavior capability-gated on the shared branch.
- Do not change package version metadata in this validation-only update.

## Touched Surfaces

- Set the default local development NetBox image tag to `v4.6.1`.
- Update the GitHub Actions NetBox matrix from `v4.6.0` to `v4.6.1`.
- Update current compatibility docs to reference `4.6.1` after validation.

## Approach

1. Update the local development `NETBOX_VER` default.
2. Update the GitHub Actions NetBox matrix.
3. Update current compatibility docs after the local 4.6.1 runtime boots and passes checks.
4. Keep historical release-plan notes unchanged because they record the version validated at that time.

## Validation

- `invoke harness-check` passed.
- `invoke harness-test` passed (`99` tests).
- `invoke docs` passed.
- `NETBOX_VER=v4.6.1 invoke build` passed.
- `NETBOX_VER=v4.6.1 invoke start` passed.
- `NETBOX_VER=v4.6.1 invoke check` passed.
- Runtime version proof: `settings.VERSION` reported `4.6.1-Docker-5.0.1`.

## Rollback

Restore the local default and CI matrix from `v4.6.1` to `v4.6.0`, and revert the current compatibility doc wording.

## Decision Log

- Kept `v4.5.9` in CI instead of moving entirely to `v4.6.1`, because the plugin still documents shared `4.5.x` and `4.6.x` support.
- Updated only current compatibility docs, not historical plan evidence, because older plan files are audit records.
