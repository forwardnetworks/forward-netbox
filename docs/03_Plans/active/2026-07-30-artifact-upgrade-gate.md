# Artifact Upgrade Gate

## Goal

Test the upgrade path, not just the clean install.

Named a GA blocker in the 2026-07-04 enterprise-readiness review and deferred
ever since. `artifact-test` installs the built wheel into an **empty** database.
That is not how any existing deployment receives a release: a migration that
drops a column, a default that never backfills, or a field whose meaning changed
all pass a clean install and break a real upgrade.

## Contract

- The release workflow installs the previous released version, writes plugin
  rows under it, then migrates to the built wheel over the same database and
  reads those rows back.
- The upgraded run proves it is the built wheel, that `migrate` succeeds on a
  populated database, that `makemigrations --check` is clean afterwards, and
  that every plugin menu route still returns 200.
- The fixture contains only synthetic constants defined in the script. No
  customer data.
- The gate runs in its own Compose project, so it cannot collide with
  `artifact-test`'s database volume or with a local stack.

## Constraints

- Do not weaken the tag-only publish trigger or Trusted Publishing scoping.
- Do not change what `artifact-test` does; the clean-install gate stays.

## Touched Surfaces

- `tasks.py` — `artifact-upgrade-test`, plus `_build_artifact_image`,
  `_artifact_run_command`, `_SERVICE_WAIT_SCRIPT` and
  `_previous_released_version` extracted from `artifact_test`.
- `scripts/validate_upgrade_state.py`
- `scripts/tests/test_tasks.py`
- `.github/workflows/release.yml`
- This plan.

## Approach

1. `_previous_released_version` reads git tags and returns the highest release
   strictly below the version being built, compared numerically. A tag is what
   was actually published and therefore what an operator upgrades from.
   `--from-version` overrides it; no tag below the build fails closed.
2. Build two images from the same Dockerfile. `PACKAGE` feeds `uv pip install`
   directly, so the old image installs `forward-netbox==<previous>` from PyPI
   and the new one installs the built wheel.
3. Bring up `postgres` and `redis` in an isolated Compose project. Neither
   publishes a host port, so this cannot collide with a local stack.
4. Old image: `migrate`, then `validate_upgrade_state.py --mode seed`.
5. New image: prove the wheel identity, `migrate` on the populated database,
   `check`, `makemigrations --check`, `--mode verify`, then the route validator.
6. Always tear down the volumes and both images.

The fixture chain — source → sync → ingestion → issue, plus an NQE map and a
drift policy — is plugin-owned, so it cannot fail for reasons unrelated to the
upgrade. `ForwardNQEMap.netbox_model` is a ContentType FK, so the set also
covers a cross-app relation rather than only plugin-local columns.

## Validation

- Ran end to end: 2.6.5 installed from PyPI, seeded, upgraded to the built
  2.6.6 wheel, `verified: true`, and all seven plugin menu routes returned 200.
- `ArtifactUpgradeTaskTest` — 9 tests covering build order, seed-then-verify,
  project isolation, explicit `--from-version`, upgrading a version from itself,
  the wrong NetBox version, numeric-not-lexical tag ordering, and the no-tag
  failure.
- `scripts/tests`: 247 tests, OK — including the pre-existing
  `test_artifact_test_uses_wheel_without_source_fallback`, which pins that the
  refactor left `artifact-test` byte-for-byte equivalent.

## Rollback

Revert the commit through the normal protected pull-request path. The change is
additive: one new task, one new script, one new workflow step. `artifact-test`
is unchanged in behaviour, so reverting cannot affect the clean-install gate.

## Decision Log

- 2026-07-30: Wired into `release.yml` rather than `ci.yml`. It is an upgrade
  gate for a *release*, and `ci.yml` already runs 34–44 minutes against a
  timeout raised only because it was too tight.
- 2026-07-30: Seeded plugin-owned models rather than NetBox core objects. A
  fixture that needs a site, manufacturer, device type and role can fail for
  reasons that have nothing to do with the upgrade, which would make the gate
  noisy and get it ignored.
- 2026-07-30: Extracted the image build and run-command helpers instead of
  copying ~40 lines. The existing `artifact-test` test asserts the full command
  contents, so an accidental divergence fails immediately.

## Evidence

- Two defects were found by *running* the gate, not by review: the seed script
  called `django.setup()` without `DJANGO_SETTINGS_MODULE`, and
  `ForwardNQEMap.netbox_model` is a ContentType FK rather than the string the
  fixture first assigned. Both passed the mocked unit tests.
- Final run: `artifact-upgrade-test passed: 2.6.5 -> 2.6.6 migrated and the rows
  seeded under the previous release survived.`
