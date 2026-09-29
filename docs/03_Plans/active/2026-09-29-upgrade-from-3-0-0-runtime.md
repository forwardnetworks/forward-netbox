# Seed the 3.0.0 upgrade source on NetBox 4.7

## Goal

The artifact upgrade gate upgrades the previous release onto the current
one. For `3.0.1` the previous release is `3.0.0`, the first release on the
NetBox 4.7 line, which declares `min_version = "4.7.0"` and refuses to load on
the gate's default from-side runtime (NetBox 4.6.5). The gate failed at its
last leg for that reason alone. Give the from side of that upgrade the runtime
`3.0.0` was tested on.

## Constraints

- Which runtime a release can run on is a property of that release, so the
  entry is keyed by version in `UPGRADE_FROM_NETBOX_OVERRIDES`, next to the
  existing `2.8.0` entry. A blanket override would move the scenario suite's
  upgrade fixtures off 4.6.5 and drop the NetBox upgrade path they exercise.
- The from-side constraints (`development/constraints-upgrade-from.txt`) are
  unchanged and resolve for `3.0.0` as they stand.

## Touched Surfaces

- `tasks.py`: one entry in `UPGRADE_FROM_NETBOX_OVERRIDES`.
- `scripts/tests/test_tasks.py`: the override list and a test naming why
  `3.0.0` needs it.
- `scripts/check_harness.py`: `4.7.0` is recorded as a historical mention
  allowed in `tasks.py`, as `4.6.6` is for the `2.8.0` entry. It is the
  runtime `3.0.0` needs, not a stale tested-on pin.

## Approach

Add `"3.0.0": "v4.7.0"` with a comment stating the release's declared minimum.
`invoke artifact-upgrade-test` reads the override when the from version is
`3.0.0`, seeds there, and migrates onto the current runtime.

## Validation

- `scripts/tests/test_tasks.py` passes with the new entry and its test.
- `invoke artifact-upgrade-test --from-version 3.0.0` was run on a tree
  declaring `3.0.1`: it seeded `3.0.0` on NetBox 4.7.0, upgraded to `3.0.1` on
  NetBox 4.7.1, and the rows seeded under the previous release survived.
- Full `invoke ci` before merge.

## Rollback

Revert the commit. The gate then fails at the upgrade leg for any release
whose previous release is `3.0.0`.

## Decision Log

- **Its own change, not folded into the release commit.** The release commit
  carries no code, so the override lands first and the release rebases onto
  it.
- **`v4.7.0`, not `v4.7.1`.** That is the runtime `3.0.0` was tested on, so
  the from side is the release as it shipped and the upgrade also exercises a
  NetBox patch bump.
