# Bulk Prefix Apply: Stop Clobbering Existing VRFs

## Goal

Fix a latent data-corruption bug in the experimental `ipam.prefix` bulk-ORM
apply path: while ensuring referenced VRFs exist, it upserted synthetic VRF rows
(`rd=None`, `description=""`, `enforce_unique=False`), overwriting an existing
VRF's real `rd` / `description` / `enforce_unique` set by the `ipam.vrf` map.

## Constraints

- Never modify an existing VRF while resolving prefix foreign keys.
- Still create VRFs that are referenced by a prefix row but do not yet exist
  (keeps the bulk path usable when the ipam.vrf map has not run).
- No change to prefix row handling or to the `ipam.vrf` map's own apply path.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` — prefix VRF-ensure block in
  `bulk_orm_apply_simple_models` resolves existing VRF names and creates only
  the missing ones.
- `forward_netbox/tests/test_apply_engine.py` — regression test asserting an
  existing VRF's fields survive and a missing VRF is created on demand.

## Approach

Collect the requested VRF names from the prefix rows, query which already exist,
and build create-only rows for `requested - existing`. Apply those (create
path), then build the `vrf_by_name` map from all requested names (existing +
newly created) for FK binding. Existing VRFs are never passed to the apply
upsert, so their fields cannot be rewritten.

## Validation

- `forward_netbox.tests.test_apply_engine`
  (`test_bulk_prefix_vrf_ensure_does_not_clobber_existing_vrf`).
- Full `forward_netbox.tests` suite.
- `invoke harness-check`, lint.

## Rollback

Revert the VRF-ensure block to the prior upsert and delete the regression test.
No schema, data, or migration impact.

## Decision Log

- Create-only-missing chosen over "resolve existing, raise on missing" (the
  adapter's behavior): it avoids the clobber while staying more lenient than the
  adapter, which suits the experimental bulk path. The adapter path is
  unchanged.
