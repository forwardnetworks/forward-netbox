# Upgrade a 2.9.x database onto 3.x

## Goal

Every push to `main` now fails its artifact upgrade test. The gate upgrades
from the newest release on PyPI below the branch's version, which for 3.0.0
is now 2.9.9. The two lanes share migrations only through `0052`; `maint/2.9.x`
then added `0053`-`0057` under names `main` does not have, and `0057` adds
`ForwardNQEMap.last_live_drift`/`_at`. A 2.9.9 database upgraded onto `main`
keeps those NOT NULL columns under a model without them, and the first NQE map
insert fails. This is exactly the path a customer on NetBox 4.6 takes to reach
4.7. Make `main` recognise the 2.9.x history.

## Constraints

- Three starting points must all migrate cleanly:
  - a 2.9.x database (2.9.x names applied, `main`'s `0053`-`0059` not);
  - a 3.0.0 database (the reverse);
  - an empty one.
- No schema change may be applied twice. The four 2.9.x field alterations
  are already made on this lane by `main`'s own `0056`-`0059`.
- `makemigrations --check` stays clean; the model gains exactly the two
  fields.

## Touched Surfaces

- `forward_netbox/migrations/`:
  - `0053_routing_policy_nqe_map_choices`,
    `0054_aci_tenant_policy_nqe_map_choices`,
    `0055_operator_delete_releases_ownership` and
    `0056_aci_attachment_nqe_map_choices` — the 2.9.x names and
    dependencies, no operations;
  - `0057_nqe_map_last_live_drift`, the real AddFields;
  - `0060_merge_maint_2_9_x_history`.
- `forward_netbox/models.py`: `ForwardNQEMap.last_live_drift` and
  `last_live_drift_at`.
- Tests: `forward_netbox/tests/test_migration_lineage.py`.

## Approach

The 2.9.x chain sits beside `main`'s as a second branch from `0052`, under its
own names, and a merge migration joins the two leaves. What each starting
point runs:

- **2.9.x database:** its own names are applied and skipped. `main`'s
  `0053`-`0059` run: the owner field, the change-control tables, and field
  alterations that are no-ops at the database level.
- **3.0.0 database:** the four empty 2.9.x-named files run as no-ops, and
  `0057` adds the two columns.
- **Empty database:** both branches run.

After the merge, every database has the same schema, and the model matches
it.

## Validation

- `test_migration_lineage.py`:
  - every 2.9.x name exists with its original dependency;
  - one leaf, with both chains among its ancestors;
  - only `0057` changes anything.
- The gate's own artifact upgrade test: 2.9.9 on NetBox 4.6.5 → this branch
  on 4.7.
- The same test run by hand with `FORWARD_NETBOX_UPGRADE_FROM_VERSION=3.0.0`.
- Full `invoke ci`, including the "No changes detected" migration check.

## Rollback

Reverting removes the 2.9.x names. A database that already ran the merge then
has unknown applied migrations, which Django tolerates, but the model loses
two columns the database still has. Roll forward instead.

## Decision Log

- **Adopt the other lane's names rather than renumber or squash.** A
  database's history is the set of names it has applied; only files carrying
  those exact names make it recognisable. Renumbering 2.9.8's migration to
  `0060`, as the port first did, would have added the columns a 2.9.x
  database already has.
- **Empty operations for the four alterations.** Replaying them would be
  harmless on a 3.0.0 database, but the migration state would then hold two
  competing definitions of the same field until the merge, and which one wins
  depends on graph order.
- **Future 2.9.x migrations.** Each needs a file of the same name here and a
  merge after it. `test_migration_lineage.py` lists the lane's history so the
  next one is added deliberately.
