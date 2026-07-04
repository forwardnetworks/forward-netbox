# Upgrade and Rollback

This plugin pins hard minimum NetBox and `netbox-branching` versions per release
(see the compatibility matrix in the top-level README). Always confirm the target
plugin version supports your NetBox version before upgrading.

## Before you upgrade

1. **Back up the NetBox database.** The plugin's sync engine performs
   inventory-wide writes, and an upgrade applies schema migrations. A restorable
   database backup is the supported rollback path (see [Rollback](#rollback)).
2. **Quiesce syncs.** Let in-flight Forward syncs finish and avoid starting new
   ones during the upgrade (check the Sync and Ingestion pages, and the RQ queues).
3. **Read the release notes** for the target version (the README compatibility
   table and `CHANGELOG.md`) for any migration or configuration notes.

## Upgrade

Run these in the NetBox Python environment (and inside every worker container in a
containerized deployment):

```bash
# 1. Install the target version.
pip install --upgrade forward-netbox            # latest
# or pin explicitly:
# pip install forward-netbox==<version>

# 2. Apply database migrations.
cd /opt/netbox/netbox && python manage.py migrate

# 3. Restart NetBox web and worker processes so the new code and any changed
#    background jobs are loaded.
```

After restart:

- Confirm the plugin version: `pip show forward-netbox`.
- Confirm NetBox starts cleanly (`python manage.py check`) with no
  `netbox_branching` dependency warnings in the log.
- Run one sync against a known source and confirm it completes and the Sync
  health panel is green.

## Rollback

If an upgrade misbehaves, roll back to the previously installed version.

> **Important:** Some releases include schema migrations, and a few (for example
> the split IP-address maps and the removal of the retired execution-run models)
> are **not fully reversible**. The reliable rollback across a schema change is to
> **restore the database backup** taken before the upgrade, not to reverse
> migrations. Reverse-migration is safe only for releases with no migration or
> with reversible migrations.

Restore-from-backup rollback (recommended):

```bash
# 1. Stop NetBox web + workers.
# 2. Restore the database backup taken before the upgrade.
# 3. Reinstall the prior plugin version.
pip install forward-netbox==<previous-version>
# 4. Start NetBox web + workers.
```

Reverse-migration rollback (only when the intervening release added no migration,
or only reversible ones — check `forward_netbox/migrations/` for the delta):

```bash
cd /opt/netbox/netbox
python manage.py migrate forward_netbox <previous-migration-name>
pip install forward-netbox==<previous-version>
# then restart web + workers
```

The stored Forward source parameters are forward- and backward-compatible across
recent releases (unknown keys are accepted and ignored), so a downgrade does not
invalidate existing sources.
