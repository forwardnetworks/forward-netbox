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
# 1. Install the target version. Keep the [integrations] extra if you run the
#    optional plugin maps (netbox-dlm / netbox-routing / netbox-cisco-aci /
#    netbox-peering-manager); plain forward-netbox is the core edition.
pip install --upgrade forward-netbox                 # core
pip install --upgrade "forward-netbox[integrations]" # keep optional plugin maps
# or pin explicitly:
# pip install "forward-netbox[integrations]==<version>"

# 2. Apply database migrations.
cd /opt/netbox/netbox && python manage.py migrate

# 3. Restart NetBox web and worker processes so the new code and any changed
#    background jobs are loaded.
```

After restart:

- Confirm the plugin version: `pip show forward-netbox`.
- Confirm NetBox starts cleanly (`python manage.py check`) with no
  `netbox_branching` dependency warnings in the log.
- When release notes require bundled NQE changes, publish the bundled queries
  with overwrite enabled before validation.
- Re-open each source after an endpoint-query upgrade. Keep **Import Generic
  SNMP Endpoints as Devices** off unless broad, sparse endpoint inventory is
  intentional; the normal endpoint toggle imports recognized console servers.
- Run **Preview Dependencies**, then one sync against a known source and confirm
  it completes and the Sync health panel is green.
- Open the **Drift Report**. Dependency preview is a workload estimate and does
  not perform an object-level comparison. If the completed sync applied any
  changes, run the sync again against the same resolved snapshot. Accept
  convergence only when the report shows a merged, zero-change, zero-failure
  ingestion for that same snapshot.
- Review **Scope Reconciliation**, including the read-only post-upgrade catalog
  counts. Run **Reconcile device scope tags** before reviewing contradictory
  include/out-of-scope labels. Prune source-scoped device orphans only after
  review. Global DLM and DeviceType candidates remain manual-review items.
- Export a support bundle. It carries aggregate post-upgrade DLM, CVE, Platform,
  and legacy endpoint DeviceType counts without sampled inventory values.

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
