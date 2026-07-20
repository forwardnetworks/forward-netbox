# Upgrade and Rollback

This plugin pins hard minimum NetBox and `netbox-branching` versions per release
(see the compatibility matrix in the top-level README). Always confirm the target
plugin version supports your NetBox version before upgrading.

## Before you upgrade

1. **Back up the NetBox database.** The plugin's sync engine performs
   inventory-wide writes, and an upgrade applies schema migrations. A restorable
   database backup is the supported rollback path (see [Rollback](#rollback)).
2. **Quiesce syncs and branches.** Let in-flight Forward syncs finish, then merge
   or discard every nonterminal Branching branch. Version 2.6 ownership tables
   exist only in the main schema; do not install or migrate while a review,
   ready, failed, merging, or pending-migrations branch remains.
3. **Verify the exact runtime target.** Version 2.6 requires NetBox 4.6.5 and
   `netboxlabs-netbox-branching` 1.1.1 on web and every worker.
4. **Read the release notes** for the target version (the README compatibility
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
- For `2.6.0`, upgrades from `2.5.11` need no query publication. Direct upgrades
  from `2.5.10` or earlier must run **Publish Bundled Queries** once with
  overwrite enabled to install the inherited `2.5.11` query fixes.
- The `2.6.0` data migration removes the legacy `auto_prune_orphans` sync
  parameter and registers existing plugin-managed tags. It deliberately invents
  no source claims and marks no pre-existing Virtual Device Context as owned.
  Orphan deletion is manual-only after upgrade.
- The same migration renames `max_changes_per_branch` to
  `max_changes_per_staging_item`, removes retired execution and automatic-prune
  controls, converts active validation/dependency-preview Job rows into
  canonical stored schedule intent, and disables the bundled no-op Virtual
  Chassis map. Customer-authored maps are unchanged.
- Existing sources with include tags and no explicit endpoint-scope decision
  are converted to `scope_endpoints_by_include_tags=true`. An explicit opt-out
  previously saved by the UI remains false. The transitional configured marker
  is removed.
- Ownerless syncs adopt the user from their latest attributable NetBox job when
  one exists. Before running an ownerless sync with no attributable history,
  edit it while signed in as the intended owner. Version 2.6 refuses execution
  without an invoking user or stored owner; it never falls back to an arbitrary
  superuser for ObjectChange attribution.
- Re-open each source after an endpoint-query upgrade. Keep **Import Generic
  SNMP Endpoints as Devices** off unless broad, sparse endpoint inventory is
  intentional; the normal endpoint toggle imports recognized console servers.
- Run **Preview Dependencies**, then run every relevant sync against a known
  snapshot so each one establishes generation-stamped scope/status/parent
  claims. When sources share a managed tag, run all of them before expecting a
  last-claim removal.
- Confirm each merge has zero failed changes. A partial merge remains open and
  retryable; it is not a baseline and does not enqueue ownership overlays.
- Wait for every required post-sync ownership job to complete. Pending, failed,
  stale, conflicting, or missing materialization evidence keeps Sync Health and
  Drift non-converged.
- Open the **Drift Report**. Dependency preview is a workload estimate and does
  not perform an object-level comparison. If the completed sync applied any
  changes, run the sync again against the same resolved snapshot. Accept
  convergence only when the report shows a merged, zero-change, zero-failure
  ingestion for that same snapshot.
- Review **Scope Reconciliation**, including the read-only post-upgrade catalog
  counts. Run **Reconcile device scope tags** before reviewing contradictory
  include/out-of-scope labels. Prune source-scoped device orphans only after
  review. Global DLM and DeviceType candidates remain manual-review items.
- Run the strict read-only control-plane gate:

  ```bash
  python manage.py forward_ownership_audit \
    --fail-on-inconsistent --require-no-open-branches
  ```

  Accept the upgrade only when `consistent` and `release_ready` are both true.
  A stale overlay reports a skip and requests catch-up against the newest
  completed ingestion.
- Module sync no longer requires an out-of-band bay import. Missing bays are
  created in the branch before their modules under the required 4.6.5/1.1.1
  matrix; Module Readiness remains an optional preflight.
- Export a support bundle. It carries aggregate post-upgrade DLM, CVE, Platform,
  and stale endpoint DeviceType counts without sampled inventory values.

## Rollback

If an upgrade misbehaves, roll back to the previously installed version.

> **Important:** Some releases include schema migrations, and a few (for example
> the split IP-address maps and the removal of the retired execution-run models)
> are **not fully reversible**. The reliable rollback across a schema change is to
> **restore the database backup** taken before the upgrade, not to reverse
> migrations. Reverse-migration is safe only for releases with no migration or
> with reversible migrations.

Version 2.6 adds the ownership control-plane schema and uses a one-way
initialization migration. Restoring the pre-upgrade database backup is the only
supported rollback from 2.6.

Restore-from-backup rollback:

```bash
# 1. Stop NetBox web + workers.
# 2. Restore the database backup taken before the upgrade.
# 3. Reinstall the prior plugin version.
pip install forward-netbox==<previous-version>
# 4. Start NetBox web + workers.
```

Reverse-migration rollback for releases that do not cross 2.6:

```bash
cd /opt/netbox/netbox
python manage.py migrate forward_netbox <previous-migration-name>
pip install forward-netbox==<previous-version>
# then restart web + workers
```

Version 2.6 validates source and sync parameter schemas strictly; unknown or
retired keys are rejected instead of ignored. Do not use parameter compatibility
as a rollback mechanism. A rollback across 2.6 requires the pre-upgrade database
backup described above.
