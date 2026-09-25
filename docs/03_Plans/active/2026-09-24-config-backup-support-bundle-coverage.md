# Config backup joins the support bundle's other problem classes

## Goal

A customer enabled config backup, ran it repeatedly, and every failure showed
only `"Forward config backup failed (ForwardSyncError)."` - no reason, no way
to self-diagnose. Finding the actual cause (an empty git repository with no
`branch` parameter set, so this plugin's own "do not guess a branch" refusal
fired) took two rounds of one-off diagnostic scripts run on the customer's
NetBox. Make the support bundle answer this without a script, matching the
coverage the bundle already has for scope reconciliation, query drift and
ingestion issues.

## Constraints

- Exports stay value-free: pks, booleans, timestamps and catalog-level facts
  only - never a data source name or a `device_config_path` template (which
  can embed customer-chosen text).
- The bundle makes no live calls (no git fetch, no Forward call); it reads
  only stored state, exactly like every other bundle section.
- A health-check message shown in the GUI may keep the data source name -
  that is the in-deployment tier, unaffected by this change.

## Touched Surfaces

- `forward_netbox/utilities/config_backup.py` - new `ConfigBackupError`
  subclass of `ForwardSyncError`, used at the module's six existing raise
  sites in place of the base class. No behavior change to what is raised or
  when.
- `forward_netbox/jobs.py` - `_run_forward_config_backup_work` gains an
  `except ConfigBackupError` branch, ahead of the generic `except Exception`,
  that preserves the exception's own message instead of collapsing it through
  `safe_operation_failure`.
- `forward_netbox/utilities/health.py` - `_config_backup_delivery_check` is
  split into a value-free `config_backup_delivery_state(sync)` (the shared
  facts) and the existing GUI check (built from it, message unchanged); a new
  `config_backup_delivery_bundle_payload(sync)` exports the value-free half.
- `forward_netbox/views.py` - `_sync_support_bundle_payload` gains a
  `config_backup_delivery` key.
- Tests: `forward_netbox/tests/test_config_backup_job_and_health.py`.

## Approach

1. **Stop discarding an already-safe message.** Every raise in
   `config_backup.py` is either a static sentence or interpolates only
   `_remote_failure_reason(exc)`, which is already built to redact URLs and
   credentials - there was never anything to hide behind
   `safe_operation_failure`'s generic classifier. `ConfigBackupError` marks
   these six sites so the job wrapper knows it is safe to keep `str(exc)`
   verbatim. Nothing outside `config_backup.py` raises it, so this changes no
   other failure path's message.
2. **One set of facts, two tiers.** `config_backup_delivery_state(sync)`
   computes what was previously inlined directly into the GUI check: whether
   the data source exists, whether its `branch` parameter is set, its
   `last_synced`, and (when Validity is installed) whether
   `device_config_path` is set and matches this plugin's prefix, and whether
   any tenant or a default data source binds to it. The GUI check formats
   this into the existing prose message (data source name included, as
   before - no behavior change there). The bundle payload emits the same
   facts as booleans/pks/timestamp only.
3. **Wire the payload into the bundle**, next to `diagnostics` and
   `operator_action_jobs` (whose `config_backup` entry already carries the
   last run's `pushed`/`data_source_synced`/`warnings` - this section answers
   the question one level up: is delivery even configured to succeed at all).

## Validation

- `test_config_backup_job_and_health.py`: the existing delivery-check tests
  are unchanged (same messages, same statuses) after the split; new tests
  cover the bundle payload for disabled/missing/unsynced/synced data sources,
  an unset vs. set `branch` parameter, Validity binding fields, and confirm
  the data source name never reaches the payload or survives
  `export_safe_payload`; a new job test confirms a `ConfigBackupError`'s own
  message reaches `job.data["error"]` verbatim while an unrelated exception
  still goes through the generic redacted path.
- `test_config_backup.py` (real local git pushes, unmodified): confirms the
  raise-site rename changed nothing about when or what is raised.
- `test_export_redaction.py`, `test_support_bundle_archive.py`: unaffected,
  run for regression.
- Full `invoke ci` before push.

## Rollback

Single change set touching no models and no migration; revert restores the
previous (generic) message and drops the bundle key.

## Decision Log

- **A typed exception subclass, not an attribute flag.** `ConfigBackupError`
  is caught by type in `jobs.py`, so only the six sites this module
  deliberately authored as safe are ever trusted - a future exception raised
  elsewhere and merely passing through this code path stays on the generic,
  redacted branch.
- **The delivery state is split from the check, not duplicated.** A second,
  independently-written implementation of "is Validity bound to this data
  source" was the more obvious shape, and the wrong one - it is exactly how
  two tiers of the same fact silently drift apart. One function computes the
  facts; each tier formats its own view of them.
- **Live connectivity checks (the actual git fetch, branch resolution) are
  NOT added to the passive bundle.** That would need a live call, which the
  bundle's whole design avoids. It remains a candidate for a separate,
  on-demand action (mirroring "Export Live Query Drift Check") in a future
  change - deferred here to keep this change bundle-only and call-free.
