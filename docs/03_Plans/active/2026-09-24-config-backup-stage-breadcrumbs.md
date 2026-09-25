# Config backup: stage breadcrumbs, best effort

## Goal

A customer's config-backup job died in under a second, twice, with only the
generic message this session's earlier fix already stopped collapsing to a
bare exception name (PR #463). The RECOVERED message named the actual
problem (an unset `branch` parameter on an empty repo) - but that was a lucky
case: `run_config_backup` has six distinct raise sites, and an instant
failure on a future customer's estate could be any of them (a bad data
source reference, an unreachable remote, an unresolvable branch, an empty
Forward fetch, or a push rejection) with no way to tell which one without
re-running a diagnostic script by hand, as this session's live incident
needed twice. Name the pipeline stage a failure happened in, in the message
itself, so the job's own `error` field answers that question.

## Constraints

- Best effort for this item, per the release plan: the higher-risk pieces
  (an on-demand live connectivity-probe button with a new persisted field
  and migration, a live check added to the sync-start path) are deferred.
  This is the safe, zero-risk slice - a string prefix on an already-safe
  message, no new query, no new write path, no schema change.
- Every message these six raise sites produce is already operator-safe (the
  `ConfigBackupError` docstring's own guarantee, established by PR #463);
  this item does not change what information is disclosed, only where in
  the pipeline it says the failure happened.
- Backward compatible: `ConfigBackupError(message)` with no `stage` produces
  the exact same `str()` as before - existing callers (tests, and any code
  constructing one without knowing about stages) are unaffected.

## Touched Surfaces

- `forward_netbox/utilities/config_backup.py`: `ConfigBackupError.__init__`
  gains `stage=`; `CONFIG_BACKUP_STAGES` constant; all six raise sites
  tagged.
- Tests: `forward_netbox/tests/test_config_backup.py` (extended).

## Approach

1. **`ConfigBackupError(message, *, stage=None)`** stores `self.stage` and
   prepends `f"[{stage}] "` to the message passed to the base exception -
   `str(exc)` (what `_run_forward_config_backup_work` preserves verbatim
   into `job.data["error"]`, per PR #463) then names both the stage and the
   reason in one string, with no other code path needing to change.
2. **`CONFIG_BACKUP_STAGES`** is the closed set - `resolve`, `fetch_remote`,
   `branch`, `nqe_fetch`, `build`, `push`, `datasource_sync` - one entry per
   phase of `run_config_backup`. `build` and `datasource_sync` currently
   have no raise site (the tree-building phase does not fail on its own,
   and the trailing `data_source.sync()` call is caught and downgraded to a
   warning by design, not raised); they are named anyway so the set matches
   the pipeline's actual shape rather than only its current failure modes,
   and a future raise site there has an obvious stage to reach for.
3. **All six raise sites tagged**: `resolve` (the two
   `config_backup_data_source` checks), `fetch_remote`, `branch`,
   `nqe_fetch` (the empty-result refusal), `push`.

## Validation

- `test_config_backup.py`: a stage is prepended to the message; no stage
  leaves it unprefixed; every `stage=` literal in the module is one of the
  six declared stages (a source scan, so a typo can never silently produce
  an unrecognized stage name); the empty-fetch refusal names `nqe_fetch`;
  the non-git data source refusal names `resolve`; an unreachable remote
  names `fetch_remote`; an unresolvable branch names `branch`; a failed
  push names `push` (and confirms the stage addition does not affect
  `_remote_failure_reason`'s existing URL redaction).
- Regression: `test_config_backup_job_and_health.py` (the existing
  backward-compatibility pin - a `ConfigBackupError` built with no stage
  keeps its exact message) - unmodified, still passes.
- `test_export_redaction`, `test_button_jobs`: unaffected, run for
  regression. 65 + 31 tests total, green.
- Full `invoke ci` before push.

## Rollback

Single change set, no migration, no model change. Revert restores the
unprefixed messages.

## Decision Log

- **A message prefix, not a separate `stage` field threaded through
  `job.data`.** The job wrapper already preserves `str(exc)` verbatim for
  this exception type; a prefix reaches the operator through the exact
  channel that already worked for the "cannot choose a branch" message,
  with no new field for the support bundle's export filter or the GUI
  template to learn about.
- **`build` and `datasource_sync` are declared with no current raise
  site.** Leaving them out of the constant would make the parity test
  (every `stage=` literal is a member) pass by omission rather than by
  design - the set is meant to describe the pipeline, not just today's
  exception coverage.
