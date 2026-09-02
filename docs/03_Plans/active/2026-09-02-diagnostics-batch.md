# Diagnostics batch: what a customer has had to be asked for, answered in place

## Goal

Close eight small open items in one reviewed change: the full-list CLI, the
quarantine cadence in Health, the RQ registry regression test, the ChangeDiff
corruption audit, the platform-name producer/consumer mismatch, the APIC CIMC
lifecycle map, the opt-in unmanaged config-backup prefix, and a dead code arm.

## Why

Each was recorded as open in a shipped plan and each is small enough that its
own pull request would be mostly ceremony. Together they are one theme: the
things a customer has been asked to run, read or explain by hand.

## Constraints

- Console output may carry device names; persisted diagnostics never do. The
  `--full` output goes to stdout and nowhere else. The ChangeDiff audit reports
  key NAMES, never payload values.
- The APIC CIMC software map is opt-in and full-only, for the same reasons
  the APIC CIMC inventory map is.
- The unmanaged prefix is opt-in and off by default: on, the config fetch is
  unscoped, which is the cost the 2.9.0 plan scoped the fetch to avoid.

## Touched Surfaces

- `forward_device_scope_reconciliation_audit --full`;
  `scope_reconciliation.py` exposes `_missing_in_netbox` beside its siblings.
- `health.py` `_quarantine_cadence_summary` and its Health card.
- `test_jobs.py` - a `KeyError` re-raises, so RQ's failed registry sees it.
- `changediff_audit.py`, `forward_changediff_audit`.
- `forward_platforms.nqe` derives the platform name with
  `normalizeDevicePlatformName(device)`, as every consumer does.
- `forward_dlm_apic_cimc_inventory_item_software.nqe`, registered opt-in and
  full-only; alias-variant exemption recorded.
- `config_backup.py` `UNMANAGED_BACKUP_REPO_PREFIX`,
  `config_backup_include_unmanaged` on the source form.
- The dead `slugify(name.replace(".", "-"))` arm at its three call sites.

## Approach

Nothing here changes a sync's behaviour by default. The platform NQE change
is the one exception: a fabric whose `platform.os` does not say "aci" now
produces its platform row under "ACI", the name every consumer already looked
for - so the consumers' rows stop skipping as missing a parent. That is the
one code-verifiable inconsistency the `softwareversion` skip diagnosis had
left, and it moves the bundled query's hash, so deployments on a pinned org
query see local drift until they republish; that is the normal flow.

## Validation

`test_scope_audit_full`, `test_quarantine_cadence`, `test_changediff_audit`,
the new `test_jobs` case, `test_config_backup` (three new), `test_query_registry`
(the new map's registration and exemption), `test_health`, `test_tag_contracts`
and the ownership suites for the slug arm. Full Django suite.

## Rollback

Revert. Each item is independent; a partial revert is one hunk.

## Decision Log

- **The changediff audit is a scan, not a repair.** Its plan asked whether
  damage exists; deleting rows is the operator's call once the answer is known.
- **Two foreign keys is the flag threshold.** One stray key is a serializer
  quirk; a Device snapshot under an IPAddress diff carries several.
- **The unmanaged prefix is a sibling of `configs/`, not a child**, because
  Validity binds `configs/<name>.cfg` to NetBox devices and an unmanaged device
  has none.

## Open

- Nothing.
