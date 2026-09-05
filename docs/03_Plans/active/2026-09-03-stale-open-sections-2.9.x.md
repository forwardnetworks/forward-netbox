# Stale 2.9.x open items: what was carried after it had shipped

## Goal

Close, in one place, the items the 2.9.0 and 2.9.1 plans still list under
`## Open` although the code landed before 3.0.0. Each entry names the commit
and the live call site, verified against the 3.0.0 tree.

This is the second sweep of this kind. `2026-09-02-stale-deferral-sweep.md`
did the same for C7/C8/C9, and the 2.9.2 plan's Decision Log names the failure
mode ("a closure nobody can see gets re-investigated") one section above the
list that committed it. One index is the fix; editing every plan is how the
next sweep gets skipped.

## Constraints

- Verification is by call site, not by symbol. A predictor that exists and is
  never called is not closed.
- Nothing here changes behaviour. It is documentation of work already done.

## Touched Surfaces

`docs/03_Plans/active/` only: `2026-08-22-release-2.9.0.md`,
`2026-08-27-release-2.9.1.md`, and this file.

## Approach

| Item | Carried in | Landed | Live at |
|---|---|---|---|
| `netbox_dlm.softwareversion` sweep enumerated the whole table | 2.9.0 | `7e07c16` (#318) | `CATALOG_SWEEP_MODELS` `workload_state.py:739`, gated `:980`, pinned `test_workload_state.py:629` |
| Unmanaged devices archived under a separate prefix was "an unanswered product question" | 2.9.0 | `0fbbc93` (#322) | `UNMANAGED_BACKUP_REPO_PREFIX`; answered as an opt-in in the comment at `config_backup.py:308`, parameter `config_backup_include_unmanaged` |
| Nothing consumes `poetry.lock` | 2.9.1 | `a26bc58` (#313) | `poetry check --lock` at `check_release_preflight.py:243` |
| A report of exactly 10 skips may be the cap, not the count | 2.8.9 → 2.9.0 | `721e816` (#319) | `sync_reporting.py:201-206` reports `total` and `remainder` |
| The skip rollup's wording was wrong for a protected-delete skip | 2.8.9 → 2.9.0 | `721e816` (#319) | `emit_dependency_skip_issue_summary`, pinned by `test_skip_rollup_direction.py:91` |
| A refused delete still wrote a durable-state tombstone | 2.8.9 → 2.9.0 | `b210944` (#323) | `2026-09-02-refused-deletes-are-retried.md`, whose own Open is "Nothing" |

## Validation

Documentation only; no test change. Each row was grepped in the 3.0.0 tree
before being written down, and each was confirmed to have a caller rather than
only a definition.

## Rollback

Revert this file and the two plan edits. No code is affected.

## Decision Log

- **The 4.7 item is not listed** because it did not go stale - it was carried
  open correctly until NetBox 4.7.0 and branching 1.2.0 shipped, and 3.0.0
  closed it on 2026-09-03.
- **Older plans keep their own `## Open` sections.** They are historical
  records of what was true at that release. This file is the current answer.
- **The host's close-mode HTTP fault is not swept**, because it was still real:
  the last caller using the fault-prone pattern is ported in the same tranche
  as this sweep rather than being declared closed here.

## Open

- Nothing. The remaining 2.9.x work is in
  `2026-09-03-release-stage-idempotency.md`.
