# Stale deferrals: what was carried open after it had shipped

## Goal

Close, in one place, the items that several release plans still list under
`## Open` although the code landed releases ago. Each entry names the commit
and the live call site, both verified against the 2.9.2 tree.

## Constraints

- Verification is by call site, not by symbol. A predictor that exists and is
  never called is not closed - this repository has shipped exactly that, which
  is why `test_protecting_reference_deletes.py` pins the caller with
  `inspect.getsource`.
- Nothing here changes behaviour. It is documentation of work already done.

## Touched Surfaces

`docs/03_Plans/active/` only:
`2026-08-05-one-validation-disposition.md`,
`2026-08-02-hidden-protect-merge-prediction.md`,
`2026-08-11-dlm-hardware-notice-leftovers.md`,
`2026-09-02-release-2.9.2.md`, and this file.

## Approach

| Item | Landed | Live at |
|---|---|---|
| C7 - merge validation disposition (#39/#50) | `bb0ac0d` (#146) | `merge.py:68,98`; written set attached at `bulk_merge.py:627`, `sync_primitives.py:145,180`; adapter half `sync_reporting.py:723`; predicate `diagnostics.py:901` |
| C8 - hidden-PROTECT merge predictor | `2484d33` (#122) | `bulk_merge.py:1480` (`include_hidden=True`), `:1515`, `:1318`, called `:2187`; `merge.py:651` |
| C9 - re-pointed maps orphan rows | `691f67a` (#182) | `full_removal_reconciliation.py`, consumed `query_fetch_execution.py:51-56` |
| `poetry.lock` must describe `pyproject.toml` | - | `check_release_preflight.py:237` |
| `requirements-release.txt` audited | - | `check_release_preflight.py:155` (`RELEASE_TOOLCHAIN`) |
| Preflight reports `skipped`, not a false `passed` | - | `check_release_preflight.py` (13 sites) |
| `exact_comparison` actually produced | - | `views.py:525,789` |
| Diff delete path has an allowlist | #217 | `DIFF_REMOVAL_MODELS` / `diff_removals_allowed` in `full_removal_reconciliation.py`, pinned by `test_diff_removal_allowlist.py` |
| `.dev0` on main decided; `--open-next` removed | #313 | absent from `scripts/release.py` |
| Undeletable ingestions #45/#46 | - | no `PROTECT` relation to `ForwardIngestion` remains |
| Route probe covers registered detail views | #330/#331 | `scripts/validate_installed_routes.py` |

## Validation

Documentation only; no test change. Each row above was grepped in the 2.9.2
tree before being written down, and the three C-items were confirmed to have
callers rather than merely definitions.

## Rollback

Revert this file and the four plan edits. No code is affected.

## Decision Log

- **The cost is real, not clerical.** The 2.9.2 plan deferred C7, C8 and C9
  with "each needs its own blast-radius review" - a review of code that was
  already merged, in one case three releases earlier. That plan's own Decision
  Log names the failure mode ("a closure nobody can see gets re-investigated")
  one section above the list that commits it.
- **One index instead of edits everywhere.** The same item is carried in up to
  four release plans. Editing every one of them is how the next sweep gets
  skipped; this file is the single place to check, and the four plans a reader
  actually picks up point at it.

## Open

- Older release plans still carry these items in their own `## Open` sections.
  They are historical records of what was true at that release and are left
  alone deliberately; this file is the current answer.
