# Post-2.5.10 Upgrade Reconciliation

## Goal

Make the first post-2.5.10 workflow identify legacy objects left by earlier DLM
and endpoint behavior, expose CVE/software association cardinality, and prove
that two consecutive corrected syncs converge without silently deleting shared
or manually curated NetBox catalog objects.

## Constraints

- Initial implementation phase only: do not publish queries or release until
  separate live acceptance is authorized.
- Keep the report read-only for global SoftwareVersion, CVE, Platform, and
  DeviceType objects because those models do not record Forward-source
  ownership and may be shared by multiple syncs or maintained manually.
- Continue using Scope Reconciliation's source-scoped prune for stale devices,
  including standalone CIMCs excluded by the corrected endpoint eligibility
  predicate.
- Work when `netbox-dlm` is absent, installed but disabled for a sync, or fully
  enabled.
- Do not include customer object names in durable fixtures or documentation.
- Keep diagnostics local on page render; do not add new Forward API calls.

## Touched Surfaces

- a focused lifecycle/upgrade reconciliation utility
- Scope Reconciliation view and template
- support-bundle or health diagnostic payload where the existing boundary
  already carries local reconciliation evidence
- focused optional-DLM, UI, and upgrade-convergence tests
- operations and upgrade documentation

## Approach

1. Compute local, read-only counts for SoftwareVersions with no running devices,
   separating versions retained by CVEs or other DLM catalog relations from
   completely unreferenced cleanup candidates.
2. Report total CVEs and how many have Vulnerability and affected-software
   associations so a global-catalog explosion is visible before release.
3. Detect only high-confidence legacy endpoint DeviceType candidates: zero
   attached devices, Opengear/Avocent manufacturer identity, and the exact
   software-bearing signatures removed by the corrected NQE.
4. Expose counts and bounded samples on Scope Reconciliation without changing
   the existing prune button's deletion scope.
5. Include the local diagnostic in support evidence without raw customer rows.
6. Add a populated-state test representing 2.5.9 artifacts, exercise corrected
   adapters twice, and assert stable second-pass behavior plus accurate
   reconciliation classification.
7. Record the affected-environment-equivalent release acceptance sequence:
   publish candidate
   queries, preview, sync, preview again, and inspect support evidence.
   Publication remained deferred until the later authorized acceptance run.

## Validation

- focused lifecycle reconciliation and Scope Reconciliation UI tests
- optional-plugin-absent and installed `netbox-dlm==0.2.0` tests
- populated 2.5.9-state to post-2.5.10 two-pass convergence test
- `invoke harness-check`
- `invoke harness-test`
- `invoke lint`
- isolated NetBox `manage.py check` and focused/full tests
- `invoke docs`
- `invoke playwright-test` or focused rendered-template coverage when the live
  browser stack is unavailable

### Results

- Focused isolated reconciliation/UI/DLM stack: 44 tests passed with 3 expected
  optional-DLM skips and a clean system check.
- Installed `netbox-dlm==0.2.0` stack: 25 tests passed with zero skips,
  including CVE/software cardinality and two-pass 2.5.9-state convergence.
- Full isolated plugin suite: 1,206 tests passed with 34 expected skips and a
  clean system check.
- Harness suite: 135 tests passed.
- `invoke harness-check`, `invoke lint`, `invoke docs`, `git diff --check`, and
  isolated `manage.py check` passed.
- Isolated Playwright suite passed and produced desktop and mobile artifacts.
- No query publication, package, push, tag, or release was performed during the
  implementation tranche. Later authorized acceptance published and matched
  all 32 bundled queries and proved an identical-snapshot zero-change sync.

## Rollback

Revert this follow-up branch. The new behavior is read-only and has no migration
or cleanup side effects.

## Decision Log

- Rejected automatic deletion of zero-device SoftwareVersions and unreferenced
  DeviceTypes: neither model records Forward-source ownership, so deletion from
  one sync could remove another sync's or an operator's catalog data.
- Rejected narrowing the global CVE query before measuring representative
  cardinality. The report makes linked versus unlinked catalog scope explicit
  so that product decision can be made from evidence.
- Reused source-scoped device pruning for stale CIMCs instead of adding a second
  deletion path.
