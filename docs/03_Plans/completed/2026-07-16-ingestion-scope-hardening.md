# 2.5.10 Ingestion and Scope Hardening

## Goal

Eliminate the ingestion-issue flood and low-fidelity endpoint imports shown by
the post-2.5.10 support bundle while preserving a reviewable, source-scoped
workflow for console servers, DLM data, CIMCs, and maintained scope tags.

## Constraints

- Initial implementation phase only: do not publish queries, push, tag, publish
  packages, or release until separate live acceptance is authorized.
- Treat the supplied support bundle as diagnostic evidence only; do not commit
  customer identifiers, screenshots, raw rows, network IDs, or snapshot IDs.
- Keep NQE as the source of truth for endpoint classification and model-shaped
  rows; Python scope probes must use the same predicates.
- Preserve generic SNMP endpoint import as an explicit opt-in, but make the safe
  default console-server-only.
- Do not automatically delete existing devices or global catalog objects.
- Keep changes compatible with optional `netbox-dlm` absence.

## Touched Surfaces

- DLM device-scope pushdown and local scope filtering
- built-in query base/alias conflict resolution and health diagnostics
- endpoint NQE, endpoint scope probe, source form parameters, and cache identity
- device adapter stale maintained-tag cleanup
- focused query, form, health, scope, DLM, and support diagnostics tests
- operations, troubleshooting, and built-in NQE reference documentation

## Approach

1. Add all device-derived DLM maps to the device-name pushdown registry and map
   `netbox_dlm.vulnerability.name` as its local device identity. This prevents a
   tag-scoped run from applying global vulnerability rows and keeps a local
   safety filter if query pushdown is unavailable.
2. Teach built-in map resolution that the alias-aware DLM hardware-notice map
   supersedes the base map. Add the pair to the visible conflict check and make
   the DLM alias diagnostic warn when both are configured.
3. Add `sync_generic_endpoints`, default false. With endpoint import enabled and
   the new option false, emit only recognized Avocent/Opengear console servers;
   allow the current generic endpoint behavior only when explicitly enabled.
4. Keep endpoint query and live probe predicates identical, including tag scope,
   generic-endpoint policy, and CIMC exclusion. Detect CIMCs by endpoint name in
   addition to profile and sysDescr.
5. Log console-server and generic endpoint counts without names so future
   support bundles explain endpoint expansion without exposing inventory.
6. When a `dcim.device` row applies successfully, remove the maintained
   `forward-out-of-scope` tag from that device. Do not otherwise alter user or
   feature tags.
7. Add regression tests matching the support-bundle failure shapes: a large
   out-of-scope vulnerability set, both hardware-notice variants enabled,
   generic/CIMC endpoint candidates, and an in-scope device carrying a stale
   out-of-scope tag.

## Validation

- Focused query-fetch, query-registry, health, endpoint, scope, form, adapter,
  DLM, and support-bundle suite: 431 passed, 26 skipped.
- Installed `netbox-dlm==0.2.0` suite: 24 passed, 0 skipped.
- Full isolated plugin suite: 1,220 passed, 34 expected optional-plugin skips;
  NetBox system check reported no issues.
- Isolated scenario suite: 11 passed; harness suite: 135 passed.
- `invoke harness-check`, `invoke lint`, `invoke docs`, and `git diff --check`
  passed.
- `invoke playwright-test` passed against a managed Chromium executable. It
  verifies the source form exposes the scope-tag, console-server import,
  generic-endpoint opt-in, and endpoint include-scope controls; desktop/mobile
  pages have no horizontal overflow.
- Aggregate `invoke ci` passed, including Docker build/start, system check,
  scenario tests, full tests, Playwright, docs, and local wheel build.
- The supplied support bundle was reproduced offline: 1,627 of 1,638 issues
  were missing-device DLM vulnerability failures and the remaining records
  represented 17 duplicate hardware-notice skips plus their bounded summary.
- The implementation phase did not publish or execute candidate queries. The
  later authorized acceptance published and matched all 32 bundled queries,
  then completed corrected and same-snapshot convergence syncs with zero
  ingestion issues, failures, or deletes.

## Release Acceptance

1. Install the candidate plugin in a staging instance that uses the same
   Forward source and NetBox data shape as the affected environment.
2. Publish the candidate bundled queries with overwrite, then prove the query
   audit reports no missing, stale, lookup-error, or contract-gap entries.
3. Keep `sync_generic_endpoints` off, enable endpoint include scoping, and
   enable scope-tag application for the affected sync.
4. Run dependency preview, sync, and a second dependency preview. The second
   preview must converge rather than reproduce mostly-drift behavior.
5. Export the support bundle and verify: no missing-device vulnerability flood;
   only the alias hardware-notice variant is effective; generic endpoint count
   is zero; CIMCs are absent from endpoint imports; recognized console-server
   counts are expected; successfully imported devices do not retain
   `forward-out-of-scope`.
6. Review Scope Reconciliation before pruning stale generic endpoints, CIMC
   devices, or legacy endpoint DeviceTypes created by earlier releases.

## Rollback

Revert this follow-up tranche. No migration or automatic cleanup is introduced;
existing endpoint devices and catalog objects remain untouched.

## Decision Log

- Rejected treating 1,627 missing DLM devices as independent failures. They are
  one scope-contract defect and should be prevented before apply.
- Rejected silently removing generic endpoint capability. It remains available
  behind an explicit source option because some operators may intentionally use
  low-fidelity generic SNMP devices.
- Rejected automatic deletion of current generic endpoint/CIMC device rows. The
  plugin lacks durable source ownership, so cleanup remains a reviewed Scope
  Reconciliation action.
- Rejected making the persisted global out-of-scope tag authoritative across
  multiple syncs. Successful in-scope apply clears stale state; live Scope
  Reconciliation remains the authoritative per-sync calculation.
