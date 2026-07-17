# Live Acceptance And 2.5.11 Release

## Goal

Close defects found only by running the post-2.5.10 candidate against the same
Forward source and tag scope as the affected environment, then finish the
validation, preview, sync, and convergence sequence. Cut 2.5.11 only after the
live evidence and full release gate are clean.

## Constraints

- Do not commit, push, tag, publish a package, or release until the live
  acceptance sequence and full release gate pass.
- Keep customer identifiers, credentials, raw inventory, and screenshots out of
  the repository and final artifacts.
- Publish candidate queries only to the already-authorized validation org
  folder.
- Do not prune devices automatically; review scope cleanup separately.
- Model CIMCs only when an exact, deterministic parent relationship exists.

## Touched Surfaces

- optional-plugin post-migrate map seeding
- query-parameter projection and preflight execution
- optional CIMC endpoint inventory NQE
- malformed optional CVE advisory URL filtering
- fail-closed validation-org source selection and redacted readiness evidence
- query-registry and query-fetch regression tests
- configuration, troubleshooting, and NQE reference documentation

## Approach

1. Seed DLM built-ins when `netbox_dlm` migrates after Forward NetBox.
2. Project runtime query arguments onto each query's declared signature and use
   the same context-aware parameter builder for preflight, sample, and fetch.
3. Add a disabled native inventory map for CIMC endpoints named exactly
   `<parent-device>-cimc`; emit a row only when the completed parent device
   exists and is in the device-name shard scope.
4. Filter catalog advisory links to valid HTTP(S) URLs so malformed optional
   source data cannot reject an otherwise valid CVE row.
5. Require explicit validation-org source selection when more than one
   configured source exists; retain only redacted selection status in release
   evidence.
6. Republish and audit the validation-org query folder, then rerun validation,
   dependency preview, a bounded sync, and a second preview.
7. Verify aggregate DLM, endpoint, CIMC, scope-tag, issue, and convergence
   evidence without retaining customer rows.
8. Restore validation state, run the full release playbook, then build, tag,
   publish, and verify 2.5.11.

## Validation

- focused query-registry/query-fetch tests
- installed `netbox-dlm` tests
- validation-org publish/audit with zero gaps
- live validation run with zero model failures
- dependency preview with explicit estimate semantics and zero deletes
- bounded sync plus second dependency preview
- full harness, lint, docs, test, and diff checks

### Live Results

- DLM-enabled validation-org publication audit: 32 of 32 published queries
  matched; zero missing, stale, lookup-error, or contract-gap results. The
  final core-runtime readiness audit matched all 26 applicable queries with the
  same zero-gap result.
- Corrected live sync: 12 model results, zero ingestion issues, zero failed
  changes, and zero deletes. Its nonzero creates/updates came from a newer
  processed snapshot than the first acceptance run.
- Same-snapshot convergence sync: zero creates, updates, deletes, failures, or
  issues; the ingestion remained baseline-ready.
- Exact scope comparison: 3,626 current scoped names, zero missing NetBox
  devices, zero missing matched include tags, and zero out-of-scope conflicts.
  All 307 imported endpoints were normalized console servers; generic endpoint
  import was off and no device used the SNMP Endpoint role.
- CIMC relationship proof: 48 inventory items on 48 unique parent devices,
  with no missing parent and no standalone `-cimc` device.
- Platform proof: all 14 Platforms had a manufacturer and no Device had a
  Platform manufacturer that disagreed with its DeviceType manufacturer.
- DLM proof: 3,390 DeviceSoftware rows covered 3,390 unique devices; all 38,904
  Vulnerability rows had Device, SoftwareVersion, and CVE relationships; the
  353 CVEs with vulnerabilities exactly matched the 353 CVEs with affected
  software. No SoftwareVersion was device-orphaned. The previously rejected
  malformed-link CVE was retained with metadata and a blank link.
- Final dependency preview: 50,677 rows of potential apply work, zero deletes,
  `workload_upper_bound` semantics, no exact comparison, and therefore no
  claimed drift or in-sync state. The identical-snapshot sync is the no-op
  convergence evidence.
- Validation map enable states were restored and the dedicated acceptance
  source/sync were removed. No NetBox inventory was pruned and the published
  validation-org queries were retained.
- A release-readiness rehearsal exposed ambiguous automatic validation-source
  selection in a multi-source runtime. The audit now fails closed unless the
  source is explicit, redacts that source from persisted evidence, and passed
  against the intended validation organization.
- Final local release gate: 136 harness tests, 11 scenario tests, 1,224 Django
  tests with 34 expected optional-plugin skips, all lint/security hooks, the
  12-check Playwright harness with six screenshots, docs, image build/start,
  and package build passed. Both 2.5.11 wheel and sdist passed `twine check`.

## Rollback

Revert the follow-up code and restore the validation database's built-in map
enable states. No migration, prune, or release is involved.

## Decision Log

- Rejected treating a completed validation job as success without inspecting
  its validation run and per-model failures.
- Rejected fuzzy CIMC-to-parent matching. Only the exact `-cimc` suffix with an
  existing completed parent is accepted.
- Rejected extending the APIC command parser with fallback rows because command
  and endpoint evidence can coexist and create duplicate coalesce candidates.
  A separate optional map keeps the evidence sources explicit.
- Rejected accepting a runtime whose reported bind path was correct but whose
  mounted source was stale. Acceptance was rerun from the rebuilt candidate
  image without the stale source bind.
- Rejected allowing the readiness audit to choose the most recently synced
  source when multiple configured sources exist. That can prove the wrong
  organization; release evidence now requires an explicit choice.
