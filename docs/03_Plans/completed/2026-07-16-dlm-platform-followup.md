# DLM And Platform Follow-up

## Goal

Make the post-2.5.10 Forward-to-NetBox workflow expose the CVE metadata and
software associations Forward already knows, and associate NetBox Platforms to
manufacturers when the selected Forward inventory proves that ownership is
unambiguous.

## Constraints

- Initial implementation phase only: do not publish queries or release until
  separate live acceptance is authorized.
- Preserve the existing global Platform identity (`name`/`slug`); do not create
  vendor-prefixed duplicate platforms or churn device Platform foreign keys.
- NetBox Platform has one optional manufacturer. A normalized platform observed
  under multiple manufacturers must remain manufacturer-less.
- Keep NQE responsible for row normalization and ambiguity decisions.
- Keep all DLM behavior optional and compatible with installations that do not
  install `netbox-dlm`.
- Do not overwrite operator/NVD CVE metadata when Forward has no corresponding
  value.
- Treat `CVE.affected_software` as cumulative catalog knowledge derived from an
  observed Forward vulnerability. Removing the last current device instance
  must not claim that the software version is no longer affected.

## Touched Surfaces

- `forward_netbox/queries/forward_platforms.nqe`
- `forward_netbox/queries/forward_dlm_cves.nqe`
- `forward_netbox/utilities/sync_runner_adapters.py`
- `forward_netbox/utilities/apply_engine_bulk.py`
- `forward_netbox/utilities/sync_dlm.py`
- focused query, adapter, bulk-engine, and optional-plugin tests
- built-in NQE map and behavior reference documentation

## Approach

1. Group completed Forward devices by normalized Platform identity. Collect the
   distinct canonical manufacturers for each group and emit manufacturer fields
   only when exactly one manufacturer owns the platform in the selected
   inventory.
2. Make the Platform map authoritative for manufacturer updates. Device apply
   may set a manufacturer when it must create a missing endpoint-only Platform,
   but it must preserve the Platform map's decision on existing records.
3. Keep adapter and bulk-ORM Platform behavior equivalent, including clearing a
   legacy manufacturer when the query reports an ambiguous platform.
4. Extend the CVE catalog query with the earliest known vendor publication date,
   one deterministic advisory URL, and maximum overall/v2/v3 CVSS scores.
5. Parse and apply those optional CVE fields only when Forward supplied values.
6. When a Vulnerability links a CVE, SoftwareVersion, and Device, add the same
   SoftwareVersion to `CVE.affected_software`. Keep this relation cumulative;
   vulnerability deletion removes the current exposure instance only.
7. Update reference docs and regression tests for query shape, adapter behavior,
   bulk parity, metadata preservation, and installed `netbox-dlm` relations.

## Validation

- Live-compiled and executed the Platform and CVE queries against the configured
  validation organization without publishing them. Platform output was unique
  by slug with no ambiguous manufacturer in the observed rows; all sampled CVE
  rows included publication date, link, and overall score.
- Passed 412 focused NetBox 4.6.4 tests covering Platform adapter/bulk parity,
  endpoint behavior, query contracts, and DLM integration.
- Passed all 1,202 plugin tests in an isolated NetBox 4.6.4 runtime (32 expected
  optional-plugin skips).
- Passed all 21 DLM integration tests with `netbox-dlm==0.2.0` installed and
  enabled, including the real `CVE.affected_software` relation.
- Passed `invoke harness-check`, all 135 `invoke harness-test` tests,
  `invoke lint`, `invoke docs`, `git diff --check`, and NetBox `manage.py check`.
- Did not run the monolithic `invoke ci` wrapper because it starts/builds the
  shared runtime and packages artifacts. Its relevant code, query, harness,
  lint, docs, and system checks were run separately against the isolated tree.
- Subsequent authorized live acceptance published and matched all 32 bundled
  queries. Aggregate merged data showed every Platform manufacturer populated,
  zero Device-to-Platform manufacturer mismatches, and exact CVE vulnerability
  to affected-software set parity.

## Rollback

Revert this follow-up branch. No migration or release-state cleanup is required.
If a preview sync has staged the behavior, discard its branch before reverting.

## Decision Log

- Rejected assigning `device.platform.vendor` directly on every device row:
  generic normalized platforms can span vendors, while NetBox permits only one
  Platform manufacturer; row order would decide the result and create drift.
- Rejected vendor-prefixing Platform names/slugs: it would churn existing device
  and DLM SoftwareVersion foreign keys and duplicate globally useful OS
  identities.
- Chose the maximum CVSS values across vendor advisories as the conservative,
  deterministic security value and the earliest advisory date as the published
  date.
- Chose an additive affected-software relation because a version remains
  affected after the last currently observed device upgrades or leaves scope.
