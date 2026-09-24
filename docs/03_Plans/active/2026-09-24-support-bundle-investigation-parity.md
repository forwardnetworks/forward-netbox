# Support bundle carries every question a customer investigation asked

## Goal

After upgrading to 2.9.8 a customer's uncovered-device count grew, endpoint
devices began arriving with full attributes, eleven models showed pending
removals, and a few rows failed on primary-IP pointers. Answering why took a
hand-written shell script run on the customer's NetBox, because the support
bundle carried none of it - and one field it did carry was stripped on export.

Make the bundle the one artifact that answers those questions, so an
investigation never again needs screenshots, CLI access or a script.

## Constraints

- Exports keep the two-tier rule: counts, primary keys, catalog names
  (manufacturer, platform, role, device type) and shapes - never device names,
  site names, tag values or raw query text.
- The bundle is generated from stored state and makes no Forward calls.
- A failing section must not break the bundle.
- No customer names or identifiers in this plan, the commits or the PR.

## Touched Surfaces

- `forward_netbox/utilities/bundle_diagnostics.py` (new) - nine sections.
- `forward_netbox/views.py` - `diagnostics` key in the support bundle.
- `forward_netbox/utilities/export_redaction.py` - `_detail` no longer a
  blanket drop suffix; bare `sample` dropped by exact key.
- `forward_netbox/utilities/query_fetch_execution.py` - `query_parameter_names`
  renamed `query_parameter_keys`.
- Tests: `test_bundle_diagnostics_parity.py` (new), `test_export_redaction.py`.

## Approach

1. **Stop stripping answers.** The export filter dropped any key ending
   `_detail`, which removed `absent_detail`/`endpoint_detail` wholesale - the
   per-cause breakdown of why devices are uncovered, whose reason/label/count
   are value-free aggregates. It also dropped plain-int fields that happened to
   end `_detail`. Only the bare `sample` key inside those rows names devices,
   and it was never matched by the `_sample` suffix at all - it stayed hidden
   only because its parent was dropped. Now `sample` is dropped by exact key
   and the aggregates survive. `query_parameter_names` (added in 2.9.8 as the
   safe replacement for parameter values) was itself stripped by the `_names`
   suffix; renamed `query_parameter_keys`.
2. **Carry the investigation's questions.** A `diagnostics` section with, each
   isolated: uncovered breakdown (manufacturer/platform/role/device type/
   status/site pk/created day, Forward-identity binding); uncovered-tag gain/
   loss timeline; scope-report trend across jobs; device renames (with a
   case-only count, for the device-identity fix); recently created devices;
   console-server counts with interface coverage; every NQE map's binding
   (execution mode, query/commit ids, path leaf and digest, raw-query digest
   and length, parameter keys, live drift incl. parameter-signature match); the
   devices and VRFs behind the latest ingestion issues (status, primary-IP
   pointers, identity bound or not, VRF reference counts); inventory totals.
3. **Change history records tags by name.** NetBox's `serialize_object` stores
   tag NAMES, so the timeline matches both the uncovered tag's slug and name;
   matching the slug alone read zero.

## Validation

- `test_bundle_diagnostics_parity`: every section computed without error, each
  answers its question against fixtures shaped like the customer's (Avocent
  console server, case-only rename, IP and VRF issues), and the exported JSON
  carries no device, site or VRF name.
- `test_export_redaction`: reason breakdown survives with only `sample`
  dropped; plain-int `_detail` fields survive; bare `sample` dropped anywhere.
- Full `invoke ci` before push.

## Rollback

Single change set; revert restores the previous bundle. No migration.

## Decision Log

- **A bundle section, not a management command.** Diagnostics belong in the
  GUI; a CLI-only answer is unfinished work, and the customer should not need
  shell access for us to diagnose them.
- **Catalog names ship; network names do not.** Manufacturer and platform are
  vendor catalog values and are what distinguish "these are console servers"
  from "these are switches". Site names can carry street addresses, so sites
  ship as pks.
- **Raw query text ships as a digest and length.** A customized query can embed
  customer names in literals; its digest still answers "is this the stock
  query".
- **Build backend pinned to poetry-core 2.5.0.** The host's system package
  moved to 2.5.0 on 2026-09-23 and the reproducible-build gate, which checks
  the pin against the host interpreter, refused every push. Bumped in
  `pyproject.toml`, `requirements-release.in` and `requirements-release.txt`
  (PyPI SHA-256 digests for the 2.5.0 wheel and sdist).
  `scripts/build_reproducible_distribution.py` builds twice and compares
  digests; it passes on 2.5.0.
- **`duplicate_device_names` added.** A customer's follow-up: the same
  endpoint name appears twice, once per site, one copy still carrying an
  include tag the other lost. The mechanism is a device lookup keyed on name
  alone - when an endpoint's site label changes between syncs, the lookup
  misses under the new site and creates a second device instead of updating
  the first, and the stale copy drifts out of tag scope. Groups every device
  by casefolded name, reporting count, distinct site count, same-site repeat
  count, and whether the group's tag SETS differ (not what they are) - the
  shape that confirms one copy fell out of scope while the other did not.
  Device and site names are never exported; site ships as a pk.
