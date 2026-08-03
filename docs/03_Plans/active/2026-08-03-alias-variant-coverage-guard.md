# Alias-Variant Coverage Guard

## Goal

Make the set of `*_with_netbox_aliases` query variants *provably* complete and
intentional, so the 2.4.2 defect class — behaviour implemented in a base query
and never ported to the variant operators actually run — fails a check instead
of shipping as a silent omission.

Prompted by a field report of `netbox_dlm.softwareversion` rows failing with
`ForwardDependencySkipError` on every sync at a site running the alias variant
of every device query, with the hypothesis that the two DLM platform maps needed
alias variants of their own.

## Contract

- Every built-in base query that names a NetBox object the alias-aware device
  query owns either ships a registered `*_with_netbox_aliases` variant, or is
  listed as an exemption with a written reason.
- Every registered alias variant resolves back to a base map *name*, so
  `_collapse_alias_variant_duplicates` can supersede its base. A variant that
  cannot be resolved would run alongside its base and flip the shared object
  between two spellings on every sync.
- Exemptions cannot rot: an exemption whose query file is gone, whose query no
  longer names an alias-sensitive object, or which contradicts a variant that
  has since been registered, fails the check.
- No behavioural change to any shipped query. Nothing an operator has enabled
  changes meaning.

## Constraints

- The local NQE validator binary is the wrong architecture and cannot
  syntax-check query files, so a query change carries unusual risk. This work
  therefore adds no query file.
- `forward_netbox/utilities/` is a high-risk path; this plan accompanies the
  diff.
- No customer identifiers in code, tests, plans, or commit messages.

## Touched Surfaces

- `forward_netbox/utilities/query_registry.py` — the rule, the exemption table,
  and `alias_variant_coverage_violations()`.
- `forward_netbox/tests/test_query_registry.py` — coverage, including negative
  cases that demonstrate the check failing.
- This plan.

## Approach

**The rule.** A base query is *alias-sensitive* when it emits a row field naming
an object the alias-aware device query creates (`device_type`,
`device_type_slug`, `platform`, `platform_slug`), or when it *produces* one of
those identities (`dcim.devicetype`, `dcim.platform`). Alias-sensitive base
queries must have a variant or an exemption. Detection reads the shipped query
source rather than a hand-maintained list, so a new map is classified by what it
actually emits.

**Why the two DLM platform maps are exemptions rather than new variants.** The
reported hypothesis was that `forward_dlm_software_versions.nqe` and
`forward_dlm_device_software.nqe` derive the platform with
`normalizeDevicePlatformName(device)` — the un-aliased name — and so cannot match
platforms created under alias-mapped names. Reading the queries does not support
that:

- `forward_devices_with_netbox_aliases.nqe:36` derives its platform with
  `let platform_name = normalizeDevicePlatformName(device)` — character for
  character what `forward_devices.nqe:32`, `forward_dlm_software_versions.nqe:21`,
  `forward_dlm_device_software.nqe:20` and `forward_dlm_vulnerabilities.nqe:21`
  use. The alias variant of the device query does not rename platforms.
- The alias data file carries only `device_type_alias` and
  `manufacturer_override` records (`scripts/build_netbox_device_type_aliases.py`).
  There is no platform record type, so there is nothing for a platform-scoped
  alias variant to map through.
- `device_type` — the only alias-mapped identity — is emitted by exactly four
  query files: the two device queries and the two hardware-notice queries. Both
  base/variant pairs already exist. That is why 2.5.3 needed a hardware-notice
  variant and why the same transformation has no counterpart here.

An alias variant of either DLM platform map would therefore be semantically
identical to its base: a catalogue entry an operator could enable expecting a
fix, and get none, in a catalogue that already carries a consolidation backlog.
The exemption table records that reasoning where the next person to ask the
question will find it, and a test asserts the premise (the shared helper) still
holds, so the exemption cannot quietly become wrong.

`forward_dlm_inventory_item_software.nqe` is exempt for a different and simpler
reason, recorded separately: it emits the hardcoded Platform `"CIMC"` that its
own apply adapter creates.

**What this does not do.** It does not explain the reported
`ForwardDependencySkipError`. That diagnosis is still open and is recorded below
rather than papered over with a no-op query.

## Validation

- `forward_netbox.tests.test_query_registry` — the check passes against the
  shipped registry; two negative tests drive it with synthetic map sets and
  assert it reports (a) an alias-sensitive base map with no variant and no
  exemption, and (b) a variant whose display name resolves to no base map.
- A test asserts every exemption is registered, has no variant, and carries a
  reason of substance.
- A test asserts the shared `normalizeDevicePlatformName(device)` expression is
  present in the alias device query and in all three DLM platform maps, so the
  exemptions' premise is checked rather than assumed.
- `invoke harness-check`, `invoke harness-test`.

## Rollback

Revert. The change is a registry-level assertion plus tests; no query, adapter,
or seeded map row changes, so nothing an operator has enabled behaves
differently.

## Decision Log

- 2026-08-03: Did not add
  `forward_dlm_software_versions_with_netbox_aliases.nqe` or
  `forward_dlm_device_software_with_netbox_aliases.nqe`. Diffing the
  hardware-notice pair shows the alias transformation is entirely
  `device_type`/`device_type_slug`; neither DLM platform map emits either field,
  and the platform value they do emit already matches the alias-aware device
  query verbatim. The files would have been placebos for a live customer-facing
  error.
- 2026-08-03: Scoped the rule to *emitted identity fields* rather than to
  "queries importing the alias data file". The latter would only ever recognise
  maps that were already fixed — it cannot detect the gap it exists to detect.
- 2026-08-03: Included `forward_dlm_vulnerabilities.nqe` in the exemption table.
  It emits exactly the same platform identity as the two maps under discussion;
  leaving it out would have made the table read as a list of two special cases
  instead of a rule.
- 2026-08-03: Added the variant-name resolution check alongside the coverage
  check. `_collapse_alias_variant_duplicates` matches on display name, and the
  one existing variant already needs a hand-written entry in
  `_EXPLICIT_ALIAS_VARIANT_BASE_NAMES` because its name does not follow the
  suffix convention. A second such variant added without that entry would cause
  perpetual churn with no test failure.

## Open

- The reported `netbox_dlm.softwareversion` `ForwardDependencySkipError` is
  **not** diagnosed. `_lookup_platform` fails only when no `dcim.Platform`
  matches the emitted slug or name, and alias mapping is now ruled out as the
  cause. One code-verifiable inconsistency found while looking, not yet tied to
  the report and not changed here: `forward_platforms.nqe` — the sole producer
  of `dcim.platform` — is the only query using `normalizePlatformName(os,
  version)`, while every consumer uses `normalizeDevicePlatformName(device)`.
  The latter additionally resolves APIC controllers and ACI devices from command
  outputs, so a device whose ACI/APIC identity is only visible in its command
  outputs is written by the device map under a platform name the platform map
  never emits. Whether that can leave the lookup without a match depends on
  prune behaviour and has not been established.
- Next diagnostic step should be the failing rows' `platform` / `platform_slug`
  values from the skip context against the site's `dcim.Platform` table, which
  settles it in one comparison.
