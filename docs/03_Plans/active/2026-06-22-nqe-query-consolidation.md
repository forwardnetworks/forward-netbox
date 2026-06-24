# NQE query consolidation via parameters (follow-up / tech debt)

Status: **BACKLOG** (not started). Logged 2026-06-22. Do after 2.0 validation ships.

## Goal

The plugin ships 49 `.nqe` queries; these will be committed into the canonical
Forward NQE library. Reduce the count via parameterization so the library does
not sprawl — without regressing the per-model sync workload model.

## Constraints

- Runtime stays 1:1 model→workload (coalesce fields, dependency rank, diff,
  by-ID resolution). Parameters collapse the *canonical library definitions*, not
  the runtime workloads — the registry binds `model → (query_path, parameters)`,
  so multiple models/variants can target one query with different params.
- No behavior change for existing syncs; merges must be parameter-equivalent.
- Each merged query still committed to the org repo + run by committed ID.

## Approach

Merge toggle/variant pairs into one parameterized query each.

Clean wins (the `_with_` variants live in `BUILTIN_OPTIONAL_QUERY_MAPS`, all
`enabled: False` — pure base-vs-enriched toggles):

- `forward_devices` + `forward_devices_with_netbox_aliases` → one query,
  `netbox_aliases: Bool`.
- `forward_device_models` + `forward_device_models_with_netbox_aliases` → one
  query, `netbox_aliases: Bool`.
- `forward_device_feature_tags` + `forward_device_feature_tags_with_rules` → one
  query, `include_rules: Bool`.
  → −3 files.

Bigger, perf-neutral (keep two scoped fetches, one source query):

- `forward_ip_addresses_ipv4` + `_ipv6` → one query, `family` param, run twice.
- `forward_prefixes_ipv4` + `_ipv6` → one query, `family` param, run twice.
  → −2 files. Cost: branchier single `.nqe` (`if family == 4 …`).

Maybe (defer): `forward_aci_command_inventory` / `forward_aci_apic_cimc_inventory`
/ `forward_aci_apic_nodes` parse overlapping APIC command output — possible
merge, but ACI parsing is gnarly; only if it pays off.

Net: 49 → ~44 (clean) / ~42 (with family merges).

## Touched Surfaces

- `forward_netbox/queries/*.nqe` — merge the listed files; add the new params.
- `forward_netbox/utilities/query_registry.py` — `BUILTIN_QUERY_MAPS` /
  `BUILTIN_OPTIONAL_QUERY_MAPS` point variants at the merged query_path + the
  toggle param; `_default_query_parameters` for the new params.
- Org NQE repo — commit the merged queries; retire the old paths after cutover.

## Validation

- Per-merge: diff the rows from old vs merged-with-param query on a live snapshot;
  must be identical.
- Full suite green; live initial + diff sync unchanged.

## Rollback

Per-merge commits; revert a merge to restore the split queries. Old org query
paths retired only after the merged ones are committed + validated.

## Decision Log

- **Parameterize, don't drop** — operator wants fewer canonical library entries;
  parameters merge variants while keeping per-model runtime workloads.
- **ipv4/ipv6 stay two fetches** — the family split was an intentional scale
  decision (task #16); a `family` param keeps two scoped runtime fetches from one
  source query, so no payload regression.
- **Deferred** — logged as tech debt; execute after 2.0 validation/release.
