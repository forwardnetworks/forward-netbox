# Site mapping rules

## Goal

An operator maps Forward devices to NetBox sites in the Forward source settings,
without writing anything in Forward. One ordered list of rules, each saying
"devices whose Forward location matches this pattern, or that carry this tag, go to
this NetBox site". By default a device's NetBox site is its Forward location; a
mapping is what changes that. The `NetBox_Site_<name>` tag convention from 3.0.16
stays as an explicit per-device override and keeps working.

Today, grouping many Forward locations into one NetBox site means tagging every
device in Forward: it needs Forward write access, it cannot reach virtual children
(firewall virtual systems, access points) without snapshot-scoped writes, and it took
hours on a 2,960-device estate. A location rule such as `DC01*` -> `dc1` matched the
same 1,480 devices exactly (probed live, read-only), covers virtual children (they
carry a location), and is reviewed and edited in NetBox.

## Constraints

- No customer names, network ids, snapshot ids or hostnames in the repo.
- No data migration: one new optional source parameter `site_rules`.
- Forward requires every declared parameter to be supplied (probed), and rejects an
  undeclared one with HTTP 400. So the plugin cannot simply omit or always send it.
- A source with no rules must behave exactly like 3.0.16 on EVERY binding: query-id,
  path, pinned, sharded, tag-scoped, republished or not. Same parameters sent after the
  narrow fallback, same durable hashes, same rows.
- A configured rule is never silently ignored. If any map that names a site cannot
  honour the rules (an unrepublished or custom or remote-only query, a map on the
  incremental diff path), the sync stops before applying anything with a message
  that names the maps and says what to do.
- Precedence per device: an explicit `NetBox_Site_<name>` tag, then the first matching
  rule by list position, then the Forward location (as today). Site names stay
  lowercased and slugged like every site name the sync makes.
- The 3.0.16 guards apply unchanged: a site the sync matched is never renamed, one
  another actor created is never deleted, and a referenced site is never deleted.
- Support bundle carries every new UI figure (standing rule).
- Customers republish the shipped queries once for 3.0.17.
- 3.0.17, after the first real 3.0.16 site move on a customer network is observed.

## Touched Surfaces

- `queries/netbox_utilities.nqe`: new helpers `siteNameForRules` and `deviceSiteNameRules`;
  `deviceSiteName` and `siteNameFor` keep their shape so customers' own org queries
  that import them keep compiling. The text "site_rules" appears in the helper file
  ONLY as a function parameter name that the registry does not detect (see Approach 3).
- `queries/forward_devices.nqe`, `forward_devices_with_netbox_aliases.nqe`,
  `forward_vlans.nqe`, `forward_locations.nqe`: declare `site_rules` in their `@query`
  signature and pass it to the helpers (including the SNMP-endpoint site in the two
  device maps); `forward_locations` keeps devices that a rule or tag places even when
  they have no Forward location.
- `utilities/site_tag.py`: Python statement of the rule + the rule parser/validator.
- `utilities/query_registry.py`: parameter default detection from the declared `@query`
  signature (not a substring of the compiled text); `SITE_RULES_PARAMETER_DEFAULT`.
- `utilities/query_fetch_execution.py`: `ForwardQueryContext.site_rules`, injection in
  `_apply_context_tag_parameters`, context cache key, the narrow fallback in the run
  path, the inline `scoped_site_names` query declared with `@query`.
- `utilities/query_execution_contract.py`: `scope_config_fingerprint` includes
  non-empty rules; the declared-parameter type comparison.
- `utilities/tier3_reducers.py` / `query_registry.py` diff selection: maps that name a
  site are full-fetch only while rules are non-empty.
- `utilities/workload_state.py`: `site_rules` excluded from the parameter hash when
  empty (so 3.0.17 does not `contract_reset` a source that did not change); included
  when non-empty.
- `utilities/scope_reconciliation.py`: the inline queries carry the rules as a declared
  parameter; per-rule match counts folded into the existing scope query (no extra
  NQE call).
- `forms.py` (both `clean()` and `save()`, which rebuild parameters from scratch),
  `utilities/model_validation.py` (allowlist and types), `models.py` (bundle
  allowlist), `api/` (through `model_validation`), `views.py` source summary,
  `utilities/health.py`: the `site_rules` source parameter, UI and checks.
- `views.py` + the scope reconciliation template: a "Site rules" card; the same figures
  in the support bundle (`bundle_label_index`).
- `sync_execution.py` and `module_readiness.py`: build parameters the same way and
  must see the rules.
- Docs: `site-tag-override.md`, `built-in-nqe-maps.md`, a troubleshooting entry.

## Approach

1. **Rule model.** A rule is `kind` (`location` | `tag`), `pattern` (location: a glob as NQE `matches`
   evaluates it, case-insensitive, probed live: `*`, `?` and `[12]` all work; reject a
   blank pattern and a lone `*`, which would catch every device; tag: an exact tag name), `site`
   (a name that slugs to a non-empty slug), and a `rank` assigned from the list position
   when the parameter is built, never stored. Save-time validation: at most 100 rules;
   no blank or duplicate (kind, pattern); no two rules whose site slugs collide with
   different names; exact keys only (`kind`, `pattern`, `site`).
2. **Form.** The help text says: the Forward location is the NetBox site unless a mapping
   applies; precedence is a tag mapping, then a location mapping, then the location.
   Each row is a kind selector, a value and a NetBox site name. A tag value is a
   dropdown filled from Forward's tag list (the existing
   `/api/plugins/forward/source/available-tags/` endpoint, which works once the source
   is saved); a location value is a typed glob with Forward's location names offered as
   suggestions. `site_rules` is added to BOTH parameter dicts (`clean()` and `save()`)
   and carried over from `existing_parameters` when the form does not post it, so
   rotating a password cannot erase the rules; a test proves it. The `enable_predict`
   key looks dropped by `save()` in the same way: verify, and fix it in the same change
   if so.
3. **Parameter detection.** `_default_query_parameters` learns `site_rules` from the
   declared `@query` signature (`declared_query_parameters`) of the compiled query, not
   from the text. Only the four maps that declare it get it, never the ~30 other
   queries that merely import `netbox_utilities`.
4. **Sending.** `site_rules` joins `ForwardQueryContext`, the context cache key and the
   scope fingerprint, and is injected in `_apply_context_tag_parameters` keyed on the
   query's declaration, in every filter mode. It is a list of `{kind, pattern, site,
   rank}` records.
5. **Un-republished queries (the safety fallback).** The plugin always sends the full
   declared set. If Forward rejects a run with "'site_rules' is not a parameter" and the
   rules are empty, retry once without `site_rules` only, and remember that per map for
   the run. If the rules are non-empty, stop with a clear message: which maps, and
   "republish the bundled queries". Custom and remote-only maps for a site-naming model
   with rules set block the same way. This replaces the earlier reliance on the legacy
   retry, which does not cover query-id bindings or sharded syncs.
6. **Incremental diff.** The maps that name a site (`dcim.site`, `ipam.vlan`, and the
   device maps) run full-fetch only while rules are non-empty, with a recorded reason
   code, because their diff revisions are parameterless.
7. **Durable state.** Empty rules do not change any hash (3.0.17 on an untouched source
   is identical to 3.0.16). Non-empty rules are in the hash for the site-naming models,
   so a rule change takes the existing `contract_reset` path: devices MOVE (the 3.0.16
   moved-device and VLAN re-stage mechanisms apply), nothing is deleted by the reset,
   but old-site VLANs and emptied sites are no longer tracked and are left in NetBox
   until Prune orphans. That is stated in the UI, the docs and the rollback.
8. **Inline queries.** `scoped_site_names` and the scope-reconciliation queries are
   declared `@query` with `site_rules` and receive it as a parameter. Operator text is
   never interpolated into NQE.
9. **Preview.** Per-rule match counts and "matches no rule" come from the existing scope
   reconciliation query on the snapshot, not an extra call; a tag rule whose tag no
   longer exists in Forward shows "matches 0 devices". The bundle carries the same
   figures under the UI's labels.
10. **Two sources.** Two sources covering the same devices with different rules would
    move them back and forth every run. Documented in the reference page; a Scope
    page warning is not built (nothing in the data model says two sources cover the
    same devices, so a warning would guess).

## Validation

- Differential, no rules: for each binding kind (query-id, path, pinned, sharded,
  tag-scoped) the parameters sent, the durable parameter hash and the contract
  decision equal 3.0.16's; against the real un-republished query shape the sync does
  not fail.
- Unit: parser and every rejection; form round trip including a password-rotation save
  keeping the rules; parameter detection only on the four maps (and a test that no
  other bundled query gains the parameter); injection in each filter mode; the narrow
  fallback (empty rules retry once and remember; non-empty rules stop with the message);
  diff maps forced to full fetch; precedence (tag > rule > location); rank by position;
  a device with no location placed by a rule; the endpoint site; hash behaviour for
  empty vs non-empty rules.
- Query: the record-list parameter through a helper function and the `matches` glob
  semantics are probed (both work, 2026-11-18); keep tests that pin them; the NQE `slugify` agreeing with Django's on rule site
  names, contract tests that the four maps declare and pass `site_rules`.
- Live (read-only NQE on a production-shaped network): with no rules the four maps return
  the same rows as 3.0.16; the real three location rules select exactly the devices the
  3.0.16 tags selected; per-rule counts equal an independent count.
- UI: the form and Scope card via agent-browser; the figures in the support bundle.
- Gates: `invoke harness-check`, scripts tests, pre-commit, full `invoke test`, then the
  release gate; a code-reviewer audit on the exact commit before merge.

## Rollback

Remove the rules (or revert the release): sites fall back to the tag convention and then
to locations. Devices a rule moved move back on the next sync. Not undone by a rollback:
VLANs and sites the move left behind in NetBox stay until Prune orphans removes them
(dry run first). Customers who have not set rules are unaffected by the release.

## Decision Log

- Rules live on the source, beside the device-tag scope: they describe how a network's
  locations map to sites and are shared by every sync of that network.
- Location globs and exact tags only; regex is more power than operators need.
- Rank is derived from list position when sent, never stored, so ties cannot exist.
- Rejected: remapping site names in Python after the fetch. It cannot see tags and would
  have to reproduce every site-naming model outside the queries; hashes would disagree.
- Rejected: separate lists for kinds, patterns and sites; NQE has no list index.
- Rejected: a configurable tag prefix; it still needs a tag per device in Forward.
- Rejected: relying on the legacy-safe default retry; it needs a query path, all-default
  parameters and sends `{}`, so it fails the common bindings (code review, 2026-11-18).
- Rejected: detecting parameters by substring of the compiled text; `netbox_utilities` is
  copied into 34 queries.
- Tag values come from Forward's tag list, not typed, so a typo cannot silently match
  nothing; a tag that disappears from Forward stays visible and is flagged.
- `NetBox_Site_<name>` stays above the rules: free to keep, and lets an operator pin one
  device.
- Whether non-empty rules belong in the durable hash is a choice with a cost either way:
  in the hash gives `contract_reset` (moves, leaves stale VLANs/sites); out of the hash
  sends rule changes down the local-delta path like a 3.0.16 tag move. Chosen: in the
  hash, because the diff path and the fingerprint already treat rules as scope, and a
  reset re-stages every device against the new site names; revisit after the first
  production move.
