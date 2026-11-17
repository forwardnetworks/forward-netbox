# Moving Devices Between Sites Keeps Their VLANs, and Never Fights Another System's Sites

## Goal

Grouping Forward locations into one NetBox site (the `NetBox_Site_<name>` tag) moves
devices between sites. Two things made that unsafe:

- A moved device's interfaces lost their untagged VLAN. The plugin clears an
  untagged VLAN that belongs to the old site (NetBox refuses a cross-site untagged
  VLAN), and the interface rows, which name a VLAN only by id, did not change, so an
  incremental run never wrote the new site's VLAN back.
- The sync renamed any site it matched to its own lowercase name and slug, and
  deleted a site it had matched once Forward stopped returning it. A customer's
  inventory system writes its own sites into NetBox; the two would rename the same
  site back and forth, and the sync could remove a site that was not its own.

## Constraints

- No customer names, network ids, snapshot ids or hostnames in the repo.
- No change for a run where no device moves site: the same rows are staged.
- A site the sync created itself is still removed once Forward stops returning it
  and nothing references it, including sites from a fast baseline load (which write
  no change log).

## Touched Surfaces

- `utilities/workload_state.py`: `_devices_changing_site`, `SITE_DEPENDENT_CHILD_MODELS`;
  unchanged `dcim.interface` rows of a moving device are staged again;
  `site_move_restaged_rows` in the summary and the durable-state log line.
- `utilities/sync_runner_adapters.py` (`_ensure_site`) and
  `utilities/apply_engine_bulk.py` (`CREATE_ONLY_UPDATE_FIELDS_BY_MODEL`): a site's
  name and slug are create-only in both apply paths.
- `utilities/sync_core_models.py`: `delete_dcim_site` keeps a site NetBox's change log
  shows another actor created, with a warning, and keeps any site something still
  references (NetBox cascades a site delete into prefixes and clusters scoped to it,
  and into change-policy rules, whose relation is declared with `related_name="+"`).
- `docs/02_Reference/site-tag-override.md`.
- Tests: `test_site_move_restages_interfaces.py`, `test_site_adoption.py`; two bulk
  engine tests in `test_apply_engine.py` updated (one asserted the old rename, one
  used a site rename only as a vehicle and now uses a manufacturer).

## Approach

1. Before the deltas, compare each device row's site with the device's site in
   NetBox; a device whose site differs is moving this run.
2. For `dcim.interface`, a row of a moving device is staged even when its hash is
   unchanged, so the interface apply writes the new site's VLAN after the device
   apply cleared the old one.
3. A site matched by slug or name is used as it is: no rename, no new slug.
4. A site is deleted only when no create record proves another actor made it; the
   proof is a create record under a request that is not one of this sync's runs.

## Validation

- `test_site_move_restages_interfaces`: a moving device's unchanged interface rows are
  staged again; nothing is staged when no device moves or the device is new; and the
  whole chain on real models: the device moves, its old-site VLAN is cleared, the
  re-applied row points the interface at the new site's VLAN.
- `test_site_adoption`: a site matched by slug keeps its name, by name keeps its slug,
  in the row path and the bulk path; a site another actor created is kept on delete,
  also with a branch active; one this sync created or with no create record is
  deleted as before.
- The chain is also tested on the bulk interface path, and a device at a site
  matched by name is not treated as moving.
- `DeleteNeverCascadesTest`: a site a prefix or a change-policy rule is scoped to is
  kept with that object (mutation-checked: fails with the check disabled); an
  unreferenced site is deleted.
- Existing workload-state, apply-engine, bulk-parity and cross-site VLAN tests pass.
- Not verified live: a real site move on a customer-shaped estate. The test host is
  down; rehearse before any customer groups locations.

## Rollback

Revert the commit. No migration.

## Decision Log

- Re-staging is limited to `dcim.interface`, the one child model whose writes depend
  on the device's site (site-scoped VLANs), and to interface maps that fetch in full
  (the default). A map on Forward's own snapshot diff never receives the unchanged
  rows; the doc says to run such a sync once with a full fetch after a move.
- A device is moving only when its site matches neither by slug nor by name, the
  same rule the device apply uses to find the site; slug alone would mark every
  device at a site matched by name as moving on every run.
- The delete guard reads main (`DEFAULT_DB_ALIAS`): deletes run in the sync's
  branch, where netbox_branching routes change-log reads to the branch's own table.
- Adoption matches by slug, which a tag can name exactly, instead of a
  case-insensitive name match that touches every site lookup in the plugin.
- Absence of a create record is not proof another actor made a site: a fast
  baseline load writes no change log, and records age out.
