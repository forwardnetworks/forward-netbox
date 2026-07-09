# Fix: netbox-branching 1.1.1 compat + endpoint identity clamping (2.4.5)

## Goal

Stop the sync-fatal `type object 'SquashMergeStrategy' has no attribute
'_split_bidirectional_cycles'` on netbox-branching 1.1.1, and stop SNMP-endpoint
rows failing validation on long/degenerate sysDescr-derived identity fields.

## Constraints

- No schema or migration changes; drop-in from 2.4.4.
- On netbox-branching 1.1.0 behavior must stay bit-identical (prefer the
  framework helper when present).
- Identity clamping must not alter endpoint names (row identity / scope
  keys) and must live in the queries — NQE is the source of truth; the plugin
  must not normalize or mutate rows.
- Never commit the ADP network id / token.

## Touched Surfaces

- `forward_netbox/utilities/bulk_merge.py` (vendored
  `_split_bidirectional_create_cycles` + `_create_has_fk_to`, guarded
  `_log_cycle_details`)
- `forward_netbox/queries/forward_devices.nqe` and
  `forward_devices_with_netbox_aliases.nqe` (endpoint identity clamping)
- `pyproject.toml` (`netboxlabs-netbox-branching >=1.1.0,<1.2.0`)
- Tests: `test_bulk_merge.py`, `test_endpoints_import.py`

## Approach

1. netbox-branching **1.1.1 removed** `SquashMergeStrategy._split_bidirectional_cycles`
   (and `_has_fk_to`) — its own ordering now breaks cycles inline, which this
   plugin's fast ordering does not use. A fresh `pip install` resolves the
   unbounded `>=1.1.0` dependency to 1.1.1 and the first merge dies. Fix:
   vendor the 2-node bidirectional-cycle splitter (faithful mirror of the 1.1.0
   idiom: NULL the nullable FK on one CREATE, append a synthetic UPDATE),
   prefer the framework helper when it exists, guard `_log_cycle_details`, and
   bound the dependency to `<1.2.0` so future internal churn fails at install
   time, not mid-merge.
2. Endpoint rows emit `device_type` from the raw SNMP sysDescr, which exceeds
   NetBox's 100-char `DeviceType.model`/`slug` limits (observed 251 chars → 18
   per-row rejects) and can slugify to `""` (the
   `At least one coalesce lookup must be provided` error). Fix: clamp identity
   **in the bundled NQE queries** — the endpoint branches derive
   `ep_model = substring(sysDescr, 0, 100)`, slugify the clamped value, and
   guard empty slugs/manufacturers with explicit fallbacks. NQE stays the
   source of truth; the plugin does not normalize or mutate rows. Because the
   queries changed, operators must Publish Bundled Queries after upgrading.

## Validation

Unit tests: the vendored splitter breaks a real Device↔VirtualChassis
bidirectional CREATE pair; ordering succeeds with the framework attribute
deleted (simulated 1.1.1); clamping covers long values, slug fallbacks,
symbol-only values, and leaves other models/normal rows untouched. Full suite +
lint + harness green.

## Rollback

Revert the branch — pure Python + dependency-range change; no data migration.

## Decision Log

- Vendor the splitter rather than adopt 1.1.1's inline cycle breaking: the
  plugin runs its own topological sort for bulk merge, so it needs the
  pre-pass regardless of framework version; preferring the framework helper
  when present keeps 1.1.0 unchanged.
- Clamp in the NQE queries rather than the plugin: NQE's `substring`
  builtin self-clamps indices, the transformation is visible in the query text
  operators publish and review, and the plugin never silently rewrites what
  the query emitted (maintainer decision — NQE is the source of truth).
- Upper-bound the branching dependency (`<1.2.0`): the bulk merge borrows
  framework internals by design; an install-time conflict is a clearer failure
  than a mid-merge AttributeError.
