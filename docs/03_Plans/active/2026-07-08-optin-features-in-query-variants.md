# Fix: opt-in features in the alias/rules query variants + discoverability (2.4.2)

## Goal

Make **Import SNMP Endpoints as Devices** (`sync_endpoints`) and **Sync Device
Tags** (`sync_device_tags`) work when the enabled maps are the **variant**
queries, not just the base queries — and make the recovery path discoverable so
this stops being a silent failure.

## Constraints

- No schema or migration changes; drop-in from 2.4.1.
- Variant query edits must preserve device-parallel execution (single
  `network.devices` reference) and stay union-type-compatible.
- Publishing writes to the Forward Org NQE library and requires operator write
  permission; the plugin must not assume it.
- Never commit the ADP network id / token; validate live via the local source.

## Touched Surfaces

- `forward_netbox/queries/forward_devices_with_netbox_aliases.nqe` (endpoint branch)
- `forward_netbox/queries/forward_device_feature_tags_with_rules.nqe` (`sync_device_tags`)
- `forward_netbox/utilities/query_registry.py` (`DEVICE_TAG_PARAMETER_QUERY_FILES`)
- `forward_netbox/utilities/health.py` (opt-in-feature-map health check)
- `forward_netbox/views.py` + `templates/.../forwardsync{,_health}.html` (Publish button)
- Tests: `test_query_variants.py`, `test_health.py`, `test_query_registry.py`

## Approach

- Port the SNMP-endpoint union branch into the alias-aware device query
  (identical to the base branch); align its device `role` to the clean role name
  so the device/endpoint union type-checks and matches the base query.
- Port the `sync_device_tags` branch into the with-rules tag query as a
  per-device inner union (single `network.devices` reference).
- Register the alias device query in `DEVICE_TAG_PARAMETER_QUERY_FILES` so the
  endpoint branch's tag-scope params are seeded and injected.
- Add a **Publish Bundled Queries** button + view on the sync Health page (and
  sync detail), pre-scoped to the sync source + `/forward_netbox_validation`
  (Overwrite on), with a clear write-permission error.
- Add a Health check that warns when an opt-in feature is enabled but no enabled
  map runs a query that provides it.
- Add a Health check that warns when a base query and its opt-in variant are both
  enabled (they double-apply rows for the same model and churn).

## Validation

Live against the ADP demo network: alias query `sync_endpoints` off→5021 rows,
on→5709 (+688; 355 Avocent/Console Server; sample `avocent-ai60 / Avocent /
Console Server`); with-rules query returns rule tags plus the `sync_device_tags`
branch. Unit tests lock variant feature parity, the single `network.devices`
reference, the Publish view (+ write-permission error), and the Health check.
Full suite (983) + lint + harness green.

## Rollback

Revert the branch (pure query text + Python + template + tests; no data
migration). Operators who already published the updated bundled queries can
re-publish the prior revision or Refresh Query IDs to the previous commit.

## Decision Log

- Duplicate the endpoint/tag branches into the variant files rather than sharing
  logic: NQE files are standalone with no include mechanism; the divergence is
  the root cause and duplication is the pragmatic fix.
- Publishing re-emits the alias-aware device role as the clean name (`ROUTER`),
  a one-time role update on alias-mapped devices — accepted because the base
  query already emits that value and the union requires a String role.
- Publish stays a one-click Health-page action (common case) while the bulk-edit
  form keeps the granular path.
