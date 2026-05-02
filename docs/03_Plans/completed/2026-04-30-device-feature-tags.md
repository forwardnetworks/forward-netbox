# Device Feature Tags

## Goal

Attach NetBox tags to devices from Forward feature evidence, starting with BGP-enabled devices tagged as `Prot_BGP`.

## Constraints

- Keep feature detection in NQE so operators can inspect and customize the mapping.
- Use native NetBox `extras.Tag` and `dcim.Device.tags` behavior.
- Do not create a separate tagging path outside the normal sync and Branching workflow.
- Do not persist customer identifiers, network IDs, snapshot IDs, or sample device names in docs or tests.

## Touched Surfaces

- `forward_netbox/choices.py`
- `forward_netbox/queries/forward_device_feature_tags.nqe`
- `forward_netbox/queries/forward_device_feature_tags_with_rules.nqe`
- `forward_netbox/utilities/query_registry.py`
- `forward_netbox/utilities/sync_contracts.py`
- `forward_netbox/utilities/branch_budget.py`
- `forward_netbox/utilities/sync.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`
- `scripts/build_netbox_feature_tag_rules.py`
- `scripts/tests/test_build_netbox_feature_tag_rules.py`
- `README.md`
- `docs/README.md`
- `docs/02_Reference/built-in-nqe-maps.md`
- `docs/02_Reference/feature-tag-rules-data-file.md`
- `docs/02_Reference/model-mapping-matrix.md`

## Approach

1. Add `extras.taggeditem` as a supported sync surface for device/tag associations.
2. Ship a built-in `Forward Device Feature Tags` NQE map that emits one row per device/tag association.
3. Detect BGP through structured Forward protocol state and emit `Prot_BGP` with slug `prot-bgp`.
4. Create or update NetBox `Tag` objects by slug and attach them to exact device-name matches.
5. Remove only the device/tag association during diff deletes; leave the global Tag object in place.
6. Include `extras.taggeditem` in device-sharded branch planning so large tag sets follow the existing branch-budget path.
7. Add a disabled optional `Forward Device Feature Tags with Rules` map that reads `netbox_feature_tag_rules` from the selected snapshot and maps enabled structured feature rules to NetBox tags.

## Validation

- Query registry tests verify the shipped feature-tag query emits the BGP tag fields and coalesce identity.
- Query registry tests verify the disabled rules-aware query references `netbox_feature_tag_rules` and emits tag fields from rule rows.
- Sync adapter tests verify create, existing-tag reuse/update, delete, and row contract validation.
- Harness tests verify the generated feature-tag rules data file has a stable schema.
- Execute the feature-tag NQE against the live smoke source without recording customer identifiers in committed artifacts.
- Run targeted NetBox tests plus the standard harness, lint, check, and test gates.

## Rollback

- Remove `extras.taggeditem` from supported models and the built-in query registry.
- Revert the feature-tag adapter, query, tests, and docs.
- Remove the optional `netbox_feature_tag_rules` data-file query and helper script.
- Any tags already merged into NetBox would remain normal NetBox objects and should be reviewed or removed by an operator if rollback is required.

## Decision Log

- Rejected: parse raw device configuration text in Python.
  - Reason: protocol detection belongs in NQE where Forward already exposes parsed feature state.
- Rejected: delete Tag objects when a diff row disappears.
  - Reason: a Tag can be shared or operator-owned; the sync should only remove its managed association from the device.
- Rejected: make data-file rules the default path.
  - Reason: the default sync must remain runnable without a Forward data file; the rules-aware map is intentionally opt-in.
