# Simple/Tree Bulk Model Parity Tests + Platform Manufacturer Preserve Fix

## Goal

Round out the adapter-vs-bulk parity safety net to every default-bulk model, and
fix the real divergence that effort uncovered: the bulk engine overwrote
`dcim.platform.manufacturer` on update, clobbering operator overrides that the
adapter deliberately preserves.

## Findings

- Adapter `_ensure_platform` sets `manufacturer` only on CREATE and preserves it
  on UPDATE, so operators can correct the NQE-sourced manufacturer in NetBox
  without the next sync reverting it.
- The bulk simple/tree update loops wrote every spec field, so bulk
  `dcim.platform` (a default-enabled bulk model) re-applied the NQE manufacturer
  on every sync — clobbering that override. Surfaced by the new platform parity
  test.

## Constraints

- Bulk and adapter must produce identical DB state for every default-bulk model.
- Manufacturer is still set on CREATE; only preserved on UPDATE.
- No change to identity/coalesce handling (devicetype matches manufacturer as
  part of its identity and is unaffected).

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` —
  `CREATE_ONLY_UPDATE_FIELDS_BY_MODEL = {"dcim.platform": {"manufacturer"}}`;
  both the simple-models and tree-models update loops skip create-only fields and
  use the shared `_model_field_value_matches`.
- `forward_netbox/tests/test_bulk_adapter_parity.py` — parity tests for
  dcim.site, dcim.manufacturer, dcim.devicerole, dcim.platform, dcim.devicetype,
  ipam.vlan, ipam.vrf.
- `forward_netbox/tests/test_apply_engine.py` — platform update test now asserts
  manufacturer preservation.

## Approach

Add a per-model create-only field set consulted by both bulk update loops; when a
field is create-only it is skipped during the existing-row comparison/write.
Platform manufacturer is the only entry. Parity tests use the savepoint harness,
capturing related objects by slug/name (not pk, which is not stable across the
two savepoint runs).

## Validation

- `forward_netbox.tests.test_bulk_adapter_parity` (7 new model parities + the
  existing ipaddress/interface/macaddress/LAG/vc/churn cases).
- `forward_netbox.tests.test_apply_engine` (platform preserve; existing bulk
  create/update).
- Full `forward_netbox.tests` suite; `invoke harness-check`, lint.

## Rollback

Remove `CREATE_ONLY_UPDATE_FIELDS_BY_MODEL` and the skip, revert the platform
test, drop the new parity tests. No schema/data/migration impact.

## Decision Log

- Adapter behavior is canonical: preserving the operator override is the
  documented intent, so the bulk path was the side that was wrong.
- Implemented as a general create-only-fields map rather than a platform special
  case, so future preserve-on-update fields are a one-line addition and both bulk
  loops share the rule.
