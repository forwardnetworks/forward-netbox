# dcim.interface Bulk-ORM (Experimental, Opt-In)

## Goal

Give the high-volume `dcim.interface` model a bulk-ORM apply path so large syncs
batch interface writes, while preserving the adapter's LAG and cable side-effect
semantics exactly. Off by default.

## Constraints

- Experimental opt-in only: `dcim.interface` moves to
  `EXPERIMENTAL_BULK_ORM_MODELS` (not the default safe set). Bulk runs only when
  `enable_bulk_orm` is set AND `dcim.interface` is listed in the sync's
  `bulk_orm_models`. Adapter otherwise.
- Exact parity for hard cases: LAG membership (parent ensure + self-LAG guard)
  and converting an interface to type `lag` while it still has a cable (cable
  removal side-effect) must behave identically to the adapter.
- Plain interfaces only get batched. Existing rows load from the DB so fields
  absent from a Forward row are written back unchanged (no clearing), matching
  the adapter upsert.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` — `bulk_orm_apply_interface`
  + dispatch.
- `forward_netbox/utilities/apply_engine_decision.py` — move `dcim.interface`
  out of `ADAPTER_REQUIRED_MODELS` (drop blocker) into
  `EXPERIMENTAL_BULK_ORM_MODELS` + `BULK_ORM_SPEC_MODELS`.
- `forward_netbox/tests/test_apply_engine.py` — plain create/update + LAG-row
  delegation tests.
- `forward_netbox/tests/test_health.py`,
  `forward_netbox/tests/test_architecture_audit_command.py` — classification /
  blocker / experimental-allowlist expectations updated.

## Approach

`bulk_orm_apply_interface` resolves devices from a prefetch map, then per row:
rows with LAG membership (`lag`) or lag-conversion-with-cable are delegated to
the untouched adapter `apply_dcim_interface` (exact parity for parent ordering
and cable removal); all other rows build the adapter's defaults (type, enabled,
optional mtu/speed/description, access/tagged mode + untagged VLAN) into create/
update buckets written via bulk_create/bulk_update in one transaction. Delegated
rows mirror the runner's dependency-skip/fail issue handling.

## Validation

- `invoke test --test-label forward_netbox.tests.test_apply_engine`
  (plain create/update; LAG-row delegation to adapter; experimental-not-
  allowlisted stays adapter).
- Regression: full `forward_netbox.tests` suite; `forward_architecture_audit`.
- `invoke lint`, `invoke harness-check`.

## Rollback

Revert the listed modules; `dcim.interface` returns to adapter-required. Opt-in
default-off means no production sync changes until explicitly allowlisted. No
data or schema migration.

## Decision Log

- Hybrid batch+delegate chosen over a full from-scratch bulk path: LAG parent→
  member ordering and cable side-effects are the documented blocker and resist
  clean batching, so those rows stay on the proven adapter while the common
  plain-interface case is batched — capturing most of the throughput win at
  minimal parity risk.
