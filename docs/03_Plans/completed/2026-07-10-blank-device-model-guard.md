# Fix: guard blank device model in the bundled device queries (2.5.1)

## Goal

Stop a device Forward collected without a resolved model from being rejected
with `dcim.device model: This field cannot be blank` — import it with a
fallback device type instead of dropping it.

## Constraints

- Query-only; the fix must live in the NQE queries (NQE is the source of truth;
  the plugin must not normalize or mutate rows).
- Must not change device `name` or any identity/scope key.
- No schema or migration changes; drop-in from 2.5.0.

## Touched Surfaces

- `forward_netbox/queries/forward_devices.nqe` (device branch model guard)
- `forward_netbox/queries/forward_devices_with_netbox_aliases.nqe` (device branch)
- Tests: `test_endpoints_import.py`

## Approach

A `dcim.device` row reaching NetBox with a blank `device_type` is rejected
(`model: This field cannot be blank`). Two sources, both in the bundled
device/endpoint queries:

1. **Device with no model.** `toString(device.platform.model)` is *null* (not
   `""`) for a device Forward couldn't resolve a model for, so a naive
   `== ""` guard misses it. Both queries now use a null-safe guard
   (`if isPresent(model_raw) && model_raw != "" then model_raw else "Unknown"`),
   the aliases variant guarding `raw_model` at source.
2. **Endpoint with empty sysDescr (the common case).** SNMP endpoints emit as
   `dcim.device` rows; `device_type` comes from `sysDescr`. An endpoint
   reporting a present-but-empty sysDescr passed the `isPresent`-only fallback,
   so `ep_model = substring("", 0, 100) = ""`. The fallback now also rejects
   empty: `if isPresent(sysDescrOpt) && sysDescrOpt != "" then sysDescrOpt else
   "SNMP Endpoint"`.

Query-only change ⇒ operators Publish Bundled Queries after upgrading.

## Validation

NQE lint clean on both queries; structure tests assert the null-safe device
guard and the empty-sysDescr endpoint guard in both files. Live on the validation
network: **0 blank device_type / slug / manufacturer across all 5645 rows** in
both queries (was 4 blank before the endpoint guard). Full suite + lint +
harness green. Field-reported: rows rejected on blank model; sync did not crash.

## Rollback

Revert the branch — pure query text + version/doc bump; no data migration.

## Decision Log

- Fall back to a shared `Unknown` device type rather than skip the row: a
  modelless device is still a real device worth importing; a single shared
  fallback DeviceType is preferable to silently dropping it.
- Clamp in the queries, not the plugin: consistent with the 2.4.5 endpoint
  decision — NQE is the source of truth and the transformation is visible in
  the published query text.
