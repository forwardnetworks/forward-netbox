# Bulk Duplicate Global IP Fix (unreleased)

## Goal

Extend the 1.5.6 duplicate-global-IP fix to the bulk apply path, which is the
default engine for `ipam.ipaddress` since 1.5.3. With duplicate global
(VRF-less) IPs for the same host, `bulk_orm_apply_ipaddress` matched the existing
IP correctly but then `ip.full_clean()` on the update path raised
`Duplicate IP address found in global table` (NetBox's `IPAddress.clean()` runs
the global-duplicate check), failing the row.

## Findings

- The bulk lookup key `(host_ip, vrf_id)` did match the prefetched key — the
  failure was not a missed match but the update-path validation.
- NetBox `IPAddress.clean()` (ip.py) raises on a duplicate global IP. The adapter
  avoids this entirely by updating via `save(update_fields=...)` with no clean.

## Constraints

- Match the adapter: do not fail the row on a pre-existing duplicate global IP.
- Deterministic duplicate selection so steady-state syncs do not churn.
- Keep field-level validation on update.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` — `bulk_orm_apply_ipaddress`:
  build `existing_by_key` with `order_by("pk")` + first-wins (lowest pk wins for
  duplicates); the update path uses `ip.clean_fields()` only (drop `ip.clean()`,
  which runs the global-duplicate check).
- `forward_netbox/tests/test_ipaddress_dup_global.py` — bulk regression test
  (no duplicate-create error; lowest-pk copy assigned; count stays 2).

## Approach

`order_by("pk")` + `if key not in existing_by_key` makes the chosen duplicate
deterministic (lowest pk). On update, run only `clean_fields()` so the
global-uniqueness check in `clean()` does not fire. The create path keeps
`full_clean()` (a genuinely new IP must validate). Duplicate IPs are left
untouched.

## Validation

- `forward_netbox.tests.test_ipaddress_dup_global` (adapter + bulk).
- `forward_netbox.tests.test_bulk_adapter_parity`,
  `forward_netbox.tests.test_apply_engine` (no regression).
- Full suite; local CI mirror (pre-commit clean + run twice; harness; harness
  tests; py_compile; mkdocs --strict; build). GitHub CI on both matrices.

## Rollback

Revert `bulk_orm_apply_ipaddress` (restore `full_clean()` on update, drop the
ordering) and the test + version bump. No schema/data impact.

## Decision Log

- `clean_fields()` only (not `clean_fields()` + `clean()`) for the ipaddress bulk
  update: `clean()` is exactly where the global-duplicate check lives, and the
  adapter skips all clean on update, so matching it keeps the engines consistent.
- Lowest-pk selection (not interface-assigned-first like the adapter) because the
  bulk prefetch is built before per-row interface context; lowest-pk is stable
  and the subsequent update reassigns the interface regardless.
