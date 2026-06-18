# ipam.ipaddress Bulk-ORM (Experimental, Opt-In)

## Goal

Give the high-cardinality `ipam.ipaddress` model a bulk-ORM apply path so large
syncs batch IP writes instead of one save per row, reducing NetBox-side apply
time — without changing behavior for existing syncs.

## Constraints

- Experimental and opt-in only: `ipam.ipaddress` is added to
  `EXPERIMENTAL_BULK_ORM_MODELS`, NOT the default safe set. It runs on bulk only
  when `enable_bulk_orm` is set AND `ipam.ipaddress` is explicitly listed in the
  sync's `bulk_orm_models`. Default routing stays on the adapter.
- Adapter parity: the bulk path must reproduce `apply_ipam_ipaddress` semantics
  exactly — device/interface resolution, dependency skip vs fail, network-id /
  broadcast skips, VRF ensure, and the null-VRF net_host vs (address, vrf)
  coalesce. Only the DB write is batched.
- Data safety: IP assignment is critical; per-row failures must isolate like the
  adapter, and the architecture/parity audits must still pass.

## Touched Surfaces

- `forward_netbox/utilities/apply_engine_bulk.py` — `bulk_orm_apply_ipaddress`
  + dispatch.
- `forward_netbox/utilities/apply_engine_decision.py` — move `ipam.ipaddress`
  out of `ADAPTER_REQUIRED_MODELS` (+ drop its blocker), into
  `EXPERIMENTAL_BULK_ORM_MODELS` and `BULK_ORM_SPEC_MODELS`.
- `forward_netbox/tests/test_apply_engine.py` — bulk create/update + missing
  interface skip tests.
- `forward_netbox/tests/test_sync.py` — classification completeness test gains an
  experimental-opt-in branch.

## Approach

`bulk_orm_apply_ipaddress` pre-fetches devices, interfaces, and existing IPs
(host_ip OR-query for net_host coalesce), pre-ensures distinct VRFs once, then
walks rows applying the exact adapter resolution/skip logic, collecting resolved
IPAddress objects into create/update buckets and writing them with
bulk_create/bulk_update inside one transaction. Dispatch routes `ipam.ipaddress`
from `bulk_orm_apply_simple_models` to the new function. The decision engine
treats it as an experimental opt-in candidate.

## Validation

- `invoke test --test-label forward_netbox.tests.test_apply_engine`
  (create/update with GenericFK assignment; missing-interface aggregated skip;
  experimental-not-allowlisted stays adapter).
- `invoke test --test-label forward_netbox.tests.test_sync` (classification
  completeness with the new experimental branch).
- `forward_architecture_audit --fail-on-gap`.
- Full suite + `invoke lint`, `invoke harness-check`.

## Rollback

Revert the listed modules; `ipam.ipaddress` returns to adapter-required. No data
or schema migration. Opt-in default-off means no production sync is affected
until explicitly allowlisted.

## Decision Log

- Kept it experimental/opt-in (not the safe set) so it ships dark and is proven
  per-environment before any default promotion — matching how `ipam.prefix` was
  staged.
- Implemented as a batched mirror of the adapter (same resolution/skip code,
  batched writes) rather than a from-scratch spec, to minimize parity risk on
  critical IP-assignment data.
- The earlier blocker note tied promotion to the B4 batch-update path; B4 was
  reverted, but bulk_create/bulk_update here are independent of that adapter
  update-batching work, so this path does not depend on B4.
