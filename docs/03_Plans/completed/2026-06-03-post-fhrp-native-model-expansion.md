# Post-FHRP Native Model Expansion

## Goal

Prepare the 1.2.0 native model expansion discovered from the NetBox and Forward
source inventory without increasing Forward SaaS API/NQE pressure.

## Constraints

- Extend the existing optional FHRP import from HSRP-only to protocol-aware HSRP
  plus VRRP when Forward exposes native VRRP group state.
- Keep FHRP import as one paged NQE map for `ipam.fhrpgroup`; do not add
  per-device, per-interface, per-group, or per-VIP Forward calls.
- Evaluate interface VLAN enrichment against the same bar:
  - native NetBox fields only
  - one bounded interface query path
  - no trunk/all-VLAN row explosion
  - no ownership surprises on existing NetBox VLAN assignments

## Non-Goals

- Do not import raw route tables, ACLs, NAT, STP, or security policy objects in
  this tranche.
- Do not parse device configs when Forward exposes native normalized fields.
- Do not add custom-field-only imports where NetBox lacks a native object model.
- Do not commit customer identifiers, network IDs, snapshot IDs, credentials, or
  raw live rows.

## Touched Surfaces

- `forward_netbox/queries/forward_hsrp_groups.nqe`
- `forward_netbox/queries/forward_interfaces.nqe`
- `forward_netbox/utilities/sync_ipam.py`
- `forward_netbox/utilities/sync_interface.py`
- `forward_netbox/tests/test_query_registry.py`
- `forward_netbox/tests/test_sync.py`
- User and reference docs for release compatibility, built-in NQE maps, model
  mapping, and configuration.
- Forward org query paths after local validation passes.

## Approach

1. Rename and broaden the bundled FHRP NQE semantics from HSRP-only to
   HSRP/VRRP row output while preserving the existing `ipam.fhrpgroup` model
   surface.
2. Add VRRP-focused adapter tests proving native group creation, VIP role,
   protocol-separated identities, and delete behavior.
3. Update query registry tests and reference docs so built-in query shape and
   documented behavior match the new protocol-aware output.
4. Inspect interface VLAN adapter/query paths and either:
   - implement a tightly bounded access/native VLAN enrichment with focused
     tests, and
   - keep tagged trunk VLAN expansion deferred because Forward can represent
     VLAN ranges and implicit all-VLAN trunks.

## Validation Plan

- Focused query registry tests for the built-in FHRP query source and required
  row fields.
- Focused NetBox sync tests for HSRP regression and VRRP behavior.
- Focused interface tests if VLAN enrichment is implemented.
- `invoke harness-check`
- `invoke harness-test`
- `invoke lint`
- `invoke check`
- Local NQE engine diagnostics for changed bundled queries.
- Forward org query update after local gates pass, with sanitized proof only.
- Full `invoke ci` before treating the tranche as release-ready.

## Validation Evidence

- Local NQE engine diagnostics passed with zero diagnostics for:
  - `forward_netbox/queries/forward_interfaces.nqe`
  - `forward_netbox/queries/forward_hsrp_groups.nqe`
- Focused NetBox tests passed: 48 tests covering query registry, LAG
  regression, bounded interface VLAN assignment, HSRP regression, and VRRP
  behavior.
- Post-fix isolated focused regression passed: 6 tests OK, including preserving
  an existing `Interface.untagged_vlan` when the Forward-reported VLAN is not
  present in the already-imported site-scoped VLAN set.
- `invoke harness-test` passed: 127 tests OK.
- `invoke lint` passed.
- `invoke harness-check` passed after plan schema alignment.
- `invoke check` passed.
- `invoke docs` passed.
- `invoke test` passed: 873 tests OK.
- `invoke scenario-test` passed: 53 tests OK.
- Post-fix `invoke ci` passed after the model, query, docs, live-validation,
  release-version, and VLAN-preservation tranche:
  - harness-check
  - harness-test
  - lint
  - build/start/check
  - scenario-test
  - scale-chaos-test
  - full NetBox test suite
  - browser-local Playwright harness
  - docs build
  - package build
- Post-version-bump `invoke docs` passed with `.venv/bin` on `PATH`.
- Post-version-bump package build passed:
  - `dist/forward_netbox-1.2.0.tar.gz`
  - `dist/forward_netbox-1.2.0-py3-none-any.whl`
- `twine check` passed for both `1.2.0` artifacts.
- `1.2.0` artifact SHA-256:
  - `forward_netbox-1.2.0.tar.gz`:
    `5d74c799a4f8c1d958b1be9d0323b499a93a76beb983f3eab5f7da7d3c1342db`
  - `forward_netbox-1.2.0-py3-none-any.whl`:
    `b9e39b6ba0da1af70bb945f644d0fa66a97b93f77d6345cd79e26543d5dec157`
- Sensitive-content scan passed after the live Forward validation evidence was
  recorded in sanitized form.
- Release version decision: promote this tranche to `v1.2.0` because it expands
  the native model surface beyond the `v1.1.x` API/NQE optimization and FHRP
  hardening line.
- Forward org query update succeeded for:
  - `/forward_netbox_validation/forward_interfaces`
  - `/forward_netbox_validation/forward_hsrp_groups`
- Live limited Forward validation passed with sanitized output:
  - `forward_interfaces`: one-row limited execution returned `mode` and
    `untagged_vlan` keys.
  - `forward_hsrp_groups`: one-row limited execution returned expected FHRP row
    keys.
  - `http_attempts`: `5`
  - `http_successes`: `5`
  - `http_429_failures`: `0`
  - `nqe_query_calls`: `2`
  - `nqe_query_pages`: `2`

## Rollback

- Revert the bundled FHRP query to HSRP-only rows and keep the existing
  `ipam.fhrpgroup` optional model gate.
- Revert the interface query to omit `mode` and `untagged_vlan`; existing custom
  interface queries remain compatible because these fields are not required by
  the model sync contract.
- Roll back Forward org query paths to the prior query text if live validation
  shows runtime incompatibility.
- Disable `ipam.fhrpgroup` for a sync to avoid all FHRP imports.

## Decision Log

- Start with VRRP because it shares the same native NetBox model and Forward
  normalized schema family as HSRP.
- Include interface access/native VLAN assignment because `ipam.vlan` is already
  applied before `dcim.interface` and the adapter can attach only already
  imported site-scoped VLANs without extra Forward calls.
- Defer tagged trunk VLAN assignment because Forward ranges and implicit
  all-VLAN trunks can produce high relationship volume and larger Branching
  diffs.
