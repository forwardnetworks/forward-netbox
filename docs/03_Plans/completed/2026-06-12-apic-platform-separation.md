# APIC Platform Separation

## Goal

Keep APIC controllers on the `APIC` NetBox platform while leaving ACI switch
devices on `ACI`, so the platform field reflects the software family instead of
collapsing both controller and switch inventory into one label.

## Constraints

- Preserve the existing ACI-family discovery and repeat-sync behavior.
- Do not change APIC node or CIMC discovery beyond the platform name emitted
  for `dcim.platform` and `dcim.device`.
- Keep the change query-driven and backed by tests.

## Touched Surfaces

- `forward_netbox/queries/netbox_utilities.nqe`
- `forward_netbox/tests/test_query_registry.py`
- `docs/02_Reference/built-in-nqe-maps.md`

## Approach

1. Split APIC platform normalization from ACI switch normalization in the
   shared NQE helpers.
2. Keep the broad ACI device detection used by the fabric/node maps.
3. Update the registry tests and reference docs to show APIC controllers as a
   distinct platform.

## Validation

- `invoke harness-check`
- `invoke harness-test`
- `invoke lint`
- `invoke test-isolated --test-label forward_netbox.tests.test_query_registry`
- Live Forward validation org query audit passed after republishing the bundled
  query set to `/forward_netbox_validation/`.
- Live CustomerOrg query check against `Forward Platforms` on snapshot `1314736`
  returned both `APIC` and `ACI` platform rows, with Cisco APIC controllers
  landing on `APIC` and ACI switches remaining on `ACI`.

## Rollback

Restore the previous shared helper logic and revert the reference/test updates.

## Decision Log

- Use a shared helper instead of local special cases so the platform contract
  stays consistent across `dcim.platform` and `dcim.device`.
