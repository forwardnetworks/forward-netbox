# CIMC Platform Separation

## Goal

Keep CIMC-managed hardware visible as its own NetBox platform instead of
collapsing it into APIC or ACI platform names.

## Constraints

- Preserve the existing APIC and ACI platform behavior.
- Keep the change query-driven and backed by tests.
- Do not change the APIC CIMC inventory-item map contract.

## Touched Surfaces

- `forward_netbox/queries/netbox_utilities.nqe`
- `forward_netbox/tests/test_query_registry.py`
- `docs/02_Reference/built-in-nqe-maps.md`
- `docs/02_Reference/model-mapping-matrix.md`

## Approach

1. Add CIMC detection to the shared platform normalization helper.
2. Keep APIC controllers on `APIC` and ACI switches on `ACI`.
3. Update the registry tests and reference docs so the platform contract is
   explicit.

## Validation

- `invoke harness-check`
- `invoke harness-test`
- `invoke lint`
- `invoke test-isolated --test-label forward_netbox.tests.test_query_registry`
- `invoke docs`
- `invoke check`
- `git diff --check`

## Rollback

Restore the previous shared helper logic and revert the test and reference
updates.

## Decision Log

- Use the shared helper rather than a query-local special case so `dcim.platform`
  and `dcim.device` stay aligned.
